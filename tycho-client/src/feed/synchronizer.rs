use std::{collections::HashMap, sync::Arc};

use tokio::{
    sync::{
        mpsc::{channel, Receiver, Sender},
        Mutex,
    },
    task::JoinHandle,
};
use tracing::{error, info, trace};
use tycho_types::{
    dto::{
        BlockParam, Deltas, ExtractorIdentity, ProtocolComponent, ProtocolComponentsRequestBody,
        ProtocolId, ProtocolStateRequestBody, ResponseAccount, ResponseProtocolState,
        StateRequestBody, VersionParam,
    },
    Bytes,
};

use super::Header;
use crate::{
    deltas::DeltasClient, feed::component_tracker::ComponentTracker, rpc::RPCClient, HttpRPCClient,
    WsDeltasClient,
};

type SyncResult<T> = anyhow::Result<T>;

#[derive(Clone)]
pub struct StateSynchronizer<R: RPCClient, D: DeltasClient> {
    extractor_id: ExtractorIdentity,
    is_native: bool,
    rpc_client: R,
    deltas_client: D,
    min_tvl_threshold: f64,
    shared: Arc<Mutex<SharedState>>,
}

struct SharedState {
    last_served_block_hash: Option<Bytes>,
    last_synced_block: Option<Header>,
    pending: Option<StateSyncMessage>,
}

#[derive(Clone, PartialEq, Debug)]
pub struct VMSnapshot {
    state: HashMap<Bytes, ResponseAccount>,
    component: ProtocolComponent,
}

#[derive(Clone, PartialEq, Debug)]
pub struct NativeSnapshot {
    state: ResponseProtocolState,
    component: ProtocolComponent,
}

#[derive(Clone, PartialEq, Debug)]
pub enum Snapshot {
    VMSnapshot(VMSnapshot),
    NativeSnapshot(NativeSnapshot),
}

#[derive(Clone, PartialEq, Debug)]
pub struct StateSyncMessage {
    /// The block number for this update.
    header: Header,
    /// Snapshot for new components.
    snapshots: HashMap<String, Snapshot>,
    /// A single delta contains state updates for all tracked components, as well as additional
    /// information about the system components e.g. newly added components (even below tvl), tvl
    /// updates, balance updates.
    deltas: Option<Deltas>,
    /// Components that stopped being tracked.
    removed_components: HashMap<String, ProtocolComponent>,
}

impl StateSyncMessage {
    pub fn merge(mut self, other: Self) -> Self {
        self.snapshots
            .extend(other.snapshots.into_iter());
        let deltas = match (self.deltas, other.deltas) {
            (Some(l), Some(r)) => Some(l.merge(r)),
            (None, Some(r)) => Some(r),
            (Some(l), None) => Some(l),
            (None, None) => None,
        };
        self.removed_components
            .extend(other.removed_components.into_iter());
        Self {
            header: other.header,
            snapshots: self.snapshots,
            deltas,
            removed_components: self.removed_components,
        }
    }
}

impl<R, D> StateSynchronizer<R, D>
where
    R: RPCClient + Clone + Send + Sync + 'static,
    D: DeltasClient + Clone + Send + Sync + 'static,
{
    pub fn new(
        extractor_id: ExtractorIdentity,
        native: bool,
        min_tvl: f64,
        rpc_client: R,
        deltas_client: D,
    ) -> Self {
        Self {
            extractor_id,
            is_native: native,
            rpc_client,
            deltas_client,
            min_tvl_threshold: min_tvl,
            shared: Arc::new(Mutex::new(SharedState {
                last_served_block_hash: None,
                last_synced_block: None,
                pending: None,
            })),
        }
    }

    pub async fn get_pending(&self, block_hash: Bytes) -> Option<StateSyncMessage> {
        let mut shared = self.shared.lock().await;
        let to_serve = shared.pending.take();
        shared.last_served_block_hash = Some(block_hash);
        to_serve
    }

    pub fn start(&self) -> SyncResult<(JoinHandle<SyncResult<()>>, Receiver<Header>)> {
        let (mut block_tx, block_rx) = channel(15);

        let this = self.clone();
        let jh = tokio::spawn(async move {
            let mut retry_count = 0;
            while retry_count < 5 {
                this.clone()
                    .state_sync(&mut block_tx)
                    .await?;
                // reset state and prepare for retry
                retry_count += 1;
            }
            Ok(())
        });

        Ok((jh, block_rx))
    }

    /// Retrieves state snapshots of the requested components
    async fn get_snapshots<'a, I: IntoIterator<Item = &'a String>>(
        &self,
        header: Header,
        tracked_components: &ComponentTracker<R>,
        ids: Option<I>,
    ) -> SyncResult<StateSyncMessage> {
        let version = VersionParam::new(
            None,
            Some(BlockParam { chain: None, hash: Some(header.hash.clone()), number: None }),
        );
        // Use given ids or use all if not passed
        let ids = ids
            .map(|it| it.into_iter().collect::<Vec<_>>())
            .unwrap_or_else(|| {
                tracked_components
                    .components
                    .keys()
                    .into_iter()
                    .collect::<Vec<_>>()
            });
        if !self.is_native {
            let contract_ids = tracked_components.get_contracts_by_component(ids);

            let contract_state = self
                .rpc_client
                .get_contract_state(
                    self.extractor_id.chain,
                    &Default::default(),
                    &StateRequestBody::new(Some(contract_ids.into_iter().collect()), version),
                )
                .await?
                .accounts
                .into_iter()
                .map(|acc| (acc.address.clone(), acc))
                .collect::<HashMap<_, _>>();

            trace!(states=?&contract_state, "Retrieved ContractState");
            Ok(StateSyncMessage {
                header,
                snapshots: tracked_components
                    .components
                    .values()
                    .into_iter()
                    .map(|comp| {
                        let component_id = &comp.id;
                        let account_snapshots: HashMap<_, _> = comp
                            .contract_ids
                            .iter()
                            .filter_map(|contract_address| {
                                // Cloning is essential to prevent mistakenly assuming a component
                                // lacks associated state due to the m2m relationship between
                                // contracts and components.
                                if let Some(state) = contract_state.get(contract_address) {
                                    Some((contract_address.clone(), state.clone()))
                                } else {
                                    error!(
                                        ?contract_address,
                                        ?component_id,
                                        "Component with lacking state encountered!"
                                    );
                                    None
                                }
                            })
                            .collect();
                        (
                            component_id.clone(),
                            Snapshot::VMSnapshot(VMSnapshot {
                                component: comp.clone(),
                                state: account_snapshots,
                            }),
                        )
                    })
                    .collect(),
                deltas: None,
                removed_components: HashMap::new(),
            })
        } else {
            let component_ids = tracked_components.get_tracked_component_ids();

            let mut protocol_states = self
                .rpc_client
                .get_protocol_states(
                    self.extractor_id.chain,
                    &Default::default(),
                    &ProtocolStateRequestBody::id_filtered(component_ids),
                )
                .await?
                .states
                .into_iter()
                .map(|state| (state.component_id.clone(), state))
                .collect::<HashMap<_, _>>();

            trace!(states=?&protocol_states, "Retrieved ProtocolStates");
            Ok(StateSyncMessage {
                header,
                snapshots: tracked_components
                    .components
                    .values()
                    .filter_map(|component| {
                        if let Some(state) = protocol_states.remove(&component.id) {
                            Some((
                                component.id.clone(),
                                Snapshot::NativeSnapshot(NativeSnapshot {
                                    state,
                                    component: component.clone(),
                                }),
                            ))
                        } else {
                            let component_id = &component.id;
                            error!(?component_id, "Missing state for native component!");
                            None
                        }
                    })
                    .collect(),
                deltas: None,
                removed_components: HashMap::new(),
            })
        }
    }

    /// Main method that does all the work.
    async fn state_sync(self, block_tx: &mut Sender<Header>) -> SyncResult<()> {
        // initialisation
        let mut tracker = ComponentTracker::new(
            self.extractor_id.chain,
            self.extractor_id.name.as_str(),
            self.min_tvl_threshold,
            self.rpc_client.clone(),
        );
        tracker.initialise_components().await?;

        let (_, mut msg_rx) = self
            .deltas_client
            .subscribe(self.extractor_id.clone())
            .await?;

        // we need to wait 2 messages because of cache gateways insertion delay.
        let _ = msg_rx
            .recv()
            .await
            .ok_or_else(|| anyhow::format_err!("Subscription ended too soon"))?;
        let first_msg = msg_rx
            .recv()
            .await
            .ok_or_else(|| anyhow::format_err!("Subscription ended too soon"))?;

        // initial snapshot
        let block = first_msg.get_block().clone();
        let header = Header::from_block(first_msg.get_block(), first_msg.is_revert());
        let snapshot = self
            .get_snapshots::<Vec<&String>>(Header::from_block(&block, false), &tracker, None)
            .await
            .expect("failed to get initial snapshot");

        let n_components = tracker.components.len();
        let extractor_id = &self.extractor_id;
        info!(
            ?n_components,
            ?extractor_id,
            "Initial snapshot retrieved, starting delta message feed"
        );

        {
            let mut shared = self.shared.lock().await;
            shared.pending = Some(snapshot);
            shared.last_synced_block = Some(header.clone());
        }
        block_tx.send(header.clone()).await?;
        loop {
            if let Some(mut deltas) = msg_rx.recv().await {
                let header = Header::from_block(deltas.get_block(), deltas.is_revert());
                let (snapshots, removed_components) = {
                    // 1. Remove components based on tvl changes
                    // 2. Add components based on tvl changes, query those for snapshots
                    let (to_add, to_remove): (Vec<_>, Vec<_>) = deltas
                        .component_tvl()
                        .iter()
                        .partition(|(_, &tvl)| tvl > self.min_tvl_threshold);

                    // Only components we don't track yet need a snapshot,
                    let requiring_snapshot = to_add
                        .iter()
                        .filter_map(|(k, _)| {
                            if !tracker.components.contains_key(*k) {
                                None
                            } else {
                                Some(*k)
                            }
                        })
                        .collect::<Vec<_>>();
                    tracker
                        .start_tracking(&requiring_snapshot)
                        .await?;
                    let snapshots = self
                        .get_snapshots(header.clone(), &tracker, Some(requiring_snapshot))
                        .await?
                        .snapshots;

                    let removed_components =
                        tracker.stop_tracking(to_remove.iter().map(|(id, _)| *id));
                    (snapshots, removed_components)
                };

                // 3. Filter deltas by currently tracked components / contracts
                if self.is_native {
                    deltas.filter_by_component(|id| tracker.components.contains_key(id));
                } else {
                    deltas.filter_by_contract(|id| tracker.contracts.contains(id));
                }

                {
                    let mut shared = self.shared.lock().await;
                    let next = StateSyncMessage {
                        header: header.clone(),
                        snapshots,
                        deltas: Some(deltas),
                        removed_components,
                    };
                    shared.pending = if let Some(prev) = shared.pending.take() {
                        Some(prev.merge(next))
                    } else {
                        Some(next)
                    };
                    shared.last_synced_block = Some(header.clone());
                }

                block_tx.send(header).await?;
            } else {
                let mut shared = self.shared.lock().await;
                shared.pending = None;
                shared.last_synced_block = None;
                shared.last_served_block_hash = None;

                return Ok(());
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{
        deltas::{DeltasClient, MockDeltasClient},
        feed::{
            component_tracker::ComponentTracker,
            synchronizer::{NativeSnapshot, Snapshot, StateSyncMessage, StateSynchronizer},
            Header,
        },
        rpc::{MockRPCClient, RPCClient},
        DeltasError, RPCError,
    };
    use async_trait::async_trait;
    use std::sync::Arc;
    use test_log::test;
    use tokio::{sync::mpsc::Receiver, task::JoinHandle};
    use tracing::info;
    use tycho_types::dto::{
        BlockAccountChanges, Chain, Deltas, ExtractorIdentity, ProtocolComponent,
        ProtocolComponentRequestParameters, ProtocolComponentRequestResponse,
        ProtocolComponentsRequestBody, ProtocolStateRequestBody, ProtocolStateRequestResponse,
        ResponseProtocolState, StateRequestBody, StateRequestParameters, StateRequestResponse,
    };
    use uuid::Uuid;

    // Required for mock client to implement clone
    struct ArcRPCClient<T>(Arc<T>);

    // Default derive(Clone) does require T to be Clone as well.
    impl<T> Clone for ArcRPCClient<T> {
        fn clone(&self) -> Self {
            ArcRPCClient(self.0.clone())
        }
    }

    #[async_trait]
    impl<T> RPCClient for ArcRPCClient<T>
    where
        T: RPCClient + Sync + Send + 'static,
    {
        async fn get_contract_state(
            &self,
            chain: Chain,
            filters: &StateRequestParameters,
            request: &StateRequestBody,
        ) -> Result<StateRequestResponse, RPCError> {
            self.0
                .get_contract_state(chain, filters, request)
                .await
        }

        async fn get_protocol_components(
            &self,
            chain: Chain,
            filters: &ProtocolComponentRequestParameters,
            request: &ProtocolComponentsRequestBody,
        ) -> Result<ProtocolComponentRequestResponse, RPCError> {
            self.0
                .get_protocol_components(chain, filters, request)
                .await
        }

        async fn get_protocol_states(
            &self,
            chain: Chain,
            filters: &StateRequestParameters,
            request: &ProtocolStateRequestBody,
        ) -> Result<ProtocolStateRequestResponse, RPCError> {
            self.0
                .get_protocol_states(chain, filters, request)
                .await
        }
    }

    // Required for mock client to implement clone
    struct ArcDeltasClient<T>(Arc<T>);

    // Default derive(Clone) does require T to be Clone as well.
    impl<T> Clone for ArcDeltasClient<T> {
        fn clone(&self) -> Self {
            ArcDeltasClient(self.0.clone())
        }
    }

    #[async_trait]
    impl<T> DeltasClient for ArcDeltasClient<T>
    where
        T: DeltasClient + Sync + Send + 'static,
    {
        async fn subscribe(
            &self,
            extractor_id: ExtractorIdentity,
        ) -> Result<(Uuid, Receiver<Deltas>), DeltasError> {
            self.0.subscribe(extractor_id).await
        }

        async fn unsubscribe(&self, subscription_id: Uuid) -> Result<(), DeltasError> {
            self.0
                .unsubscribe(subscription_id)
                .await
        }

        async fn connect(&self) -> Result<JoinHandle<Result<(), DeltasError>>, DeltasError> {
            self.0.connect().await
        }

        async fn close(&self) -> Result<(), DeltasError> {
            self.0.close().await
        }
    }

    fn with_mocked_clients(
        native: bool,
        rpc_client: Option<MockRPCClient>,
        deltas_client: Option<MockDeltasClient>,
    ) -> StateSynchronizer<ArcRPCClient<MockRPCClient>, ArcDeltasClient<MockDeltasClient>> {
        let rpc_client = ArcRPCClient(Arc::new(rpc_client.unwrap_or_else(|| MockRPCClient::new())));
        let deltas_client =
            ArcDeltasClient(Arc::new(deltas_client.unwrap_or_else(|| MockDeltasClient::new())));

        StateSynchronizer::new(
            ExtractorIdentity::new(Chain::Ethereum, "uniswap-v2"),
            native,
            0.0,
            rpc_client,
            deltas_client,
        )
    }

    fn state_snapshot_native() -> ProtocolStateRequestResponse {
        ProtocolStateRequestResponse {
            states: vec![ResponseProtocolState {
                component_id: "Component1".to_string(),
                ..Default::default()
            }],
        }
    }

    fn state_snapshot_vm() -> StateRequestResponse {
        todo!();
    }

    // test cases:
    // some components
    // none components
    // native
    // vm
    #[test(tokio::test)]
    async fn test_get_snapshots() {
        let header = Header::default();
        let mut rpc = MockRPCClient::new();
        rpc.expect_get_protocol_states()
            .returning(|_, _, _| Ok(state_snapshot_native()));
        let mut state_sync = with_mocked_clients(true, Some(rpc), None);
        let mut tracker = ComponentTracker::new(
            Chain::Ethereum,
            "uniswap-v2",
            0.0,
            state_sync.rpc_client.clone(),
        );
        tracker.components.insert(
            "Component1".to_string(),
            ProtocolComponent { id: "Component1".to_string(), ..Default::default() },
        );
        dbg!(&tracker.components);
        let components_arg = ["Component1".to_string()];
        let exp = StateSyncMessage {
            header: header.clone(),
            snapshots: state_snapshot_native()
                .states
                .into_iter()
                .map(|state| {
                    (
                        state.component_id.clone(),
                        Snapshot::NativeSnapshot(NativeSnapshot {
                            state,
                            component: ProtocolComponent {
                                id: "Component1".to_string(),
                                ..Default::default()
                            },
                        }),
                    )
                })
                .collect(),
            deltas: None,
            removed_components: Default::default(),
        };

        let snap = state_sync
            .get_snapshots(header, &tracker, Some(&components_arg))
            .await
            .expect("Retrieving snapshot failed");

        assert_eq!(snap, exp);
    }
}
