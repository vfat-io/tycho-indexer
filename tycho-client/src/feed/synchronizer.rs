use std::{collections::HashMap, sync::Arc};

use tokio::{
    select,
    sync::{
        mpsc::{channel, Receiver, Sender},
        oneshot, Mutex,
    },
    task::JoinHandle,
};
use tracing::{debug, error, info, instrument, trace, warn};
use tycho_types::{
    dto::{
        BlockParam, Deltas, ExtractorIdentity, ProtocolComponent, ProtocolStateRequestBody,
        ResponseAccount, ResponseProtocolState, StateRequestBody, VersionParam,
    },
    Bytes,
};

use super::Header;
use crate::{deltas::DeltasClient, feed::component_tracker::ComponentTracker, rpc::RPCClient};

type SyncResult<T> = anyhow::Result<T>;

#[derive(Clone)]
pub struct StateSynchronizer<R: RPCClient, D: DeltasClient> {
    extractor_id: ExtractorIdentity,
    is_native: bool,
    rpc_client: R,
    deltas_client: D,
    max_retries: u64,
    min_tvl_threshold: f64,
    shared: Arc<Mutex<SharedState>>,
    end_tx: Arc<Mutex<Option<oneshot::Sender<()>>>>,
}

#[derive(Debug)]
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
        // be careful with removed and snapshots attributes here, these can be ambiguous.
        self.removed_components
            .retain(|k, _| !other.snapshots.contains_key(k));
        self.snapshots
            .retain(|k, _| !other.removed_components.contains_key(k));

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
        max_retries: u64,
        rpc_client: R,
        deltas_client: D,
    ) -> Self {
        Self {
            extractor_id,
            is_native: native,
            rpc_client,
            deltas_client,
            min_tvl_threshold: min_tvl,
            max_retries,
            shared: Arc::new(Mutex::new(SharedState {
                last_served_block_hash: None,
                last_synced_block: None,
                pending: None,
            })),
            end_tx: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn get_pending(&self, block_hash: Bytes) -> Option<StateSyncMessage> {
        let mut shared = self.shared.lock().await;
        let to_serve = shared.pending.take();
        shared.last_served_block_hash = Some(block_hash);
        to_serve
    }

    pub async fn start(&self) -> SyncResult<(JoinHandle<SyncResult<()>>, Receiver<Header>)> {
        let (mut block_tx, block_rx) = channel(15);

        let this = self.clone();
        let jh = tokio::spawn(async move {
            let mut retry_count = 0;
            while retry_count < this.max_retries {
                info!(extractor_id=%&this.extractor_id, retry_count, "(Re)starting synchronization loop");
                let (end_tx, end_rx) = oneshot::channel::<()>();
                {
                    let mut end_tx_guard = this.end_tx.lock().await;
                    *end_tx_guard = Some(end_tx);
                }

                select! {
                    res = this.clone().state_sync(&mut block_tx) => {
                        match  res
                        {
                            Err(e) => {
                                error!(
                                    extractor_id=%&this.extractor_id,
                                    retry_count,
                                    error=%e,
                                    "State synchronization errored!"
                                );
                            }
                            _ => {
                                warn!(
                                    extractor_id=%&this.extractor_id,
                                    retry_count,
                                    "State sync exited with Ok(())"
                                );
                            }
                        }
                    },
                    _ = end_rx => {
                        info!(
                            extractor_id=%&this.extractor_id,
                            retry_count,
                            "StateSynchronizer received close signal. Stopping"
                        );
                        return Ok(())
                    }
                }
                retry_count += 1;
            }
            Err(anyhow::format_err!("Max retries exceeded giving up"))
        });

        Ok((jh, block_rx))
    }

    pub async fn close(&mut self) -> SyncResult<()> {
        let mut end_tx = self.end_tx.lock().await;
        if let Some(tx) = end_tx.take() {
            let _ = tx.send(());
            Ok(())
        } else {
            Err(anyhow::format_err!("Not started"))
        }
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
                    &StateRequestBody::new(
                        Some(
                            contract_ids
                                .clone()
                                .into_iter()
                                .collect(),
                        ),
                        version,
                    ),
                )
                .await?
                .accounts
                .into_iter()
                .map(|acc| (acc.address.clone(), acc))
                .collect::<HashMap<_, _>>();

            trace!(states=?&contract_state, "Retrieved ContractState");
            Ok(StateSyncMessage {
                header,
                // iteration over all component is not ideal for performance but reduces the
                // required state and state updating. Since we e.g. have no mapping from
                // contract_address to corresponding protocol component. Snapshots should not be
                // retrieved too frequently, so we are ok for now this can be optimised should it be
                // required.
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
                                } else if contract_ids.contains(contract_address) {
                                    // only emit error even if we did actually request this
                                    // address.
                                    error!(
                                        ?contract_address,
                                        ?component_id,
                                        "Component with lacking state encountered!"
                                    );
                                    None
                                } else {
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
                // actually this may be improved by iterating over the requested ids, it is kept
                // similar to the contract state only for consistency reasons.
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
                        } else if ids.contains(&&component.id) {
                            // only emit error event if we requested this component
                            let component_id = &component.id;
                            error!(?component_id, "Missing state for native component!");
                            None
                        } else {
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
    #[instrument(skip(self, block_tx),fields(extractor_id = %self.extractor_id))]
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
            .map_err(|rpc_err| {
                anyhow::format_err!("failed to get initial snapshot: {}", rpc_err)
            })?;

        let n_components = tracker.components.len();
        info!(n_components, "Initial snapshot retrieved, starting delta message feed");

        {
            let mut shared = self.shared.lock().await;
            shared.pending = Some(snapshot);
            shared.last_synced_block = Some(header.clone());
        }
        block_tx.send(header.clone()).await?;
        loop {
            if let Some(mut deltas) = msg_rx.recv().await {
                let header = Header::from_block(deltas.get_block(), deltas.is_revert());
                debug!(block_number=?header.number, "Received delta message");
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
                            if tracker.components.contains_key(*k) {
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
                let n_changes = deltas.n_changes();

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
                debug!(block_number=?header.number, n_changes, "Finished processing delta message");
                block_tx.send(header).await?;
            } else {
                let mut shared = self.shared.lock().await;
                warn!(shared = ?&shared, "Deltas channel closed, resetting shared state.");
                shared.pending = None;
                shared.last_synced_block = None;
                shared.last_served_block_hash = None;

                return Err(anyhow::format_err!("Deltas channel closed!"));
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
            synchronizer::{
                NativeSnapshot, Snapshot, StateSyncMessage, StateSynchronizer, VMSnapshot,
            },
            Header,
        },
        rpc::{MockRPCClient, RPCClient},
        DeltasError, RPCError,
    };
    use async_trait::async_trait;
    use mockall::predicate::always;
    use std::{sync::Arc, time::Duration};
    use test_log::test;
    use tokio::{
        sync::mpsc::{channel, Receiver, Sender},
        task::JoinHandle,
        time::timeout,
    };
    use tycho_types::{
        dto::{
            Block, BlockEntityChangesResult, Chain, Deltas, ExtractorIdentity, ProtocolComponent,
            ProtocolComponentRequestParameters, ProtocolComponentRequestResponse,
            ProtocolComponentsRequestBody, ProtocolId, ProtocolStateRequestBody,
            ProtocolStateRequestResponse, ResponseAccount, ResponseProtocolState, StateRequestBody,
            StateRequestParameters, StateRequestResponse,
        },
        Bytes,
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
            50.0,
            1,
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

    #[test(tokio::test)]
    async fn test_get_snapshots_native() {
        let header = Header::default();
        let mut rpc = MockRPCClient::new();
        rpc.expect_get_protocol_states()
            .returning(|_, _, _| Ok(state_snapshot_native()));
        let state_sync = with_mocked_clients(true, Some(rpc), None);
        let mut tracker = ComponentTracker::new(
            Chain::Ethereum,
            "uniswap-v2",
            0.0,
            state_sync.rpc_client.clone(),
        );
        let component = ProtocolComponent { id: "Component1".to_string(), ..Default::default() };
        tracker
            .components
            .insert("Component1".to_string(), component.clone());
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
                            component: component.clone(),
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

    fn state_snapshot_vm() -> StateRequestResponse {
        StateRequestResponse {
            accounts: vec![
                ResponseAccount { address: Bytes::from("0x0badc0ffee"), ..Default::default() },
                ResponseAccount { address: Bytes::from("0xbabe42"), ..Default::default() },
            ],
        }
    }

    #[test(tokio::test)]
    async fn test_get_snapshots_vm() {
        let header = Header::default();
        let mut rpc = MockRPCClient::new();
        rpc.expect_get_contract_state()
            .returning(|_, _, _| Ok(state_snapshot_vm()));
        let state_sync = with_mocked_clients(false, Some(rpc), None);
        let mut tracker = ComponentTracker::new(
            Chain::Ethereum,
            "uniswap-v2",
            0.0,
            state_sync.rpc_client.clone(),
        );
        let component = ProtocolComponent {
            id: "Component1".to_string(),
            contract_ids: vec![Bytes::from("0x0badc0ffee"), Bytes::from("0xbabe42")],
            ..Default::default()
        };
        tracker
            .components
            .insert("Component1".to_string(), component.clone());
        let components_arg = ["Component1".to_string()];
        let exp = StateSyncMessage {
            header: header.clone(),
            snapshots: [(
                component.id.clone(),
                Snapshot::VMSnapshot(VMSnapshot {
                    state: state_snapshot_vm()
                        .accounts
                        .into_iter()
                        .map(|state| (state.address.clone(), state))
                        .collect(),
                    component: component.clone(),
                }),
            )]
            .into_iter()
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

    #[test(tokio::test)]
    async fn test_get_pending() {
        let state_sync = with_mocked_clients(true, None, None);
        let state_msg = StateSyncMessage {
            header: Default::default(),
            snapshots: Default::default(),
            deltas: None,
            removed_components: Default::default(),
        };
        {
            let mut guard = state_sync.shared.lock().await;
            guard.pending = Some(state_msg.clone());
        }

        let res = state_sync
            .get_pending(Bytes::default())
            .await
            .expect("Could not get pending state!");

        assert_eq!(res, state_msg);
        {
            let guard = state_sync.shared.lock().await;
            assert!(guard.pending.is_none());
        }
    }

    fn mock_clients_for_state_sync() -> (MockRPCClient, MockDeltasClient, Sender<Deltas>) {
        let mut rpc_client = MockRPCClient::new();
        // Mocks for the start_tracking call, these need to come first because they are more
        // specific, see: https://docs.rs/mockall/latest/mockall/#matching-multiple-calls
        rpc_client
            .expect_get_protocol_components()
            .with(
                always(),
                always(),
                mockall::predicate::function(
                    move |request_params: &ProtocolComponentsRequestBody| {
                        if let Some(ids) = request_params.component_ids.as_ref() {
                            ids.contains(&"Component3".to_string())
                        } else {
                            false
                        }
                    },
                ),
            )
            .returning(|_, _, _| {
                // return Component3
                Ok(ProtocolComponentRequestResponse {
                    protocol_components: vec![
                        // this component shall have a tvl update above threshold
                        ProtocolComponent { id: "Component3".to_string(), ..Default::default() },
                    ],
                })
            });
        rpc_client
            .expect_get_protocol_states()
            .with(
                always(),
                always(),
                mockall::predicate::function(move |request_params: &ProtocolStateRequestBody| {
                    let expected_id =
                        ProtocolId { chain: Chain::Ethereum, id: "Component3".to_string() };
                    if let Some(ids) = request_params.protocol_ids.as_ref() {
                        ids.contains(&expected_id)
                    } else {
                        false
                    }
                }),
            )
            .returning(|_, _, _| {
                // return Component3 state
                Ok((ProtocolStateRequestResponse {
                    states: vec![ResponseProtocolState {
                        component_id: "Component3".to_string(),
                        ..Default::default()
                    }],
                }))
            });

        // mock calls for the initial state snapshots
        rpc_client
            .expect_get_protocol_components()
            .returning(|_, _, _| {
                // Initial sync of components
                Ok(ProtocolComponentRequestResponse {
                    protocol_components: vec![
                        // this component shall have a tvl update above threshold
                        ProtocolComponent { id: "Component1".to_string(), ..Default::default() },
                        // this component shall have a tvl update below threshold.
                        ProtocolComponent { id: "Component2".to_string(), ..Default::default() },
                        // a third component will have a tvl update above threshold
                    ],
                })
            });
        rpc_client
            .expect_get_protocol_states()
            .returning(|_, _, _| {
                // Initial state snapshot
                Ok((ProtocolStateRequestResponse {
                    states: vec![
                        ResponseProtocolState {
                            component_id: "Component1".to_string(),
                            ..Default::default()
                        },
                        ResponseProtocolState {
                            component_id: "Component2".to_string(),
                            ..Default::default()
                        },
                    ],
                }))
            });
        // Mock deltas client and messages
        let mut deltas_client = MockDeltasClient::new();
        let (tx, rx) = channel(1);
        deltas_client
            .expect_subscribe()
            .return_once(move |_| {
                // Return subscriber id and a channel
                Ok((Uuid::default(), rx))
            });
        (rpc_client, deltas_client, tx)
    }

    /// Test strategy
    ///
    /// - initial snapshot returns a two component
    /// - send 2 dummy messages, containing only blocks
    /// - third message contains a new component with some significant tvl, one initial component
    ///   slips below tvl threshold, another one is above tvl but does not get re-requested.
    #[test(tokio::test)]
    async fn test_state_sync() {
        let (rpc_client, deltas_client, tx) = mock_clients_for_state_sync();
        let deltas = [
            Deltas::Native(BlockEntityChangesResult {
                extractor: "uniswap-v2".to_string(),
                chain: Chain::Ethereum,
                block: Block {
                    number: 1,
                    hash: Bytes::from("0x01"),
                    parent_hash: Bytes::from("0x00"),
                    chain: Chain::Ethereum,
                    ts: Default::default(),
                },
                revert: false,
                ..Default::default()
            }),
            Deltas::Native(BlockEntityChangesResult {
                extractor: "uniswap-v2".to_string(),
                chain: Chain::Ethereum,
                block: Block {
                    number: 2,
                    hash: Bytes::from("0x02"),
                    parent_hash: Bytes::from("0x01"),
                    chain: Chain::Ethereum,
                    ts: Default::default(),
                },
                revert: false,
                ..Default::default()
            }),
            Deltas::Native(BlockEntityChangesResult {
                extractor: "uniswap-v2".to_string(),
                chain: Chain::Ethereum,
                block: Block {
                    number: 3,
                    hash: Bytes::from("0x03"),
                    parent_hash: Bytes::from("0x02"),
                    chain: Chain::Ethereum,
                    ts: Default::default(),
                },
                revert: false,
                component_tvl: [
                    ("Component1".to_string(), 100.0),
                    ("Component2".to_string(), 0.0),
                    ("Component3".to_string(), 1000.0),
                ]
                .into_iter()
                .collect(),
                ..Default::default()
            }),
        ];
        let mut state_sync = with_mocked_clients(true, Some(rpc_client), Some(deltas_client));

        // Test starts here
        let (jh, mut block_rx) = state_sync
            .start()
            .await
            .expect("Failed to start state synchronizer");
        tx.send(deltas[0].clone())
            .await
            .expect("deltas channel msg 0 closed!");
        tx.send(deltas[1].clone())
            .await
            .expect("deltas channel msg 1 closed!");
        timeout(Duration::from_millis(100), block_rx.recv())
            .await
            .expect("waiting for first state msg timed out!")
            .expect("state sync block sender closed!");
        tx.send(deltas[2].clone())
            .await
            .expect("deltas channel msg 2 closed!");
        let header = timeout(Duration::from_millis(100), block_rx.recv())
            .await
            .expect("waiting for second state msg timed out!")
            .expect("state sync block sender closed!");
        state_sync.close().await;
        let res = state_sync
            .get_pending(header.hash.clone())
            .await
            .expect("Failed to get pending state");
        let exit = jh
            .await
            .expect("state sync task panicked!");

        // assertions
        let exp = StateSyncMessage {
            header,
            snapshots: [
                // since we did not retrieve the first message the snapshot first,
                // "Component1" should be included.
                (
                    "Component1".to_string(),
                    Snapshot::NativeSnapshot(NativeSnapshot {
                        state: ResponseProtocolState {
                            component_id: "Component1".to_string(),
                            ..Default::default()
                        },
                        component: ProtocolComponent {
                            id: "Component1".to_string(),
                            ..Default::default()
                        },
                    }),
                ),
                // This is the new component we queried once it passed the tvl threshold.
                (
                    "Component3".to_string(),
                    Snapshot::NativeSnapshot(NativeSnapshot {
                        state: ResponseProtocolState {
                            component_id: "Component3".to_string(),
                            ..Default::default()
                        },
                        component: ProtocolComponent {
                            id: "Component3".to_string(),
                            ..Default::default()
                        },
                    }),
                ),
            ]
            .into_iter()
            .collect(),
            // Our deltas are empty and since merge methods are
            // tested in tycho-types we don't have much to do here.
            deltas: Some(deltas[2].clone()),
            // "Component2" was removed, because it's tvl changed to 0.
            removed_components: [(
                "Component2".to_string(),
                ProtocolComponent { id: "Component2".to_string(), ..Default::default() },
            )]
            .into_iter()
            .collect(),
        };
        assert_eq!(res, exp);
        assert!(exit.is_ok());
    }
}
