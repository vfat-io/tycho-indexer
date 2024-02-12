use std::{collections::HashMap, sync::Arc};

use tokio::{
    sync::{
        mpsc::{channel, Receiver, Sender},
        Mutex,
    },
    task::JoinHandle,
};
use tracing::{error, info};
use tycho_types::{
    dto::{
        BlockParam, Deltas, ExtractorIdentity, ProtocolComponent,
        ProtocolComponentRequestParameters, ProtocolComponentsRequestBody, ProtocolId,
        ProtocolStateRequestBody, ResponseAccount, ResponseProtocolState, StateRequestBody,
        VersionParam,
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
pub struct StateSynchronizer<R: RPCClient> {
    extractor_id: ExtractorIdentity,
    is_native: bool,
    rpc_client: R,
    deltas_client: WsDeltasClient,
    min_tvl_threshold: f64,
    shared: Arc<Mutex<SharedState>>,
}

struct SharedState {
    last_served_block_hash: Option<Bytes>,
    last_synced_block: Option<Header>,
    pending_deltas: HashMap<Bytes, StateSyncMessage>,
    pending: Option<StateSyncMessage>,
}

#[derive(Clone)]
pub struct VMSnapshot {
    state: HashMap<Bytes, ResponseAccount>,
    component: ProtocolComponent,
}

#[derive(Clone)]
pub struct NativeSnapshot {
    state: ResponseProtocolState,
    component: ProtocolComponent,
}

#[derive(Clone)]
pub enum Snapshot {
    VMSnapshot(VMSnapshot),
    NativeSnapshot(NativeSnapshot),
}

#[derive(Clone)]
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
    pub fn merge(self, other: Self) -> Self {
        todo!()
    }
}

impl<R> StateSynchronizer<R>
where
    R: RPCClient + Clone + Send + Sync + 'static,
{
    pub fn new(
        extracor_id: ExtractorIdentity,
        rpc_client: HttpRPCClient,
        deltas_client: WsDeltasClient,
    ) -> Self {
        todo!();
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
    async fn get_snapshots<'a, I: IntoIterator<Item = &'a str>>(
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
                    .map(String::as_str)
                    .collect::<Vec<_>>()
            });

        if self.is_native {
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
            .get_snapshots::<Vec<&str>>(Header::from_block(&block, false), &tracker, None)
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
            shared
                .pending_deltas
                .insert(header.hash.clone(), snapshot);
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
                        .await;
                    let snapshots = self
                        .get_snapshots(
                            header.clone(),
                            &tracker,
                            Some(
                                requiring_snapshot
                                    .into_iter()
                                    .map(String::as_str),
                            ),
                        )
                        .await?
                        .snapshots;

                    let removed_components = tracker
                        .stop_tracking(to_remove.iter().map(|(id, _)| *id))
                        .await;
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
                shared.pending_deltas.clear();
                shared.last_synced_block = None;
                shared.last_served_block_hash = None;

                return Ok(());
            }
        }
    }
}
