use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    sync::Arc,
};

use tokio::{
    sync::{
        mpsc::{channel, Receiver, Sender},
        Mutex,
    },
    task::JoinHandle,
};
use tracing::error;
use tycho_types::{
    dto::{
        Block, BlockAccountChanges, BlockEntityChangesResult, BlockParam, ContractId, Deltas,
        ExtractorIdentity, ProtocolComponent, ProtocolComponentId,
        ProtocolComponentRequestParameters, ProtocolComponentsRequestBody, ProtocolId,
        ProtocolStateRequestBody, ResponseAccount, ResponseProtocolState, StateRequestBody,
        StateRequestParameters, VersionParam,
    },
    Bytes,
};

use super::Header;
use crate::{deltas::DeltasClient, rpc::RPCClient, HttpRPCClient, WsDeltasClient};

type SyncResult<T> = anyhow::Result<T>;

#[derive(Clone)]
pub struct StateSynchronizer {
    extractor_id: ExtractorIdentity,
    protocol_system: String,
    is_native: bool,
    rpc_client: HttpRPCClient,
    deltas_client: WsDeltasClient,
    // We will need to request a snapshot for components/Contracts that we did not emit a
    // snapshot for yet but are relevant now, e.g. because min tvl threshold exceeded.
    tracked_components: Arc<Mutex<HashMap<ProtocolComponentId, ProtocolComponent>>>,
    // derived from tracked components, we need this if subscribed to a vm extractor cause updates
    // are emitted on a contract level instead of on a component level.
    tracked_contracts: Arc<Mutex<HashSet<Bytes>>>,

    last_served_block_hash: Arc<Mutex<Option<Bytes>>>,
    last_synced_block: Arc<Mutex<Option<Header>>>,
    // Since we may return a mix of snapshots and deltas (e.g. if a new component crossed the tvl
    // threshold) the value type is a vector.
    pending_deltas: Arc<Mutex<HashMap<Bytes, StateMessage>>>,
    // Waiting for the rpc endpoints to support tvl.
    #[allow(dead_code)]
    min_tvl_threshold: f64,
}

#[derive(Clone)]
pub struct VMSnapshot {
    state: Vec<ResponseAccount>,
    component: ProtocolComponent,
}

#[derive(Clone)]
pub struct NativeSnapshot {
    state: ResponseProtocolState,
    component: ProtocolComponent,
}

#[derive(Clone)]
pub enum SnapOrDelta {
    VMSnapshot(VMSnapshot),
    NativeSnapshot(NativeSnapshot),
    Deltas(Deltas),
}

#[derive(Clone)]
pub struct StateMessage {
    header: Header,
    state_updates: Vec<SnapOrDelta>,
    removed_components: HashMap<String, ProtocolComponent>,
}

impl StateMessage {
    fn is_all_snapshots(&self) -> bool {
        let mut is_all_snapshots = true;
        for msg in &self.state_updates {
            if let SnapOrDelta::Deltas(_) = &msg {
                return false;
            } else {
                is_all_snapshots &= true;
            }
        }
        is_all_snapshots
    }

    pub fn merge(self, other: Self) -> Self {
        todo!()
    }
}

impl StateSynchronizer {
    pub fn new(
        extracor_id: ExtractorIdentity,
        rpc_client: HttpRPCClient,
        deltas_client: WsDeltasClient,
    ) -> Self {
        todo!();
    }

    pub async fn get_pending(&self, block_hash: Bytes) -> SyncResult<StateMessage> {
        // Batch collect all changes up to the requested block
        // Must either hit a snapshot, or the last served block hash if it does not it errors
        let mut state = self.pending_deltas.lock().await;
        let mut target_hash = self.last_served_block_hash.lock().await;

        let mut current_hash = block_hash.clone();
        let mut to_serve: Option<StateMessage> = None;

        // If this is the first request, we have to hit a snapshot else this will error!
        while Some(&current_hash) != target_hash.as_ref() {
            if let Some(data) = state.remove(&current_hash) {
                current_hash = data.header.hash.clone();
                let is_all_snapshots = data.is_all_snapshots();
                to_serve = Some(to_serve.map_or(data.clone(), |current| current.merge(data)));

                if is_all_snapshots {
                    break;
                }
            } else {
                // TODO inform if we were looking for a snapshot or not
                anyhow::bail!("hash not found");
            }
        }
        *target_hash = Some(block_hash);
        Ok(to_serve.unwrap())
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

    /// Retrieve all components that belong to the system we are extracing and have sufficient tvl.
    async fn initialise_components(&mut self) {
        let filters = ProtocolComponentRequestParameters::tvl_filtered(self.min_tvl_threshold);
        let request = ProtocolComponentsRequestBody::system_filtered(&self.protocol_system);
        let mut tracked_components = self.tracked_components.lock().await;
        *tracked_components = self
            .rpc_client
            .get_protocol_components(self.extractor_id.chain, &filters, &request)
            .await
            .expect("could not init protocol components")
            .protocol_components
            .into_iter()
            .map(|pc| (pc.get_id(), pc))
            .collect::<HashMap<_, _>>();
    }

    /// Add a new component to be tracked
    async fn start_tracking(&self, new_components: &[ProtocolComponentId]) {
        let filters = ProtocolComponentRequestParameters::default();
        let request = ProtocolComponentsRequestBody::id_filtered(
            new_components
                .iter()
                .map(|pc_id| pc_id.id.clone())
                .collect(),
        );
        let mut tracked_components = self.tracked_components.lock().await;
        tracked_components.extend(
            self.rpc_client
                .get_protocol_components(self.extractor_id.chain, &filters, &request)
                .await
                .expect("could not get new protocol components")
                .protocol_components
                .into_iter()
                .map(|pc| (pc.get_id(), pc)),
        );
    }

    /// Stop tracking a component
    async fn stop_tracking(&self, components: &[ProtocolComponentId]) {
        let mut tracked_components = self.tracked_components.lock().await;
        components.iter().for_each(|k| {
            tracked_components.remove(k);
        });
    }

    /// Retrieves state snapshots of the requested components
    async fn get_snapshots<'a, I: IntoIterator<Item = &'a ProtocolComponentId>>(
        &self,
        ids: I,
        header: Header,
    ) -> SyncResult<StateMessage> {
        let version = VersionParam::new(
            None,
            Some(BlockParam { chain: None, hash: Some(header.hash.clone()), number: None }),
        );
        let tracked_components = self.tracked_components.lock().await;
        if self.is_native {
            let mut contract_ids = Vec::new();
            ids.into_iter().for_each(|cid| {
                let comp = tracked_components
                    .get(cid)
                    .expect("requested component that is not present");
                contract_ids.extend(comp.contract_ids.iter().cloned());
            });

            let mut contract_state = self
                .rpc_client
                .get_contract_state(
                    self.extractor_id.chain,
                    &Default::default(),
                    &StateRequestBody::new(Some(contract_ids), version),
                )
                .await?
                .accounts
                .into_iter()
                .map(|acc| (acc.address.clone(), acc))
                .collect::<HashMap<_, _>>();

            Ok(StateMessage {
                header,
                state_updates: tracked_components
                    .values()
                    .into_iter()
                    .map(|comp| {
                        let component_id = &comp.id;
                        let account_snapshots: Vec<_> = comp
                            .contract_ids
                            .iter()
                            .filter_map(|contract_address| {
                                if let Some(state) = contract_state.remove(contract_address) {
                                    Some(state)
                                } else {
                                    // TODO: remove the entire component in this case, warn and then
                                    // continue
                                    error!(
                                        ?contract_address,
                                        ?component_id,
                                        "Component without state encountered!"
                                    );
                                    None
                                }
                            })
                            .collect();
                        SnapOrDelta::VMSnapshot(VMSnapshot {
                            component: comp.clone(),
                            state: account_snapshots,
                        })
                    })
                    .collect(),
                removed_components: HashMap::new(),
            })
        } else {
            let mut component_ids = Vec::new();
            ids.into_iter().for_each(|cid| {
                let comp = tracked_components
                    .get(cid)
                    .expect("requested component that is not present");
                component_ids
                    .push(ProtocolId { chain: self.extractor_id.chain, id: comp.id.clone() });
            });

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

            Ok(StateMessage {
                header,
                state_updates: tracked_components
                    .values()
                    .filter_map(|component| {
                        if let Some(state) = protocol_states.remove(&component.id) {
                            Some(SnapOrDelta::NativeSnapshot(NativeSnapshot {
                                state,
                                component: component.clone(),
                            }))
                        } else {
                            let component_id = &component.id;
                            error!(?component_id, "Missing state for native component!");
                            None
                        }
                    })
                    .collect::<Vec<_>>(),
                removed_components: HashMap::new(),
            })
        }
    }

    async fn state_sync(mut self, block_tx: &mut Sender<Header>) -> SyncResult<()> {
        // initialisation
        self.initialise_components().await;
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
        let snapshot = async {
            let tracked_components = self.tracked_components.lock().await;
            self.get_snapshots(tracked_components.keys(), Header::from_block(&block, false))
                .await
                .expect("failed to get initial snapshot")
        }
        .await;
        self.pending_deltas
            .lock()
            .await
            .insert(header.hash.clone(), snapshot);
        *self.last_synced_block.lock().await = Some(header.clone());
        block_tx.send(header.clone()).await?;
        loop {
            if let Some(deltas) = msg_rx.recv().await {
                let header = Header::from_block(deltas.get_block(), deltas.is_revert());
                // TODO:
                // 1. Remove components based on tvl changes (not available yet)
                // 2. Add components based on tvl changes, query those for snapshots
                // 3. Filter deltas by tracked components / contracts
                self.pending_deltas.lock().await.insert(
                    block.hash.clone(),
                    StateMessage {
                        header: header.clone(),
                        state_updates: vec![SnapOrDelta::Deltas(deltas)],
                        removed_components: HashMap::new(),
                    },
                );
                *self.last_synced_block.lock().await = Some(header.clone());
                block_tx.send(header).await?;
            } else {
                self.pending_deltas.lock().await.clear();
                *self.last_synced_block.lock().await = None;
                *self.last_served_block_hash.lock().await = None;
                return Ok(());
            }
        }
    }
}
