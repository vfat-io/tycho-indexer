use std::{
    borrow::BorrowMut,
    collections::{HashMap, HashSet},
    sync::Arc,
};

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
        BlockParam, Chain, Deltas, ExtractorIdentity, ProtocolComponent,
        ProtocolComponentRequestParameters, ProtocolComponentsRequestBody, ProtocolId,
        ProtocolStateRequestBody, ResponseAccount, ResponseProtocolState, StateRequestBody,
        VersionParam,
    },
    Bytes,
};

use super::Header;
use crate::{deltas::DeltasClient, rpc::RPCClient, HttpRPCClient, WsDeltasClient};

type SyncResult<T> = anyhow::Result<T>;

#[derive(Clone)]
pub struct StateSynchronizer {
    extractor_id: ExtractorIdentity,
    is_native: bool,
    rpc_client: HttpRPCClient,
    deltas_client: WsDeltasClient,
    min_tvl_threshold: f64,
    shared: Arc<Mutex<SharedState>>,
}

struct SharedState {
    last_served_block_hash: Option<Bytes>,
    last_synced_block: Option<Header>,
    pending_deltas: HashMap<Bytes, StateMessage>,
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
    /// Usually a single delta, but may contain additional snapshots
    // TODO: consider moving snapshots to their own attributes, to avoid a the nested delta enum
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

/// Helper struct to store which components are being tracked atm.
struct ComponentTracker {
    chain: Chain,
    protocol_system: String,
    min_tvl_threshold: f64,
    // We will need to request a snapshot for components/Contracts that we did not emit as
    // snapshot for yet but are relevant now, e.g. because min tvl threshold exceeded.
    components: HashMap<String, ProtocolComponent>,
    /// derived from tracked components, we need this if subscribed to a vm extractor cause updates
    /// are emitted on a contract level instead of on a component level.
    contracts: HashSet<Bytes>,
    /// Client to retrieve necessary protocol components from the rpc.
    rpc_client: HttpRPCClient,
}

impl ComponentTracker {
    fn new() -> Self {
        todo!();
    }
    /// Retrieve all components that belong to the system we are extracing and have sufficient tvl.
    async fn initialise_components(&mut self) {
        let filters = ProtocolComponentRequestParameters::tvl_filtered(self.min_tvl_threshold);
        let request = ProtocolComponentsRequestBody::system_filtered(&self.protocol_system);
        self.components = self
            .rpc_client
            .get_protocol_components(self.chain, &filters, &request)
            .await
            .expect("could not init protocol components")
            .protocol_components
            .into_iter()
            .map(|pc| (pc.id.clone(), pc))
            .collect::<HashMap<_, _>>();
        self.update_contracts();
    }

    fn update_contracts(&mut self) {
        self.contracts.extend(
            self.components
                .values()
                .flat_map(|comp| comp.contract_ids.iter().cloned()),
        );
    }

    /// Add a new component to be tracked
    async fn start_tracking(&mut self, new_components: &[&String]) {
        let filters = ProtocolComponentRequestParameters::default();
        let request = ProtocolComponentsRequestBody::id_filtered(
            new_components
                .iter()
                .map(|pc_id| pc_id.to_string())
                .collect(),
        );

        self.components.extend(
            self.rpc_client
                .get_protocol_components(self.chain, &filters, &request)
                .await
                .expect("could not get new protocol components")
                .protocol_components
                .into_iter()
                .map(|pc| (pc.id.clone(), pc)),
        );
        self.update_contracts();
    }

    /// Stop tracking components
    async fn stop_tracking<'a, I: IntoIterator<Item = &'a String>>(
        &mut self,
        to_remove: I,
    ) -> HashMap<String, ProtocolComponent> {
        to_remove
            .into_iter()
            .filter_map(|k| {
                let comp = self.components.remove(k);
                if let Some(component) = &comp {
                    for contract in component.contract_ids.iter() {
                        self.contracts.remove(contract);
                    }
                }
                comp.map(|c| (k.clone(), c))
            })
            .collect()
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
        let mut shared = self.shared.lock().await;
        let target_hash = shared.last_served_block_hash.clone();

        let mut current_hash = block_hash.clone();
        let mut to_serve: Option<StateMessage> = None;

        // If this is the first request, we have to hit a snapshot else this will error!
        let state = shared.pending_deltas.borrow_mut();
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
        shared.last_served_block_hash = Some(block_hash);
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

    /// Retrieves state snapshots of the requested components
    async fn get_snapshots<'a, I: IntoIterator<Item = &'a str>>(
        &self,
        ids: I,
        header: Header,
        tracked_components: &HashMap<String, ProtocolComponent>,
    ) -> SyncResult<StateMessage> {
        let version = VersionParam::new(
            None,
            Some(BlockParam { chain: None, hash: Some(header.hash.clone()), number: None }),
        );
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
        let mut tracker = ComponentTracker::new();
        tracker.initialise_components().await;

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
            .get_snapshots(
                tracker
                    .components
                    .keys()
                    .map(String::as_str),
                Header::from_block(&block, false),
                &tracker.components,
            )
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
                let (mut state_updates, removed_components) = {
                    // 1. Remove components based on tvl changes (not available yet)
                    // 2. Add components based on tvl changes, query those for snapshots
                    let (to_add, to_remove): (Vec<_>, Vec<_>) = deltas
                        .component_tvl()
                        .iter()
                        .partition(|(_, &tvl)| tvl > self.min_tvl_threshold);

                    // only components we don't track yet need a snapshot,
                    // TODO: additional indentation required to tracked_components gets dropped
                    // later on. Most likely we don't need this if we simply own tracked_components
                    // and contracts.
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
                    let state_updates = self
                        .get_snapshots(
                            requiring_snapshot
                                .into_iter()
                                .map(String::as_str),
                            header.clone(),
                            &tracker.components,
                        )
                        .await?
                        .state_updates;

                    let removed_components = tracker
                        .stop_tracking(to_remove.iter().map(|(id, _)| *id))
                        .await;
                    (state_updates, removed_components)
                };

                // 3. Filter deltas by currently tracked components / contracts
                if self.is_native {
                    deltas.filter_by_component(|id| tracker.components.contains_key(id));
                } else {
                    deltas.filter_by_contract(|id| tracker.contracts.contains(id));
                }

                state_updates.push(SnapOrDelta::Deltas(deltas));

                {
                    let mut shared = self.shared.lock().await;
                    shared.pending_deltas.insert(
                        block.hash.clone(),
                        StateMessage { header: header.clone(), state_updates, removed_components },
                    );
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
