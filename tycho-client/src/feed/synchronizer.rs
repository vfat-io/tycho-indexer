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
use tycho_types::{
    dto::{
        Block, BlockAccountChanges, BlockEntityChangesResult, BlockParam, ContractId, Deltas,
        ExtractorIdentity, ProtocolComponent, ProtocolComponentId,
        ProtocolComponentRequestParameters, ProtocolComponentsRequestBody, ResponseAccount,
        ResponseProtocolComponent, StateRequestBody, StateRequestParameters, VersionParam,
    },
    Bytes,
};

use super::Header;
use crate::{deltas::DeltasClient, rpc::RPCClient, HttpRPCClient, WsDeltasClient};

type SyncResult<T> = anyhow::Result<T>;

#[derive(Clone)]
pub struct StateSynchronizer {
    extractor_id: ExtractorIdentity,
    rpc_client: HttpRPCClient,
    deltas_client: WsDeltasClient,
    // We will need to request a snapshot for components/Contracts that we did not emit a
    // snapshot for yet but are relevant now, e.g. because min tvl threshold exceeded.
    tracked_components: Arc<Mutex<HashMap<ProtocolComponentId, ResponseProtocolComponent>>>,
    // derived from tracked components
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
    component: ResponseProtocolComponent,
}

#[derive(Clone)]
pub enum SnapOrDelta {
    VMSnapshot(VMSnapshot),
    // TODO: type missing
    NativeSnapshot,
    Deltas(Deltas),
}

#[derive(Clone)]
pub struct StateMessage {
    header: Header,
    state_updates: Vec<SnapOrDelta>,
    removed_components: HashSet<String>,
    removed_contracts: HashSet<Bytes>,
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

    /// Retrieve all components that currently match our min tvl filter
    async fn initialise_components(&mut self) {
        let filters =
            // TODO: min_tvl_threshold should be f64
            ProtocolComponentRequestParameters { tvl_gt: Some(self.min_tvl_threshold as u64) };
        let request = ProtocolComponentsRequestBody { protocol_system: None, component_ids: None };
        let mut tracked_components = self.tracked_components.lock().await;
        *tracked_components = self
            .rpc_client
            .get_protocol_components(&filters, &request)
            .await
            .expect("could not init protocol components")
            .protocol_components
            .into_iter()
            .map(|pc| (pc.get_id(), pc))
            .collect::<HashMap<_, _>>();
    }

    /// Add a new component to be tracked
    async fn start_tracking(&self, new_components: &[ProtocolComponentId]) {
        let filters = ProtocolComponentRequestParameters { tvl_gt: None };
        let request = ProtocolComponentsRequestBody {
            protocol_system: None,
            component_ids: Some(
                new_components
                    .iter()
                    .map(|pc_id| pc_id.id.clone())
                    .collect(),
            ),
        };
        let mut tracked_components = self.tracked_components.lock().await;
        tracked_components.extend(
            self.rpc_client
                .get_protocol_components(&filters, &request)
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

    /// Retrieves state snapshots of the passed components
    async fn get_snapshots<'a, I: IntoIterator<Item = &'a ProtocolComponentId>>(
        &self,
        ids: I,
        header: Header,
    ) -> SyncResult<StateMessage> {
        // TODO: it is either contracts or components
        let mut contract_ids = Vec::new();
        let mut component_ids = Vec::new();
        let tracked_components = self.tracked_components.lock().await;

        ids.into_iter().for_each(|cid| {
            let comp = tracked_components
                .get(cid)
                .expect("requested component that is not present");
            if comp.contract_ids.is_empty() {
                component_ids.push(comp.id.clone());
            } else {
                contract_ids.extend(comp.contract_ids.iter().cloned());
            }
        });

        let version = VersionParam::new(
            None,
            Some(BlockParam { chain: None, hash: Some(header.hash.clone()), number: None }),
        );
        if !contract_ids.is_empty() {
            let filters = Default::default();
            let body = StateRequestBody::new(Some(contract_ids), version);
            let mut snap = self
                .rpc_client
                .get_contract_state(self.extractor_id.chain, &filters, &body)
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
                    .filter_map(|comp| {
                        if comp.contract_ids.is_empty() {
                            return None;
                        }
                        let account_snapshots: Vec<_> = comp
                            .contract_ids
                            .iter()
                            .map(|cid| snap.remove(cid).unwrap())
                            .collect();
                        Some(SnapOrDelta::VMSnapshot(VMSnapshot {
                            component: comp.clone(),
                            state: account_snapshots,
                        }))
                    })
                    .collect(),
                removed_components: HashSet::new(),
                removed_contracts: HashSet::new(),
            })
        } else {
            // TODO: handle native protocols
            todo!()
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
                        removed_components: HashSet::new(),
                        removed_contracts: HashSet::new(),
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
