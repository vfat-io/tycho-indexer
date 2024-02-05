use std::{collections::HashMap, sync::Arc};

use tokio::{
    sync::{
        mpsc::{channel, Receiver, Sender},
        Mutex,
    },
    task::JoinHandle,
};
use tycho_types::{
    dto::{
        Block, BlockAccountChanges, BlockParam, Deltas, ExtractorIdentity, ResponseAccount,
        StateRequestBody, StateRequestParameters, VersionParam,
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
    // we will need to request a snapshot for components that we did not emit a
    // snapshot for yet but are relevant now, e.g. because min tvl threshold exceeded.
    // Currently waiting for necessary endpoints to be implemented.
    #[allow(dead_code)]
    last_snapshot_components: Vec<()>,
    last_served_block_hash: Arc<Mutex<Option<Bytes>>>,
    last_synced_block: Arc<Mutex<Option<Header>>>,
    // Since we may return a mix of snapshots and deltas (e.g. if a new component crossed the tvl
    // threshold) the value type is a vector.
    pending_deltas: Arc<Mutex<HashMap<Bytes, Vec<StateMsg>>>>,
    // Waiting for the rpc endpoints to support tvl.
    #[allow(dead_code)]
    min_tvl_threshold: f64,
}

pub enum StateMsg {
    Snapshot { header: Header, state: Vec<ResponseAccount> },
    Deltas { header: Header, deltas: Deltas },
}

impl StateMsg {
    pub fn get_block(&self) -> &Header {
        match self {
            StateMsg::Snapshot { header, .. } => header,
            StateMsg::Deltas { header, .. } => header,
        }
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

    pub async fn get_pending(&self, block_hash: Bytes) -> SyncResult<Vec<StateMsg>> {
        // Batch collect all changes up to the requested block
        // Must either hit a snapshot, or the last served block hash if it does not it errors
        let mut state = self.pending_deltas.lock().await;
        let mut target_hash = self.last_served_block_hash.lock().await;

        let mut current_hash = block_hash.clone();
        let mut to_serve = Vec::new();

        // If this is the first request, we have to hit a snapshot else this will error!
        while Some(&current_hash) != target_hash.as_ref() {
            if let Some(data) = state.remove(&current_hash) {
                let mut is_all_snapshots = true;
                for msg in data.into_iter() {
                    if let StateMsg::Deltas { header, deltas } = &msg {
                        current_hash = deltas.get_block().parent_hash.clone();
                        to_serve.push(msg);
                        is_all_snapshots = false;
                    } else {
                        to_serve.push(msg);
                        is_all_snapshots &= true;
                    }
                }
                if is_all_snapshots {
                    break;
                }
            } else {
                // TODO inform if we were looking for a snapshot or not
                anyhow::bail!("hash not found");
            }
        }
        *target_hash = Some(block_hash);
        Ok(to_serve)
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

    async fn state_sync(self, block_tx: &mut Sender<Header>) -> SyncResult<()> {
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

        let block = first_msg.get_block().clone();
        let params = StateRequestParameters { tvl_gt: None, inertia_min_gt: None };
        let body = StateRequestBody {
            contract_ids: None,
            version: VersionParam::new(None, Some(BlockParam::from(&block))),
        };
        // TODO: only request components we are interested in
        let snapshot = self
            .rpc_client
            .get_contract_state(self.extractor_id.chain, &params, &body)
            .await?;
        let header = Header::from_block(first_msg.get_block(), first_msg.is_revert());
        self.pending_deltas.lock().await.insert(
            header.hash.clone(),
            vec![StateMsg::Snapshot { header: header.clone(), state: snapshot.accounts }],
        );
        *self.last_synced_block.lock().await = Some(header.clone());
        block_tx.send(header.clone()).await?;
        loop {
            if let Some(deltas) = msg_rx.recv().await {
                let header = Header::from_block(deltas.get_block(), deltas.is_revert());
                // TODO: Three branches here, either we are not interested in the delta, we already
                // have been interested or we just became interested (in this case we need to get a
                // snapshot first though)
                self.pending_deltas.lock().await.insert(
                    block.hash.clone(),
                    vec![StateMsg::Deltas { header: header.clone(), deltas }],
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
