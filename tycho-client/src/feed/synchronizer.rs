use std::{collections::HashMap, sync::Arc};

use tokio::{
    sync::{
        mpsc::{channel, Receiver, Sender},
        Mutex, RwLock,
    },
    task::JoinHandle,
};
use tycho_types::{
    dto::{
        Block, BlockAccountChanges, BlockParam, ExtractorIdentity, ResponseAccount,
        StateRequestBody, StateRequestParameters, VersionParam,
    },
    Bytes,
};

use crate::{deltas::DeltasClient, rpc::RPCClient, HttpRPCClient, WsDeltasClient};

type SyncResult<T> = anyhow::Result<T>;

#[derive(Clone)]
pub struct StateSynchronizer {
    extractor_id: ExtractorIdentity,
    rpc_client: HttpRPCClient,
    deltas_client: WsDeltasClient,
    // we will need to request a snapthot for components that we did not emit a
    // snapshot for yet but are relevant now, e.g. because min tvl threshold exceeded.
    last_snapshot_components: Vec<()>,
    last_served_block_hash: Arc<Mutex<Option<Bytes>>>,
    last_synced_block: Arc<Mutex<Option<Block>>>,
    pending_deltas: Arc<Mutex<HashMap<Bytes, StateMsg>>>,
}

pub enum StateMsg {
    Snapshot { block: Block, state: Vec<ResponseAccount> },
    Deltas(BlockAccountChanges),
}

impl StateMsg {
    pub fn get_block(&self) -> &Block {
        match self {
            StateMsg::Snapshot { block, state } => block,
            StateMsg::Deltas(delta) => &delta.block,
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
                if let StateMsg::Deltas(delta) = &data {
                    current_hash = delta.block.parent_hash.clone();
                    to_serve.push(data);
                } else {
                    to_serve.push(data);
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

    pub fn start(&self) -> SyncResult<(JoinHandle<SyncResult<()>>, Receiver<Block>)> {
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

    async fn state_sync(self, block_tx: &mut Sender<Block>) -> SyncResult<()> {
        let (_, mut msg_rx) = self
            .deltas_client
            .subscribe(self.extractor_id.clone())
            .await?;
        let first_msg = msg_rx
            .recv()
            .await
            .ok_or_else(|| anyhow::format_err!("Subscription ended too soon"))?;

        let block = first_msg.block.clone();
        let params = StateRequestParameters {
            chain: self.extractor_id.chain,
            tvl_gt: None,
            inertia_min_gt: None,
        };
        let body = StateRequestBody {
            contract_ids: None,
            version: VersionParam::new(None, Some(BlockParam::from(&block))),
        };
        let snapshot = self
            .rpc_client
            .get_contract_state(&params, &body)
            .await?;
        self.pending_deltas.lock().await.insert(
            first_msg.block.hash.clone(),
            StateMsg::Snapshot { block: block.clone(), state: snapshot.accounts },
        );
        *self.last_synced_block.lock().await = Some(block.clone());
        block_tx.send(block.clone()).await?;
        loop {
            if let Some(deltas) = msg_rx.recv().await {
                let block = deltas.block.clone();
                self.pending_deltas
                    .lock()
                    .await
                    .insert(block.hash.clone(), StateMsg::Deltas(deltas));
                *self.last_synced_block.lock().await = Some(block.clone());
                block_tx.send(block).await?;
            } else {
                self.pending_deltas.lock().await.clear();
                *self.last_synced_block.lock().await = None;
                *self.last_served_block_hash.lock().await = None;
                return Ok(());
            }
        }
    }
}
