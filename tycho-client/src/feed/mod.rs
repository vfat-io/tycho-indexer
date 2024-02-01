use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use lru::LruCache;
use tokio::{
    sync::{
        mpsc::{channel, Receiver, Sender},
        RwLock,
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
struct Synchronizer {
    extractor_id: ExtractorIdentity,
    rpc_client: HttpRPCClient,
    deltas_client: WsDeltasClient,
    pending_deltas: Arc<RwLock<HashMap<Bytes, StateMsg>>>,
}

enum StateMsg {
    Snapshot(Vec<ResponseAccount>),
    Deltas(Vec<BlockAccountChanges>),
}

impl Synchronizer {
    pub fn new(
        extracor_id: ExtractorIdentity,
        rpc_client: HttpRPCClient,
        deltas_client: WsDeltasClient,
    ) -> Self {
        todo!();
    }

    pub async fn get_pending(&self, block_id: Bytes) -> SyncResult<Vec<StateMsg>> {
        // Batch collect all changes up to the requested block
        todo!();
    }

    pub fn start(&self) -> SyncResult<(JoinHandle<SyncResult<()>>, Receiver<Block>)> {
        let (block_tx, block_rx) = channel(15);
        let jh = tokio::spawn(async move { Ok(()) });

        Ok((jh, block_rx))
    }

    async fn state_sync(self, block_tx: Sender<Block>) -> SyncResult<()> {
        let (_, mut msg_rx) = self
            .deltas_client
            .subscribe(self.extractor_id.clone())
            .await?;
        let first_msg = msg_rx
            .recv()
            .await
            .ok_or_else(|| anyhow::format_err!("Subscription ended too soon"))?;

        let params = StateRequestParameters {
            chain: self.extractor_id.chain,
            tvl_gt: None,
            inertia_min_gt: None,
        };
        let body = StateRequestBody {
            contract_ids: None,
            version: VersionParam::new(None, Some(BlockParam::from(first_msg.block.clone()))),
        };
        let snapshot = self
            .rpc_client
            .get_contract_state(&params, &body)
            .await?;
        self.pending_deltas
            .blocking_write()
            .insert(first_msg.block.hash.clone(), StateMsg::Snapshot(snapshot.accounts));

        loop {
            if let Some(deltas) = msg_rx.recv().await {
                let block = deltas.block.clone();
                self.pending_deltas
                    .blocking_write()
                    .insert(block.hash.clone(), StateMsg::Deltas(vec![deltas]));
                block_tx.send(block).await?;
            } else {
                return Ok(());
            }
        }
    }
}
