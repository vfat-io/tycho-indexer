use std::{collections::HashMap, time::Duration};

use chrono::{Local, NaiveDateTime};
use futures03::{future::join_all, stream::FuturesUnordered};

use tokio::{
    sync::mpsc::{self, Receiver},
    task::JoinHandle,
    time::timeout,
};
use tracing::{info, warn};
use tycho_types::dto::{Block, ExtractorIdentity};

use crate::{deltas::DeltasClient, HttpRPCClient, WsDeltasClient};

use self::{
    block_history::{BlockHistory, BlockPosition},
    synchronizer::{StateMsg, StateSynchronizer},
};

mod block_history;
mod synchronizer;

type BlockSyncResult<T> = anyhow::Result<T>;

pub struct BlockSynchronizer {
    rpc_client: HttpRPCClient,
    deltas_client: WsDeltasClient,
    synchronizers: HashMap<ExtractorIdentity, StateSynchronizer>,
}

enum SynchronizerState {
    Started,
    Ready(Block),
    // TODO: looks like stale and delayed are the same, if a delayed synchronizer makes basically
    // no progress, we consider it stale at that point we should purge it.
    Stale(Block),
    Delayed(Block),
    // For this to happen we must have a gap, and a gap usually means a new snapshot from the
    // StateSynchroniser. This can only happen if we are processing too slow and the one of the
    // synchronisers restarts e.g. because tycho ended the subscription.
    Advanced(Block),
    Ended,
}

pub struct SynchronizerHandle {
    state: SynchronizerState,
    sync: StateSynchronizer,
    modfiy_ts: NaiveDateTime,
    rx: Receiver<Block>,
}

impl SynchronizerHandle {
    async fn try_advance(&mut self, block_history: &BlockHistory, max_wait: u64) {
        match &self.state {
            SynchronizerState::Started | SynchronizerState::Ended => {
                // TODO: warn, should not happen, ignore
            }
            SynchronizerState::Advanced(b) => {
                let future_block = b.clone();
                // Transition to ready once we arrived at the expected height
                self.transition(future_block, block_history);
            }
            SynchronizerState::Ready(previous_block) => {
                // Try to recv the next expected block, update state accordingly.
                self.handle_ready_state(max_wait, block_history, previous_block.clone())
                    .await;
            }
            SynchronizerState::Delayed(_) => {
                // try to catch up all currently queued blocks until the expected block
                self.try_catch_up(block_history, max_wait)
                    .await;
            }
            SynchronizerState::Stale(_) => {
                self.try_catch_up(block_history, max_wait)
                    .await;
            }
        }
    }

    async fn handle_ready_state(
        &mut self,
        max_wait: u64,
        block_history: &BlockHistory,
        previous_block: Block,
    ) {
        match timeout(Duration::from_millis(max_wait), self.rx.recv()).await {
            Ok(Some(b)) => self.transition(b, block_history),
            Ok(None) => {
                self.state = SynchronizerState::Ended;
                self.modfiy_ts = Local::now().naive_utc();
            }
            Err(_) => {
                // trying to advance a block timed out
                self.state = SynchronizerState::Delayed(previous_block.clone());
            }
        }
    }

    async fn try_catch_up(&mut self, block_history: &BlockHistory, max_wait: u64) {
        let mut results = Vec::new();
        while let Ok(block) = self.rx.try_recv() {
            let block_pos = block_history
                .determine_block_position(&block)
                .unwrap();
            results.push(Ok(Some(block)));
            if matches!(block_pos, BlockPosition::Latest) {
                break;
            }
        }
        results.push(timeout(Duration::from_millis(max_wait), self.rx.recv()).await);

        let mut blocks = results
            .into_iter()
            .filter_map(|e| match e {
                Ok(Some(b)) => Some(b),
                _ => None,
            })
            .collect::<Vec<_>>();

        if let Some(b) = blocks.pop() {
            // we were able to get at least one block out
            self.transition(b, block_history);
        }
    }

    fn transition(&mut self, latest_retrieved: Block, block_history: &BlockHistory) {
        // TODO: special handling for reverts required here
        match block_history
            .determine_block_position(&latest_retrieved)
            .expect("Block positiion could not be determined.")
        {
            BlockPosition::NextExpected => {
                self.state = SynchronizerState::Ready(latest_retrieved.clone());
            }
            BlockPosition::Latest | BlockPosition::Delayed => {
                let now = Local::now().naive_utc();
                if self
                    .modfiy_ts
                    .signed_duration_since(now) >
                    chrono::Duration::seconds(60)
                {
                    // TODO: warn
                    self.state = SynchronizerState::Stale(latest_retrieved.clone());
                } else {
                    self.state = SynchronizerState::Delayed(latest_retrieved.clone());
                }
            }
            BlockPosition::Advanced => {
                // TODO: warn
                self.state = SynchronizerState::Advanced(latest_retrieved.clone());
            }
        }
        self.modfiy_ts = Local::now().naive_utc();
    }
}

impl BlockSynchronizer {
    pub async fn run(
        mut self,
    ) -> BlockSyncResult<(JoinHandle<BlockSyncResult<()>>, Receiver<Vec<StateMsg>>)> {
        let ws_task = self.deltas_client.connect().await?;
        let state_sync_tasks = FuturesUnordered::new();
        // TODO: fill with last X blocks
        let mut block_history = BlockHistory::new(vec![], 15);
        let mut state_synchronizers = HashMap::with_capacity(self.synchronizers.len());

        // No await in here so all synchronizers start at the same time!!
        for (extractor_id, synchronizer) in self.synchronizers.drain() {
            let (jh, rx) = synchronizer.start()?;
            state_sync_tasks.push(jh);
            state_synchronizers.insert(
                extractor_id.clone(),
                SynchronizerHandle {
                    state: SynchronizerState::Started,
                    sync: synchronizer,
                    modfiy_ts: Local::now().naive_utc(),
                    rx,
                },
            );
        }

        // startup, schedule first set of futures and wait for them to return to initialise
        // synchronisers.
        let mut startup_futures = Vec::new();
        for (id, sh) in state_synchronizers.iter_mut() {
            let fut = async {
                let res = timeout(Duration::from_secs(12), sh.rx.recv()).await;
                (id.clone(), res)
            };
            startup_futures.push(fut);
        }
        join_all(startup_futures)
            .await
            .into_iter()
            .for_each(|(extractor_id, res)| {
                let synchronizer = state_synchronizers
                    .get_mut(&extractor_id)
                    .unwrap();
                match res {
                    Ok(Some(b)) => {
                        synchronizer.state = SynchronizerState::Ready(b);
                    }
                    Ok(None) => {
                        warn!(?extractor_id, "Dead synchronizer at startup will be purged!");
                        synchronizer.state = SynchronizerState::Ended;
                    }
                    Err(_) => {
                        warn!(?extractor_id, "Stale synchronizer at startup will be purged!");
                        synchronizer.state = SynchronizerState::Ended;
                    }
                }
            });

        // Purge any stale synchronisers, require rest to be ready on the same block, else fail.
        // It's probably worth doing more complex things here if this is problematic e.g. setting
        // some as delayed and waiting for those to catch up, but I want to go with the simplest
        // solution first.
        state_synchronizers.retain(|_, v| matches!(v.state, SynchronizerState::Ready(_)));
        let blocks: Vec<_> = state_synchronizers
            .values()
            .into_iter()
            .map(|v| match v.state {
                SynchronizerState::Ready(ref b) => b,
                _ => panic!("Unreachable"),
            })
            .collect();

        if let Some(first) = blocks.first().copied() {
            if !blocks.iter().all(|v| first == *v) {
                anyhow::bail!("not all synchronizers on same block!")
            }
            let start_block = first;
            let n_healthy = blocks.len();
            info!(?start_block, ?n_healthy, "Block synchronisation started successfully!")
        } else {
            anyhow::bail!("Not a single synchronizer healthy!")
        }

        block_history
            .push(blocks[0].clone())
            .expect("history not avail");
        let (sync_tx, sync_rx) = mpsc::channel(30);
        let jh = tokio::spawn(async move {
            loop {
                // Pull and send data from ready synchronisers
                let mut pulled_data_fut = Vec::new();
                for (id, sh) in state_synchronizers.iter() {
                    match &sh.state {
                        SynchronizerState::Ready(_) => {
                            let sync = self
                                .synchronizers
                                .get(id)
                                .expect("state invalid");
                            pulled_data_fut.push(
                                sync.get_pending(
                                    block_history
                                        .latest()
                                        .expect("block")
                                        .hash
                                        .clone(),
                                ),
                            );
                        }
                        _ => continue,
                    }
                }
                let synced_msgs = join_all(pulled_data_fut)
                    .await
                    .into_iter()
                    .filter_map(|r| match r {
                        Ok(v) => Some(v),
                        Err(_) => None,
                    })
                    .flatten()
                    .collect::<Vec<_>>();
                sync_tx.send(synced_msgs).await?;

                // Wait for the next emission, this is guaranteed to return within the specified
                // timeout
                let mut recv_futures = Vec::new();
                for sh in state_synchronizers.values_mut() {
                    recv_futures.push(sh.try_advance(&block_history, 300));
                }
                join_all(recv_futures).await;

                // Purge any bad synchronizers
                state_synchronizers.retain(|_, v| match v.state {
                    SynchronizerState::Started | SynchronizerState::Ended => false,
                    SynchronizerState::Stale(_) => false,
                    SynchronizerState::Ready(_) => true,
                    SynchronizerState::Delayed(_) => true,
                    SynchronizerState::Advanced(_) => true,
                });

                block_history
                    .push(
                        state_synchronizers
                            .values()
                            .into_iter()
                            .filter_map(|v| match &v.state {
                                SynchronizerState::Ready(b) => Some(b),
                                _ => None,
                            })
                            .next()
                            .expect("Did not advance")
                            .clone(),
                    )
                    .expect("history bad");
            }
            Ok(())
        });
        // enter the business as usual loop, query the synchronizer for the last emitted block
        // then schedule the wait procedure
        // We know the next block number we expect - this helps us detect gaps (and reverts but they
        // don't require special handling)
        // So the wait procdure consists in waiting for any of the receivers to emit a new block
        // we check if that block corresponds with what we expected - if that is the case we deem
        // the respective synchronizer as ready.
        // At this point a timeout starts for the remaining channels, as soon as they reply, we
        // again check against the expected block number and if they all reply within the timeout
        // they are all considered as ready.
        //
        // Now when either all receivers emitted a block or the timeout passed, we are ready for the
        // next step. At this point each synchronizer can be at one of the following states.
        // 1. Ready, it emitted the expected block, or a block of same height within the timeout
        // 2. Advanced, it emitted a future block within the timeout: likely a reconnect happened
        //    and we are too slow to process if this happens
        // 3. Stale, it did not emit anything within the timeout: Connection issue, or firehose too
        //    slow.
        // 4. Delayed, it emitted an older block than expected: Can happen if the extractor has to
        //    start syncing and falls behind others.
        //
        // We will only query synchronizer for state if they are in the ready state or if they are
        // advanced and have state present for the current block. For others we will inform
        // clients about their current sync state.
        //
        // Any extractors that have been stale or delayed for too long we will stop waiting for - so
        // we will end their syncronizers.
        // Any advanced extractors, we will not wait for in the future, they will be assumed ready
        // until we reach their height.

        Ok((jh, sync_rx))
    }
}
