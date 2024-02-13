use std::collections::HashMap;

use chrono::{Local, NaiveDateTime};
use futures03::{future::join_all, stream::FuturesUnordered, StreamExt};

use crate::{deltas::DeltasClient, HttpRPCClient, WsDeltasClient};
use tokio::{
    select,
    sync::mpsc::{self, Receiver},
    task::JoinHandle,
    time::timeout,
};
use tracing::{debug, error, info, warn};
use tycho_types::{
    dto::{Block, ExtractorIdentity},
    Bytes,
};

use self::{
    block_history::{BlockHistory, BlockPosition},
    synchronizer::{StateSyncMessage, StateSynchronizer},
};

mod block_history;
mod component_tracker;
mod synchronizer;

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Header {
    pub hash: Bytes,
    pub number: u64,
    pub parent_hash: Bytes,
    pub revert: bool,
}

impl Header {
    fn from_block(block: &Block, revert: bool) -> Self {
        Self {
            hash: block.hash.clone(),
            number: block.number,
            parent_hash: block.parent_hash.clone(),
            revert,
        }
    }
}

type BlockSyncResult<T> = anyhow::Result<T>;

pub struct BlockSynchronizer {
    deltas_client: WsDeltasClient,
    synchronizers:
        Option<HashMap<ExtractorIdentity, StateSynchronizer<HttpRPCClient, WsDeltasClient>>>,
    block_time: std::time::Duration,
    max_wait: std::time::Duration,
}

#[derive(Clone)]
enum SynchronizerState {
    Started,
    Ready(Header),
    // TODO: looks like stale and delayed are the same, if a delayed synchronizer makes basically
    // no progress, we consider it stale at that point we should purge it.
    Stale(Header),
    Delayed(Header),
    // For this to happen we must have a gap, and a gap usually means a new snapshot from the
    // StateSynchroniser. This can only happen if we are processing too slow and the one of the
    // synchronisers restarts e.g. because tycho ended the subscription.
    Advanced(Header),
    Ended,
}

pub struct SynchronizerHandle {
    extractor_id: ExtractorIdentity,
    state: SynchronizerState,
    sync: StateSynchronizer<HttpRPCClient, WsDeltasClient>,
    modify_ts: NaiveDateTime,
    rx: Receiver<Header>,
}

impl SynchronizerHandle {
    async fn try_advance(&mut self, block_history: &BlockHistory, max_wait: std::time::Duration) {
        let extractor_id = &self.extractor_id;
        let latest_block = block_history.latest();
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
            SynchronizerState::Delayed(old_block) => {
                // try to catch up all currently queued blocks until the expected block
                debug!(
                    ?old_block,
                    ?latest_block,
                    ?extractor_id,
                    "Trying to catch up to latest block"
                );
                self.try_catch_up(block_history, max_wait)
                    .await;
            }
            SynchronizerState::Stale(old_block) => {
                debug!(
                    ?old_block,
                    ?latest_block,
                    ?extractor_id,
                    "Trying to catch up to latest block"
                );
                self.try_catch_up(block_history, max_wait)
                    .await;
            }
        }
    }

    async fn handle_ready_state(
        &mut self,
        max_wait: std::time::Duration,
        block_history: &BlockHistory,
        previous_block: Header,
    ) {
        let extractor_id = &self.extractor_id;
        match timeout(max_wait, self.rx.recv()).await {
            Ok(Some(b)) => self.transition(b, block_history),
            Ok(None) => {
                error!(
                    ?extractor_id,
                    ?previous_block,
                    "Extractor terminated: closed header channel!"
                );
                self.state = SynchronizerState::Ended;
                self.modify_ts = Local::now().naive_utc();
            }
            Err(_) => {
                // trying to advance a block timed out
                debug!(?extractor_id, ?previous_block, "Extractor did not check in within time.");
                self.state = SynchronizerState::Delayed(previous_block.clone());
            }
        }
    }

    async fn try_catch_up(&mut self, block_history: &BlockHistory, max_wait: std::time::Duration) {
        let mut results = Vec::new();
        let extractor_id = &self.extractor_id;
        while let Ok(block) = self.rx.try_recv() {
            let block_pos = block_history
                .determine_block_position(&block)
                .unwrap();
            results.push(Ok(Some(block)));
            if matches!(block_pos, BlockPosition::Latest) {
                debug!(?extractor_id, "Extractor managed to catch up to latest state!");
                break;
            }
        }
        results.push(timeout(max_wait, self.rx.recv()).await);

        let mut blocks = results
            .into_iter()
            .filter_map(|e| match e {
                Ok(Some(b)) => Some(b),
                _ => None,
            })
            .collect::<Vec<_>>();

        if let Some(b) = blocks.pop() {
            // we were able to get at least one block out
            debug!(?extractor_id, "Extractor managed to catch up to ready state!");
            self.transition(b, block_history);
        }
    }

    fn transition(&mut self, latest_retrieved: Header, block_history: &BlockHistory) {
        let extractor_id = &self.extractor_id;
        let last_message_at = self.modify_ts;
        let block = &latest_retrieved;
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
                    .modify_ts
                    .signed_duration_since(now) >
                    chrono::Duration::seconds(60)
                {
                    warn!(
                        ?extractor_id,
                        ?last_message_at,
                        ?block,
                        "Extractor transition to stale."
                    );
                    self.state = SynchronizerState::Stale(latest_retrieved.clone());
                } else {
                    warn!(
                        ?extractor_id,
                        ?last_message_at,
                        ?block,
                        "Extractor transition to delayed."
                    );
                    self.state = SynchronizerState::Delayed(latest_retrieved.clone());
                }
            }
            BlockPosition::Advanced => {
                error!(
                    ?extractor_id,
                    ?last_message_at,
                    ?block,
                    "Extractor transition to advanced."
                );
                self.state = SynchronizerState::Advanced(latest_retrieved.clone());
            }
        }
        self.modify_ts = Local::now().naive_utc();
    }
}

pub struct FeedMessage {
    state_msgs: HashMap<String, StateSyncMessage>,
    sync_states: HashMap<String, SynchronizerState>,
}

impl FeedMessage {
    fn new(
        state_msgs: HashMap<String, StateSyncMessage>,
        sync_states: HashMap<String, SynchronizerState>,
    ) -> Self {
        Self { state_msgs, sync_states }
    }
}

impl BlockSynchronizer {
    pub async fn run(mut self) -> BlockSyncResult<(JoinHandle<()>, Receiver<FeedMessage>)> {
        let ws_task = self.deltas_client.connect().await?;
        let mut state_sync_tasks = FuturesUnordered::new();
        let mut synchronizers = self
            .synchronizers
            .take()
            .expect("No synchronisers set!");
        let mut sync_handles = HashMap::with_capacity(synchronizers.len());

        for (extractor_id, synchronizer) in synchronizers.drain() {
            let (jh, rx) = synchronizer.start().await?;
            state_sync_tasks.push(jh);
            sync_handles.insert(
                extractor_id.clone(),
                SynchronizerHandle {
                    extractor_id,
                    state: SynchronizerState::Started,
                    sync: synchronizer,
                    modify_ts: Local::now().naive_utc(),
                    rx,
                },
            );
        }

        // startup, schedule first set of futures and wait for them to return to initialise
        // synchronisers.
        let mut startup_futures = Vec::new();
        for (id, sh) in sync_handles.iter_mut() {
            let fut = async {
                let res = timeout(self.block_time.into(), sh.rx.recv()).await;
                (id.clone(), res)
            };
            startup_futures.push(fut);
        }
        join_all(startup_futures)
            .await
            .into_iter()
            .for_each(|(extractor_id, res)| {
                let synchronizer = sync_handles
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
        sync_handles.retain(|_, v| matches!(v.state, SynchronizerState::Ready(_)));
        let blocks: Vec<_> = sync_handles
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

        // Enter the main loop: query the synchronizer for the last emitted block, then schedule the
        // wait procedure for a next block.
        //
        // We know the next block number we expect - this helps us detect gaps (and reverts but they
        // don't require special handling)
        // So the wait procdure consists in waiting for any of the receivers to emit a new block
        // we check if that block corresponds with what we expected - if that is the case we deem
        // the respective synchronizer as ready.
        // At this point a timeout starts for the remaining channels, as soon as they reply, we
        // again check against the expected block number and if they all reply with the expected
        // next block within the timeout they are all considered as ready. (Currently not
        // implemented)
        //
        // Now when either all receivers emitted a block or the timeout passed, we are ready for the
        // next step. At this point each synchronizer can be at one of the following states.
        // 1. Ready, it emitted the expected block, or a block of same height within the timeout
        // 2. Advanced, it emitted a future block within the timeout: likely a reconnect happened
        //    and there is some significant back pressure on our channels. This basically means we
        //    are too slow to process messages.
        // 3. Delayed, it did not emit anything within the timeout, or an older block than expected:
        //    Extractor syncing, connection issue, or firehose too slow.
        //
        // We will only query synchronizer for state if they are in the ready state or if they are
        // advanced in which case we simply take the advanced state. For others we will inform
        // clients about their current sync state but not actually emit old data.
        //
        // Any extractors that have been stale or delayed for too long we will stop waiting for - so
        // we will end their syncronizers.
        //
        // Any advanced extractors, we will not wait for in the future, they will be assumed ready
        // until we reach their height.
        let mut block_history = BlockHistory::new(vec![blocks[0].clone()], 15);
        let (sync_tx, sync_rx) = mpsc::channel(30);
        let main_loop_jh: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
            loop {
                // Pull data from ready synchronisers
                let mut pulled_data_fut = Vec::new();
                let latest_block_hash = &block_history
                    .latest()
                    .ok_or_else(|| anyhow::format_err!("Block history was empty!"))?
                    .hash;

                for (id, sh) in sync_handles.iter() {
                    match &sh.state {
                        SynchronizerState::Ready(_) => {
                            pulled_data_fut.push(async {
                                sh.sync
                                    .get_pending(latest_block_hash.clone())
                                    .await
                                    .map(|x| (id.name.clone(), x))
                            });
                        }
                        _ => continue,
                    }
                }
                let synced_msgs = join_all(pulled_data_fut)
                    .await
                    .into_iter()
                    // justification to ignore missing data here? IMO indicates a bug if a
                    // ready extractor has not data available.
                    .filter_map(|x| x)
                    .collect::<HashMap<_, _>>();

                // Send retrieved data to receivers.
                sync_tx
                    .send(FeedMessage::new(
                        synced_msgs,
                        sync_handles
                            .iter()
                            .map(|(a, b)| (a.name.to_string(), b.state.clone()))
                            .collect(),
                    ))
                    .await?;

                // Here we simply wait block_time + max_wait. This will not work for chains with
                // unkown block times but is simple enough for now.
                // If we would like to support unkown block times we could: Instruct all handles to
                // await the max block time, if a header arrives within that time transition as
                // usual, but via a select statement get notified (using e.g. Notify) if any other
                // handle finishes before the timeout. Then await again but this time only for
                // max_wait and then proceed as usual. So basically each try_advance task would have
                // a select statement that allows it to exit the first timeout
                // preemtpively if any other try_advance task finished earlier.
                let mut recv_futures = Vec::new();
                for sh in sync_handles.values_mut() {
                    recv_futures
                        .push(sh.try_advance(&block_history, self.block_time + self.max_wait));
                }
                join_all(recv_futures).await;

                // Purge any bad synchronizers, respective warnings are issues at transition time.
                sync_handles.retain(|_, v| match v.state {
                    SynchronizerState::Started | SynchronizerState::Ended => false,
                    SynchronizerState::Stale(_) => false,
                    SynchronizerState::Ready(_) => true,
                    SynchronizerState::Delayed(_) => true,
                    SynchronizerState::Advanced(_) => true,
                });

                block_history
                    .push(
                        sync_handles
                            .values()
                            .into_iter()
                            .filter_map(|v| match &v.state {
                                SynchronizerState::Ready(b) => Some(b),
                                _ => None,
                            })
                            .next()
                            // no synchronizers is ready
                            .ok_or_else(|| {
                                anyhow::format_err!("Not a single snychronizer was ready.")
                            })?
                            .clone(),
                    )
                    .map_err(|err| anyhow::format_err!("Failed processing new block: {}", err))?;
            }
        });

        let nanny_jh = tokio::spawn(async move {
            select! {
                error = ws_task => {
                    error!(?error, "websocket task exited");
                }
                error = state_sync_tasks.select_next_some() => {
                    error!(?error, "state synchronizer exited");
                },
                error = main_loop_jh => {
                    error!(?error, "feed main loop exited");
                }
            }
        });
        Ok((nanny_jh, sync_rx))
    }
}
