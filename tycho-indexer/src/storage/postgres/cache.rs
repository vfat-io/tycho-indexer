use diesel_async::{
    pooled_connection::deadpool::Pool, scoped_futures::ScopedFutureExt, AsyncConnection,
    AsyncPgConnection,
};
use ethers::types::H256;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

use crate::{
    extractor::evm::{self, EVMStateGateway},
    models::{Chain, ExtractionState},
    storage::{BlockIdentifier, StorageError},
};

#[allow(dead_code)]
enum WriteOp {
    UpsertBlock(evm::Block),
    UpsertTx(evm::Transaction),
    SaveExtractionState(ExtractionState),
    InsertContract(evm::Account),
    UpdateContracts(Vec<(H256, evm::AccountUpdate)>),
    RevertContractState(BlockIdentifier),
}
#[allow(dead_code)]
struct Transaction {
    block: evm::Block,
    operations: Vec<WriteOp>,
    tx: oneshot::Sender<Result<(), StorageError>>,
}

/// # Write Cache
///
/// This struct handles writes in a centralised and sequential manner. It
/// provides a write-through cache through message passing. This means multiple
/// "writers" can send transactions of write operations simultenously. Each of
/// those transactions is supposed to relate to a block. As soon as a new block
/// is observed, the currently pending changes are flushed to the database.
///
/// In case a new transaction with an older block comes in, the transaction is
/// immediately applied to the database.
///
/// In case a the incoming transactions block is too far ahead / does not
/// connect with the last persisted block, an error is raised.
///
/// Transactions operations are deduplicated, but are executed as separate
/// database transactions therefore in case a transaction fails, it should not
/// affect any other pending transactions.
///
/// ## Deduplication
/// Block, transaction and revert operations are deduplicated. Meaning that if
/// they happen within a batch, they will only be sent once to the actual
/// database.
///
/// ## Design Decisions
/// The current design is bound to evm and diesel models. The bound is
/// purposfully kept somewhat decoupled but not entirely. The reason is to
/// ensure fast development but also have a path that shows how we could
/// decouple especially from evm bounds models, as most likely we will soon have
/// additional chains to deal with.
///
/// Read Operations
/// The class does provide read operations for completeness, but it will not consider any
/// cached changes while reading. Any reads are direct pass throughs to the database.
#[allow(dead_code)]
struct WriteThroughCache {
    name: String,
    chain: Chain,
    pool: Pool<AsyncPgConnection>,
    state_gateway: EVMStateGateway<AsyncPgConnection>,
    persisted_block: evm::Block,
    pending_block: Option<evm::Block>,
    pending_txns: Vec<Transaction>,
    rx: mpsc::Receiver<Transaction>,
}
#[allow(dead_code)]
impl WriteThroughCache {
    pub async fn write(&mut self, tx: Transaction) -> Result<(), StorageError> {
        let incoming = tx.block;
        match self.pending_block {
            Some(pending) => {
                if pending.hash == incoming.hash {
                    self.pending_txns.push(tx);
                } else if incoming.number < pending.number {
                    let mut conn = self
                        .pool
                        .get()
                        .await
                        .expect("pool should be connected");

                    conn.transaction(|conn| {
                        async {
                            self.execute_tx(tx.operations, conn)
                                .await
                        }
                        .scope_boxed()
                    })
                    .await?;
                } else if incoming.parent_hash == pending.hash {
                    // TODO: handle errors
                    self.flush().await;
                } else {
                    return Err(StorageError::Unexpected("Invalid cache state!".into()));
                }
            }
            None => {
                self.pending_block = Some(tx.block);
                self.pending_txns.push(tx);
            }
        }
        Ok(())
    }

    async fn execute_tx(
        &mut self,
        #[allow(unused_variables)] ops: Vec<WriteOp>,
        #[allow(unused_variables)] conn: &mut AsyncPgConnection,
    ) -> Result<(), StorageError> {
        todo!();
    }

    async fn execute_write(
        &mut self,
        #[allow(unused_variables)] operation: WriteOp,
        #[allow(unused_variables)] conn: &mut AsyncPgConnection,
    ) -> Result<(), StorageError> {
        todo!();
    }

    async fn flush(&mut self) {
        // We flush e.g if any historical data is requested that
        // can't be satisfied from the internal cache.
        // This should only happen during reverts or if someone
        // requests data that is not yet in the database
        let mut conn = self
            .pool
            .get()
            .await
            .expect("pool should be connected");

        let txns = std::mem::take(&mut self.pending_txns);
        for tx in txns.into_iter() {
            conn.transaction(|conn| {
                async {
                    self.execute_tx(tx.operations, conn)
                        .await
                }
                .scope_boxed()
            })
            .await
            .expect("tx ok");

            // TODO: once executed, remove any duplicated operations from the remaining transactions
        }

        self.persisted_block = self.pending_block.unwrap();
        self.pending_block = None
    }

    pub fn run(self) -> JoinHandle<()> {
        // read from rx and call write
        todo!()
    }
}

#[allow(dead_code)]
pub struct CachedGateway {
    tx: mpsc::Sender<Transaction>,
    pool: Pool<AsyncPgConnection>,
    state_gateway: EVMStateGateway<AsyncPgConnection>,
}

impl CachedGateway {
    // TODO: implement the usual gateway methods here, but they are translated into write ops, for
    // reads call the gateway directly
    #[allow(dead_code)]
    pub async fn upsert_block(
        &self,
        #[allow(unused_variables)] new: &evm::Block,
    ) -> Result<(), StorageError> {
        todo!()
    }
}

#[allow(dead_code)]
pub fn new_cached_gateway() -> Result<(JoinHandle<()>, CachedGateway), StorageError> {
    todo!()
}
