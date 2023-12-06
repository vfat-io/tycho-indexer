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
#[derive(PartialEq)]
enum WriteOp {
    UpsertBlock(evm::Block),
    UpsertTx(evm::Transaction),
    SaveExtractionState(ExtractionState),
    InsertContract(evm::Account),
    UpdateContracts(Vec<(H256, evm::AccountUpdate)>),
    RevertContractState(BlockIdentifier), //add a tx here
}

struct DBTransaction {
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
struct DBCacheWriteExecutor {
    name: String,
    chain: Chain,
    pool: Pool<AsyncPgConnection>,
    state_gateway: EVMStateGateway<AsyncPgConnection>,
    persisted_block: evm::Block,
    /// The `pending_block` field denotes the most recent block that is pending processing in this
    /// cache context. It's important to note that this is distinct from the blockchain's
    /// concept of a pending block. Typically, this block corresponds to the latest block that
    /// has been validated and confirmed on the blockchain.
    pending_block: Option<evm::Block>,
    pending_db_txs: Vec<DBTransaction>,
    rx: mpsc::Receiver<DBTransaction>,
}

impl DBCacheWriteExecutor {
    pub async fn write(&mut self, db_tx: DBTransaction) -> Result<(), StorageError> {
        let incoming = db_tx.block;
        match self.pending_block {
            Some(pending) => {
                if pending.hash == incoming.hash {
                    self.pending_db_txs.push(db_tx);
                } else if incoming.number < pending.number {
                    let mut conn = self
                        .pool
                        .get()
                        .await
                        .expect("pool should be connected");

                    conn.transaction(|conn| {
                        async {
                            self.execute_many_write_ops(db_tx.operations, conn)
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
                self.pending_block = Some(db_tx.block);
                self.pending_db_txs.push(db_tx);
            }
        }
        Ok(())
    }

    async fn execute_many_write_ops(
        &mut self,
        ops: Vec<WriteOp>,
        conn: &mut AsyncPgConnection,
    ) -> Result<(), StorageError> {
        todo!();
    }

    async fn execute_write(
        &mut self,
        operation: WriteOp,
        conn: &mut AsyncPgConnection,
    ) -> Result<(), StorageError> {
        todo!();
    }

    /// Commits the current cached state to the database in a single batch operation.
    /// Extracts and deduplicates write operations from `pending_db_txs`, executes them,
    /// updates `persisted_block` with `pending_block`, and sets `pending_block` to `None`.
    async fn flush(&mut self) {
        let mut conn = self
            .pool
            .get()
            .await
            .expect("pool should be connected");

        let db_txs = std::mem::take(&mut self.pending_db_txs);
        let mut db_operations: Vec<WriteOp> = Vec::new();

        for db_tx in db_txs.into_iter() {
            for op in db_tx.operations {
                // TODO: It's more efficient to use a hashset here
                if !db_operations.contains(&op) {
                    // Only insert into db_operations if it was not already in seen_operations
                    db_operations.push(op);
                }
            }
        }

        // Send the batch to the database
        conn.transaction(|conn| {
            async {
                self.execute_many_write_ops(db_operations, conn)
                    .await
            }
            .scope_boxed()
        })
        .await
        .expect("tx ok");

        self.persisted_block = self.pending_block.unwrap();
        self.pending_block = None;
    }

    pub fn run(self) -> JoinHandle<()> {
        // read from rx and call write
        todo!()
    }
}

pub struct CachedGateway {
    tx: mpsc::Sender<DBTransaction>,
    pool: Pool<AsyncPgConnection>,
    state_gateway: EVMStateGateway<AsyncPgConnection>,
}

impl CachedGateway {
    // TODO: implement the usual gateway methods here, but they are translated into write ops, for
    // reads call the gateway directly
    pub async fn upsert_block(&self, new: &evm::Block) -> Result<(), StorageError> {
        todo!()
    }
}

pub fn new_cached_gateway() -> Result<(JoinHandle<()>, CachedGateway), StorageError> {
    todo!()
}
