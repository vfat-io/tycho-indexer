#![allow(dead_code)]
#![allow(unused_variables)]
use std::ops::{Deref, DerefMut};

use diesel_async::{
    pooled_connection::deadpool::Pool, scoped_futures::ScopedFutureExt, AsyncConnection,
    AsyncPgConnection,
};

use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tracing::log::debug;

use crate::{
    extractor::evm::{self, AccountUpdate, EVMStateGateway},
    models::{Chain, ExtractionState},
    storage::{BlockIdentifier, BlockOrTimestamp, StorageError, TxHash},
};

/// Represents different types of database write operations.
#[derive(PartialEq, Clone)]
enum WriteOp {
    UpsertBlock(evm::Block),
    UpsertTx(evm::Transaction),
    SaveExtractionState(ExtractionState),
    InsertContract(evm::Account),
    UpdateContracts(Vec<(TxHash, evm::AccountUpdate)>),
    RevertContractState(BlockIdentifier),
}

/// Represents a transaction in the database, including the block information,
/// a list of operations to be performed, and a channel to send the result.
struct DBTransaction {
    block: evm::Block,
    operations: Vec<WriteOp>,
    tx: oneshot::Sender<Result<(), StorageError>>,
}

/// Represents different types of messages that can be sent to the DBCacheWriteExecutor.
enum DBCacheMessage {
    Write(DBTransaction),
    Flush(oneshot::Sender<Result<(), StorageError>>),
}

/// # Write Cache
///
/// This struct handles writes in a centralised and sequential manner. It
/// provides a write-through cache through message passing. This means multiple
/// "writers" can send transactions of write operations simultaneously. Each of
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
    persisted_block: Option<evm::Block>,
    /// The `pending_block` field denotes the most recent block that is pending processing in this
    /// cache context. It's important to note that this is distinct from the blockchain's
    /// concept of a pending block. Typically, this block corresponds to the latest block that
    /// has been validated and confirmed on the blockchain.
    pending_block: Option<evm::Block>,
    pending_db_txs: Vec<DBTransaction>,
    rx: mpsc::Receiver<DBCacheMessage>,
}

impl DBCacheWriteExecutor {
    fn new(
        name: String,
        chain: Chain,
        pool: Pool<AsyncPgConnection>,
        state_gateway: EVMStateGateway<AsyncPgConnection>,
        rx: mpsc::Receiver<DBCacheMessage>,
    ) -> Self {
        Self {
            name,
            chain,
            pool,
            state_gateway,
            persisted_block: None,
            pending_block: None,
            pending_db_txs: Vec::<DBTransaction>::new(),
            rx,
        }
    }

    /// Spawns a task to process incoming database messages (write requests or flush commands).
    pub fn run(mut self) -> JoinHandle<()> {
        tokio::spawn(async move {
            while let Some(message) = self.rx.recv().await {
                match message {
                    DBCacheMessage::Write(db_tx) => {
                        // Process the write transaction
                        self.write(db_tx)
                            .await
                            .expect("write through cache should succeed");
                    }
                    DBCacheMessage::Flush(sender) => {
                        // Flush the current state and send back the result
                        let flush_result = self.flush().await;
                        let _ = sender.send(Ok(flush_result));
                    }
                }
            }
        })
    }

    /// Processes and caches incoming database transactions until a new block is received.
    ///
    /// This method handles incoming write transactions. Transactions for the current pending block
    /// are cached and accumulated. If a transaction belongs to an older block, it is
    /// immediately applied to the database. In cases where the incoming transaction's
    /// block is a direct successor of the current pending block, it triggers a flush
    /// of all cached transactions to the database, ensuring they are applied in a
    /// single batch. This approach optimizes database writes by batching them and
    /// reducing the frequency of write operations, while also maintaining the integrity
    /// and order of blockchain data.
    ///
    /// Errors are raised if the incoming transaction's block does not logically follow
    /// the sequence of the blockchain (e.g., if the block is too far ahead or does not
    /// connect with the last persisted block).
    async fn write(&mut self, new_db_tx: DBTransaction) -> Result<(), StorageError> {
        match self.pending_block {
            Some(pending) => {
                if pending.hash == new_db_tx.block.hash {
                    // New database transaction for the same block
                    self.pending_db_txs.push(new_db_tx);
                } else if new_db_tx.block.number < pending.number {
                    // New database transaction for an old block
                    let mut conn = self
                        .pool
                        .get()
                        .await
                        .expect("pool should be connected");

                    conn.transaction(|conn| {
                        async {
                            for op in new_db_tx.operations {
                                match self.execute_write_op(op, conn).await {
                                    Err(StorageError::DuplicateEntry(entity, id)) => {
                                        // As this db transaction is old. It can contains already
                                        // stored blocks or txs, we log the duplicate entry error
                                        // and continue
                                        debug!("Duplicate entry for {} with id {}", entity, id);
                                    }
                                    Err(e) => {
                                        return Err(e);
                                    }
                                    _ => {}
                                }
                            }
                            Result::<(), StorageError>::Ok(())
                        }
                        .scope_boxed()
                    })
                    .await?;

                    //Notify that it
                    new_db_tx
                        .tx
                        .send(Ok(()))
                        .expect("response");
                } else if new_db_tx.block.parent_hash == pending.hash {
                    // New database transaction for the next block
                    self.flush().await;
                    self.pending_block = Some(new_db_tx.block);
                    self.pending_db_txs.push(new_db_tx);
                } else {
                    // Other cases return unexpected error
                    return Err(StorageError::Unexpected("Invalid cache state!".into()));
                }
            }
            None => {
                self.pending_block = Some(new_db_tx.block);
                self.pending_db_txs.push(new_db_tx);
            }
        }
        Ok(())
    }

    /// Commits the current cached state to the database in a single batch operation.
    /// Extracts write operations from `pending_db_txs`, remove duplicates, executes them,
    /// updates `persisted_block` with `pending_block`, and sets `pending_block` to `None`.
    async fn flush(&mut self) {
        let mut conn = self
            .pool
            .get()
            .await
            .expect("pool should be connected");

        let db_txs = std::mem::take(&mut self.pending_db_txs);
        let mut seen_operations: Vec<WriteOp> = Vec::new();

        conn.transaction(|conn| {
            async {
                for db_tx in db_txs.into_iter() {
                    let mut res = Ok(());

                    for op in &db_tx.operations {
                        if !seen_operations.contains(op) {
                            // Only executes if it was not already in seen_operations
                            match self
                                .execute_write_op(op.clone(), conn)
                    .await
                            {
                                Ok(_) => {
                                    seen_operations.push(op.clone());
                                }
                                Err(e) => {
                                    res = Err(e);
                                    break;
                                }
                            };
                        }
                    }

                    // If there was any error during the execution of this db_tx we notify the
                    // sender.
                    if let Err(e) = res {
                        db_tx
                            .tx
                            .send(Err(e))
                            .expect("send error back to db tx ok")
                    } else {
                        db_tx.tx.send(Ok(())).expect("response");
                    }
                }
                Result::<(), StorageError>::Ok(())
            }
            .scope_boxed()
        })
        .await
        .expect("tx ok");

        self.persisted_block = Some(self.pending_block.unwrap());
        self.pending_block = None;
    }

    /// Executes an operation.
    ///
    /// This function handles different types of write operations such as
    /// upserts, updates, and reverts, ensuring data consistency in the database.
    async fn execute_write_op(
        &mut self,
        operation: WriteOp,
        conn: &mut AsyncPgConnection,
    ) -> Result<(), StorageError> {
        match operation {
            WriteOp::UpsertBlock(block) => {
                self.state_gateway
                    .upsert_block(&block, conn)
                    .await
            }
            WriteOp::UpsertTx(transaction) => {
                self.state_gateway
                    .upsert_tx(&transaction, conn)
                    .await
            }
            WriteOp::SaveExtractionState(state) => {
                self.state_gateway
                    .save_state(&state, conn)
                            .await
            }
            WriteOp::InsertContract(contract) => {
                self.state_gateway
                    .insert_contract(&contract, conn)
                    .await
            }
            WriteOp::UpdateContracts(contracts) => {
                let collected_changes: Vec<(TxHash, &AccountUpdate)> = contracts
                    .iter()
                    .map(|(tx, update)| (tx.clone(), update))
                    .collect();
                let changes_slice = collected_changes.as_slice();
                self.state_gateway
                    .update_contracts(&self.chain, changes_slice, conn)
                    .await
            }
            WriteOp::RevertContractState(block) => {
                self.state_gateway
                    .revert_state(&block, conn)
                    .await
            }
            }
    }
}

pub struct CachedGateway {
    tx: mpsc::Sender<DBCacheMessage>,
    pool: Pool<AsyncPgConnection>,
    state_gateway: EVMStateGateway<AsyncPgConnection>,
}

impl CachedGateway {
    // TODO: implement the usual gateway methods here, but they are translated into write ops, for
    // reads call the gateway directly
    // pub async fn upsert_block(&self, new: &evm::Block) -> Result<(), StorageError> {
    //     todo!()
    // }
    pub async fn get_account_delta(
        &self,
        chain: &Chain,
        start_version: Option<&BlockOrTimestamp>,
        end_version: &BlockOrTimestamp,
        db: &mut AsyncPgConnection,
    ) -> Result<Vec<AccountUpdate>, StorageError> {
        //TODO: handle multiple extractors reverts
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .send(DBCacheMessage::Flush(tx))
            .await;
        let _ = rx.await;
        self.state_gateway
            .get_account_delta(chain, start_version, end_version, db)
            .await
    }
}

// These two implementation allow to inherit EVMStateGateway methods. If CachedGateway doesn't
// implement the called method and EVMStateGateway does, then the call will be forwarded to
// EVMStateGateway.
impl Deref for CachedGateway {
    type Target = EVMStateGateway<AsyncPgConnection>;

    fn deref(&self) -> &Self::Target {
        &self.state_gateway
    }
}

impl DerefMut for CachedGateway {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.state_gateway
    }
}

pub fn new_cached_gateway(
    pool: Pool<AsyncPgConnection>,
) -> Result<(JoinHandle<()>, CachedGateway), StorageError> {
    todo!()
}
