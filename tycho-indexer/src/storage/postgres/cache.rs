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
use tracing::log::{debug, info};

use crate::{
    extractor::evm::{self, AccountUpdate, EVMStateGateway},
    models::{Chain, ExtractionState},
    storage::{BlockIdentifier, BlockOrTimestamp, StorageError, TxHash},
};

/// Represents different types of database write operations.
#[derive(PartialEq, Clone)]
pub enum WriteOp {
    UpsertBlock(evm::Block),
    UpsertTx(evm::Transaction),
    SaveExtractionState(ExtractionState),
    InsertContract(evm::Account),
    UpdateContracts(Vec<(TxHash, AccountUpdate)>),
}

/// Represents a transaction in the database, including the block information,
/// a list of operations to be performed, and a channel to send the result.
pub struct DBTransaction {
    block: evm::Block,
    operations: Vec<WriteOp>,
    tx: oneshot::Sender<Result<(), StorageError>>,
}

impl DBTransaction {
    pub fn new(
        block: evm::Block,
        operations: Vec<WriteOp>,
        tx: oneshot::Sender<Result<(), StorageError>>,
    ) -> Self {
        Self { block, operations, tx }
    }
}

/// Represents different types of messages that can be sent to the DBCacheWriteExecutor.
pub enum DBCacheMessage {
    Write(DBTransaction),
    Flush(oneshot::Sender<Result<(), StorageError>>),
    Revert(BlockIdentifier, oneshot::Sender<Result<(), StorageError>>),
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
/// In case the incoming transactions block is too far ahead / does not
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
/// purposefully kept somewhat decoupled but not entirely. The reason is to
/// ensure fast development but also have a path that shows how we could
/// decouple especially from evm bounds models, as most likely we will soon have
/// additional chains to deal with.
///
/// Read Operations
/// The class does provide read operations for completeness, but it will not consider any
/// cached changes while reading. Any reads are direct pass through to the database.
pub(crate) struct DBCacheWriteExecutor {
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
    pending_db_txs: Vec<WriteOp>,
    msg_receiver: mpsc::Receiver<DBCacheMessage>,
    error_transmitter: mpsc::Sender<StorageError>,
}

impl DBCacheWriteExecutor {
    pub(crate) fn new(
        name: String,
        chain: Chain,
        pool: Pool<AsyncPgConnection>,
        state_gateway: EVMStateGateway<AsyncPgConnection>,
        msg_receiver: mpsc::Receiver<DBCacheMessage>,
        error_transmitter: mpsc::Sender<StorageError>,
    ) -> Self {
        Self {
            name,
            chain,
            pool,
            state_gateway,
            persisted_block: None,
            pending_block: None,
            pending_db_txs: Vec::<WriteOp>::new(),
            msg_receiver,
            error_transmitter,
        }
    }

    /// Spawns a task to process incoming database messages (write requests or flush commands).
    pub fn run(mut self) -> JoinHandle<()> {
        info!("DBCacheWriteExecutor {} started!", self.name);
        tokio::spawn(async move {
            while let Some(message) = self.msg_receiver.recv().await {
                match message {
                    DBCacheMessage::Write(db_tx) => {
                        // Process the write transaction
                        self.write(db_tx).await;
                    }
                    DBCacheMessage::Flush(sender) => {
                        // Flush the current state and send back the result
                        let flush_result = self.flush().await;
                        sender
                            .send(flush_result)
                            .expect("Should successfully notify sender");
                    }
                    DBCacheMessage::Revert(to, sender) => {
                        let revert_result = self.revert(&to).await;
                        sender
                            .send(revert_result)
                            .expect("Should successfully notify sender");
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
    /// Errors are sent to the error channel if the incoming transaction's block does not logically
    /// follow the sequence of the blockchain (e.g., if the block is too far ahead or does not
    /// connect with the last persisted block) or if we fail to send a transaction to the database.
    async fn write(&mut self, mut new_db_tx: DBTransaction) {
        match self.pending_block {
            Some(pending) => {
                if pending.hash == new_db_tx.block.hash {
                    // New database transaction for the same block are cached
                    self.pending_db_txs
                        .append(&mut new_db_tx.operations);
                    new_db_tx
                        .tx
                        .send(Ok(()))
                        .expect("Should successfully notify sender");
                } else if new_db_tx.block.number < pending.number {
                    // New database transaction for an old block are directly sent to the database
                    let mut conn = self
                        .pool
                        .get()
                        .await
                        .expect("pool should be connected");

                    let res = conn
                        .transaction(|conn| {
                            async {
                                for op in new_db_tx.operations {
                                    match self.execute_write_op(op, conn).await {
                                        Err(StorageError::DuplicateEntry(entity, id)) => {
                                            // As this db transaction is old. It can contain already
                                            // stored blocks or txs, we log the duplicate entry
                                            // error and continue
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
                        .await;

                    // Forward the result to the sender
                    new_db_tx
                        .tx
                        .send(res)
                        .expect("Should successfully notify sender");
                } else if new_db_tx.block.parent_hash == pending.hash {
                    debug!("New block received {} !", new_db_tx.block.parent_hash);
                    // New database transaction for the next block, we flush and cache it
                    self.flush()
                        .await
                        .expect("Flush should succeed");
                    self.pending_block = Some(new_db_tx.block);

                    self.pending_db_txs
                        .append(&mut new_db_tx.operations);
                    new_db_tx
                        .tx
                        .send(Ok(()))
                        .expect("Should successfully notify sender");
                } else {
                    // Other cases send unexpected error
                    self.error_transmitter
                        .send(StorageError::Unexpected("Invalid cache state!".into()))
                        .await
                        .expect("Should successfully notify error");
                }
            }
            None => {
                // if self.pending_block == None, this case can happen when we start Tycho or after
                // a call to flush().
                self.pending_block = Some(new_db_tx.block);

                self.pending_db_txs
                    .append(&mut new_db_tx.operations);
                new_db_tx
                    .tx
                    .send(Ok(()))
                    .expect("Should successfully notify sender");
            }
        }
    }

    /// Commits the current cached state to the database in a single batch operation.
    /// Extracts write operations from `pending_db_txs`, remove duplicates, executes them,
    /// updates `persisted_block` with `pending_block`, and sets `pending_block` to `None`.
    async fn flush(&mut self) -> Result<(), StorageError> {
        let mut conn = self
            .pool
            .get()
            .await
            .expect("pool should be connected");

        let db_txs = std::mem::take(&mut self.pending_db_txs);
        let mut seen_operations: Vec<WriteOp> = Vec::new();

        conn.transaction(|conn| {
            async {
                for op in db_txs.into_iter() {
                    if !seen_operations.contains(&op) {
                        // Only executes if it is not already in seen_operations
                        match self
                            .execute_write_op(op.clone(), conn)
                            .await
                        {
                            Ok(_) => {
                                seen_operations.push(op.clone());
                            }
                            Err(e) => {
                                // If and error happens, revert the whole transaction
                                return Err(e);
                            }
                        };
                    }
                }
                Result::<(), StorageError>::Ok(())
            }
            .scope_boxed()
        })
        .await?;

        self.persisted_block = self.pending_block;
        self.pending_block = None;
        Ok(())
    }

    /// Reverts the whole database state to `to`.
    async fn revert(&mut self, to: &BlockIdentifier) -> Result<(), StorageError> {
        self.flush()
            .await
            .expect("Flush should succeed");
        let mut conn = self
            .pool
            .get()
            .await
            .expect("pool should be connected");

        self.persisted_block = Some(
            self.state_gateway
                .get_block(to, &mut conn)
                .await
                .expect("get block ok"),
        );
        self.pending_block = None;
        self.state_gateway
            .revert_state(to, &mut conn)
            .await
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
        }
    }
}

pub struct CachedGateway {
    tx: mpsc::Sender<DBCacheMessage>,
    pool: Pool<AsyncPgConnection>,
    state_gateway: EVMStateGateway<AsyncPgConnection>,
}

impl CachedGateway {
    #[allow(private_interfaces)]
    pub fn new(
        tx: mpsc::Sender<DBCacheMessage>,
        pool: Pool<AsyncPgConnection>,
        state_gateway: EVMStateGateway<AsyncPgConnection>,
    ) -> Self {
        CachedGateway { tx, pool, state_gateway }
    }
    pub async fn upsert_block(&self, new: &evm::Block) -> Result<(), StorageError> {
        let (tx, rx) = oneshot::channel();
        let db_tx = DBTransaction::new(*new, vec![WriteOp::UpsertBlock(*new)], tx);
        self.tx
            .send(DBCacheMessage::Write(db_tx))
            .await
            .expect("Send message to receiver ok");
        rx.await
            .expect("Receive confirmation ok")
    }

    pub async fn upsert_tx(
        &self,
        block: &evm::Block,
        new: &evm::Transaction,
    ) -> Result<(), StorageError> {
        let (tx, rx) = oneshot::channel();
        let db_tx = DBTransaction::new(*block, vec![WriteOp::UpsertTx(*new)], tx);
        self.tx
            .send(DBCacheMessage::Write(db_tx))
            .await
            .expect("Send message to receiver ok");
        rx.await
            .expect("Receive confirmation ok")
    }

    pub async fn save_state(
        &self,
        block: &evm::Block,
        new: &ExtractionState,
    ) -> Result<(), StorageError> {
        let (tx, rx) = oneshot::channel();
        let db_tx = DBTransaction::new(*block, vec![WriteOp::SaveExtractionState(new.clone())], tx);
        self.tx
            .send(DBCacheMessage::Write(db_tx))
            .await
            .expect("Send message to receiver ok");
        rx.await
            .expect("Receive confirmation ok")
    }

    pub async fn insert_contract(
        &self,
        block: &evm::Block,
        new: &evm::Account,
    ) -> Result<(), StorageError> {
        let (tx, rx) = oneshot::channel();
        let db_tx = DBTransaction::new(*block, vec![WriteOp::InsertContract(new.clone())], tx);
        self.tx
            .send(DBCacheMessage::Write(db_tx))
            .await
            .expect("Send message to receiver ok");
        rx.await
            .expect("Receive confirmation ok")
    }

    pub async fn update_contracts(
        &self,
        block: &evm::Block,
        new: &[(TxHash, AccountUpdate)],
    ) -> Result<(), StorageError> {
        let (tx, rx) = oneshot::channel();
        let db_tx = DBTransaction::new(*block, vec![WriteOp::UpdateContracts(new.to_owned())], tx);
        self.tx
            .send(DBCacheMessage::Write(db_tx))
            .await
            .expect("Send message to receiver ok");
        rx.await
            .expect("Receive confirmation ok")
    }

    pub async fn revert_state(&self, to: &BlockIdentifier) -> Result<(), StorageError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(DBCacheMessage::Revert(to.clone(), tx))
            .await
            .expect("Send message to receiver ok");
        rx.await
            .expect("Receive confirmation ok")
    }

    pub async fn get_accounts_delta(
        &self,
        chain: &Chain,
        start_version: Option<&BlockOrTimestamp>,
        end_version: &BlockOrTimestamp,
        db: &mut AsyncPgConnection,
    ) -> Result<Vec<AccountUpdate>, StorageError> {
        //TODO: handle multiple extractors reverts
        self.flush()
            .await
            .expect("Flush should succeed");
        self.state_gateway
            .get_accounts_delta(chain, start_version, end_version, db)
            .await
    }

    pub async fn flush(&self) -> Result<(), StorageError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(DBCacheMessage::Flush(tx))
            .await
            .expect("Send message to receiver ok");
        rx.await
            .expect("Receive confirmation ok")
    }
}

// These two implementations allow us to inherit EVMStateGateway methods. If CachedGateway doesn't
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

#[cfg(test)]
mod test {
    use std::{str::FromStr, sync::Arc};

    use diesel_async::{
        pooled_connection::{deadpool::Pool, AsyncDieselConnectionManager},
        AsyncConnection, AsyncPgConnection,
    };
    use ethers::{prelude::H256, types::H160};
    use tokio::sync::{
        mpsc,
        oneshot::{self},
    };

    use crate::{
        extractor::{evm, evm::EVMStateGateway},
        hex_bytes::Bytes,
        models::{Chain, ExtractionState},
        storage::{
            postgres::{
                cache::{
                    CachedGateway, DBCacheMessage, DBCacheWriteExecutor, DBTransaction, WriteOp,
                },
                db_fixtures, PostgresGateway,
            },
            BlockIdentifier, StorageError,
            StorageError::NotFound,
        },
    };

    use tokio::sync::mpsc::error::TryRecvError::Empty;

    async fn setup_gateway() -> Pool<AsyncPgConnection> {
        let database_url =
            std::env::var("DATABASE_URL").expect("Database URL must be set for testing");
        let config = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
        let pool = Pool::builder(config)
            .max_size(1)
            .build()
            .unwrap();

        let mut connection = pool
            .get()
            .await
            .expect("Failed to get a connection from the pool");
        connection
            .begin_test_transaction()
            .await
            .expect("Failed to start test transaction");

        pool
    }

    #[tokio::test]
    async fn test_write_and_flush() {
        // Setup
        let connection_pool = setup_gateway().await;
        let mut connection = connection_pool
            .get()
            .await
            .expect("Failed to get a connection from the pool");
        db_fixtures::insert_chain(&mut connection, "ethereum").await;
        let gateway: EVMStateGateway<AsyncPgConnection> = Arc::new(
            PostgresGateway::<
                evm::Block,
                evm::Transaction,
                evm::Account,
                evm::AccountUpdate,
                evm::ERC20Token,
            >::from_connection(&mut connection)
            .await,
        );
        drop(connection);
        let (tx, rx) = mpsc::channel(10);
        let (err_tx, mut err_rx) = mpsc::channel(10);

        let write_executor = DBCacheWriteExecutor::new(
            "ethereum".to_owned(),
            Chain::Ethereum,
            connection_pool.clone(),
            gateway.clone(),
            rx,
            err_tx,
        );

        let handle = write_executor.run();

        // Send write block message
        let block = get_sample_block(1);
        let os_rx = send_write_message(&tx, block, vec![WriteOp::UpsertBlock(block)]).await;
        os_rx
            .await
            .expect("Response from channel ok")
            .expect("Transaction cached");

        // Send flush message
        let os_rx_flush = send_flush_message(&tx).await;
        os_rx_flush
            .await
            .expect("Response from channel ok")
            .expect("DB transaction not flushed");

        let maybe_err = err_rx
            .try_recv()
            .expect_err("Error channel should be empty");

        handle.abort();

        // Assert that block_1 has been cached and flushed with no error
        let mut connection = connection_pool
            .get()
            .await
            .expect("Failed to get a connection from the pool");

        let block_id = BlockIdentifier::Number((Chain::Ethereum, 1));
        let fetched_block = gateway
            .get_block(&block_id, &mut connection)
            .await
            .expect("Failed to fetch extraction state");

        assert_eq!(fetched_block, block);
        // Assert no error happened
        assert_eq!(maybe_err, Empty);
    }

    #[tokio::test]
    async fn test_writes_and_new_blocks() {
        // Setup
        let connection_pool = setup_gateway().await;
        let mut connection = connection_pool
            .get()
            .await
            .expect("Failed to get a connection from the pool");
        db_fixtures::insert_chain(&mut connection, "ethereum").await;
        let gateway: EVMStateGateway<AsyncPgConnection> = Arc::new(
            PostgresGateway::<
                evm::Block,
                evm::Transaction,
                evm::Account,
                evm::AccountUpdate,
                evm::ERC20Token,
            >::from_connection(&mut connection)
            .await,
        );
        drop(connection);
        let (tx, rx) = mpsc::channel(10);
        let (err_tx, mut err_rx) = mpsc::channel(10);

        let write_executor = DBCacheWriteExecutor::new(
            "ethereum".to_owned(),
            Chain::Ethereum,
            connection_pool.clone(),
            gateway.clone(),
            rx,
            err_tx,
        );

        let handle = write_executor.run();

        // Send first block messages
        let block_1 = get_sample_block(1);
        let tx_1 = get_sample_transaction(1);
        let extraction_state_1 = get_sample_extraction(1);
        let os_rx_1 = send_write_message(
            &tx,
            block_1,
            vec![
                WriteOp::UpsertBlock(block_1),
                WriteOp::UpsertTx(tx_1),
                WriteOp::SaveExtractionState(extraction_state_1.clone()),
            ],
        )
        .await;
        os_rx_1
            .await
            .expect("Response from channel ok")
            .expect("Transaction cached");

        // Send second block messages
        let block_2 = get_sample_block(2);
        let os_rx_2 = send_write_message(&tx, block_2, vec![WriteOp::UpsertBlock(block_2)]).await;
        os_rx_2
            .await
            .expect("Response from channel ok")
            .expect("Transaction cached");

        // Send third block messages
        let block_3 = get_sample_block(3);
        let os_rx_3 = send_write_message(&tx, block_3, vec![WriteOp::UpsertBlock(block_3)]).await;
        os_rx_3
            .await
            .expect("Response from channel ok")
            .expect("Transaction cached");

        let maybe_err = err_rx
            .try_recv()
            .expect_err("Error channel should be empty");

        handle.abort();

        // Assert that messages from block 1 and 2 has been cached and flushed, and that block 3 is
        // still cached
        let mut connection = connection_pool
            .get()
            .await
            .expect("Failed to get a connection from the pool");

        let block_id_1 = BlockIdentifier::Number((Chain::Ethereum, 1));
        let fetched_block_1 = gateway
            .get_block(&block_id_1, &mut connection)
            .await
            .expect("Failed to fetch block");

        let fetched_tx = gateway
            .get_tx(&tx_1.hash.as_bytes().into(), &mut connection)
            .await
            .expect("Failed to fetch tx");

        let fetched_extraction_state = gateway
            .get_state("vm:test", &Chain::Ethereum, &mut connection)
            .await
            .expect("Failed to fetch extraction state");

        let block_id_2 = BlockIdentifier::Number((Chain::Ethereum, 2));
        let fetched_block_2 = gateway
            .get_block(&block_id_2, &mut connection)
            .await
            .expect("Failed to fetch block");

        let block_id_3 = BlockIdentifier::Number((Chain::Ethereum, 3));
        let fetched_block_3 = gateway
            .get_block(&block_id_3, &mut connection)
            .await
            .expect_err("Failed to fetch block");

        // Assert block 1 messages have been flushed
        assert_eq!(fetched_block_1, block_1);
        assert_eq!(fetched_tx, tx_1);
        assert_eq!(fetched_extraction_state, extraction_state_1);
        // Assert block 2 messages have been flushed
        assert_eq!(fetched_block_2, block_2);
        // Assert block 3 is still pending in cache
        assert_eq!(
            fetched_block_3,
            NotFound("Block".to_owned(), "Number((Ethereum, 3))".to_owned())
        );

        // Assert no error happened
        assert_eq!(maybe_err, Empty);
    }

    #[tokio::test]
    async fn test_revert() {
        // Setup
        let connection_pool = setup_gateway().await;
        let mut connection = connection_pool
            .get()
            .await
            .expect("Failed to get a connection from the pool");
        setup_data(&mut connection).await;
        let gateway: EVMStateGateway<AsyncPgConnection> = Arc::new(
            PostgresGateway::<
                evm::Block,
                evm::Transaction,
                evm::Account,
                evm::AccountUpdate,
                evm::ERC20Token,
            >::from_connection(&mut connection)
            .await,
        );
        drop(connection);
        let (tx, rx) = mpsc::channel(10);
        let (err_tx, mut err_rx) = mpsc::channel(10);

        let write_executor = DBCacheWriteExecutor::new(
            "ethereum".to_owned(),
            Chain::Ethereum,
            connection_pool.clone(),
            gateway.clone(),
            rx,
            err_tx,
        );

        let handle = write_executor.run();

        // Revert to block 1
        let (os_tx, os_rx) = oneshot::channel();
        let target = BlockIdentifier::Number((Chain::Ethereum, 1));

        tx.send(DBCacheMessage::Revert(target, os_tx))
            .await
            .expect("Failed to send write message through mpsc channel");

        os_rx
            .await
            .expect("Response from channel ok")
            .expect("Revert ok");

        let maybe_err = err_rx
            .try_recv()
            .expect_err("Error channel should be empty");

        handle.abort();

        // Assert that block 1 is still here and block above have been reverted
        let mut connection = connection_pool
            .get()
            .await
            .expect("Failed to get a connection from the pool");

        let block_id_1 = BlockIdentifier::Number((Chain::Ethereum, 1));
        let fetched_block_1 = gateway
            .get_block(&block_id_1, &mut connection)
            .await
            .expect("Failed to fetch block");

        let fetched_tx = gateway
            .get_tx(
                &H256::from_str(
                    "0xbb7e16d797a9e2fbc537e30f91ed3d27a254dd9578aa4c3af3e5f0d3e8130945",
                )
                .unwrap()
                .as_bytes()
                .into(),
                &mut connection,
            )
            .await
            .expect("Failed to fetch tx");

        let block_id_2 = BlockIdentifier::Number((Chain::Ethereum, 2));
        let fetched_block_2 = gateway
            .get_block(&block_id_2, &mut connection)
            .await
            .expect_err("Failed to fetch block");

        // Assert block 1 and txs at this block are still there
        assert_eq!(fetched_block_1.number, 1);
        assert_eq!(
            fetched_tx.block_hash,
            H256::from_str("0x88e96d4537bea4d9c05d12549907b32561d3bf31f45aae734cdc119f13406cb6")
                .unwrap()
        );
        // Assert block 2 has been reverted
        assert_eq!(
            fetched_block_2,
            NotFound("Block".to_owned(), "Number((Ethereum, 2))".to_owned())
        );

        // Assert no error happened
        assert_eq!(maybe_err, Empty);
    }

    #[tokio::test]
    async fn test_cached_gateway() {
        // Setup
        let connection_pool = setup_gateway().await;
        let mut connection = connection_pool
            .get()
            .await
            .expect("Failed to get a connection from the pool");
        db_fixtures::insert_chain(&mut connection, "ethereum").await;
        let gateway = Arc::new(
            PostgresGateway::<
                evm::Block,
                evm::Transaction,
                evm::Account,
                evm::AccountUpdate,
                evm::ERC20Token,
            >::from_connection(&mut connection)
            .await,
        );
        drop(connection);
        let (tx, rx) = mpsc::channel(10);
        let (err_tx, mut err_rx) = mpsc::channel(10);

        let write_executor = DBCacheWriteExecutor::new(
            "ethereum".to_owned(),
            Chain::Ethereum,
            connection_pool.clone(),
            gateway.clone(),
            rx,
            err_tx,
        );

        let handle = write_executor.run();
        let cached_gw = CachedGateway::new(tx, connection_pool.clone(), gateway);

        // Send first block messages
        let block_1 = get_sample_block(1);
        let tx_1 = get_sample_transaction(1);
        cached_gw
            .upsert_block(&block_1)
            .await
            .expect("Upsert block 1 ok");
        cached_gw
            .upsert_tx(&block_1, &tx_1)
            .await
            .expect("Upsert tx 1 ok");

        // Send second block messages
        let block_2 = get_sample_block(2);
        cached_gw
            .upsert_block(&block_2)
            .await
            .expect("Upsert block 2 ok");

        // Send third block messages
        let block_3 = get_sample_block(3);
        cached_gw
            .upsert_block(&block_3)
            .await
            .expect("Upsert block 3 ok");

        let maybe_err = err_rx
            .try_recv()
            .expect_err("Error channel should be empty");

        handle.abort();

        // Assert that messages from block 1 and 2 has been cached and flushed, and that block 3 is
        // still cached
        let mut connection = connection_pool
            .get()
            .await
            .expect("Failed to get a connection from the pool");

        let block_id_1 = BlockIdentifier::Number((Chain::Ethereum, 1));
        let fetched_block_1 = cached_gw
            .get_block(&block_id_1, &mut connection)
            .await
            .expect("Failed to fetch block");

        let fetched_tx = cached_gw
            .get_tx(&tx_1.hash.as_bytes().into(), &mut connection)
            .await
            .expect("Failed to fetch tx");

        let block_id_2 = BlockIdentifier::Number((Chain::Ethereum, 2));
        let fetched_block_2 = cached_gw
            .get_block(&block_id_2, &mut connection)
            .await
            .expect("Failed to fetch block");

        let block_id_3 = BlockIdentifier::Number((Chain::Ethereum, 3));
        let fetched_block_3 = cached_gw
            .get_block(&block_id_3, &mut connection)
            .await
            .expect_err("Failed to fetch block");

        // Assert block 1 messages have been flushed
        assert_eq!(fetched_block_1, block_1);
        assert_eq!(fetched_tx, tx_1);
        // Assert block 2 messages have been flushed
        assert_eq!(fetched_block_2, block_2);
        // Assert block 3 is still pending in cache
        assert_eq!(
            fetched_block_3,
            NotFound("Block".to_owned(), "Number((Ethereum, 3))".to_owned())
        );
        // Assert no error happened
        assert_eq!(maybe_err, Empty);
    }

    fn get_sample_block(version: usize) -> evm::Block {
        match version {
            1 => evm::Block {
                number: 1,
                chain: Chain::Ethereum,
                hash: "0x88e96d4537bea4d9c05d12549907b32561d3bf31f45aae734cdc119f13406cb6"
                    .parse()
                    .expect("Invalid hash"),
                parent_hash: H256::zero(),
                ts: "2020-01-01T01:00:00"
                    .parse()
                    .expect("Invalid timestamp"),
            },
            2 => evm::Block {
                number: 2,
                chain: Chain::Ethereum,
                hash: "0xb495a1d7e6663152ae92708da4843337b958146015a2802f4193a410044698c9"
                    .parse()
                    .expect("Invalid hash"),
                parent_hash: "0x88e96d4537bea4d9c05d12549907b32561d3bf31f45aae734cdc119f13406cb6"
                    .parse()
                    .expect("Invalid hash"),
                ts: "2020-01-01T02:00:00"
                    .parse()
                    .expect("Invalid timestamp"),
            },
            3 => evm::Block {
                number: 3,
                chain: Chain::Ethereum,
                hash: "0x3d6122660cc824376f11ee842f83addc3525e2dd6756b9bcf0affa6aa88cf741"
                    .parse()
                    .expect("Invalid hash"),
                parent_hash: "0xb495a1d7e6663152ae92708da4843337b958146015a2802f4193a410044698c9"
                    .parse()
                    .expect("Invalid hash"),
                ts: "2020-01-01T03:00:00"
                    .parse()
                    .expect("Invalid timestamp"),
            },
            _ => panic!("Block version not found"),
        }
    }

    fn get_sample_transaction(version: usize) -> evm::Transaction {
        match version {
            1 => evm::Transaction {
                hash: H256::from_str(
                    "0xbb7e16d797a9e2fbc537e30f91ed3d27a254dd9578aa4c3af3e5f0d3e8130945",
                )
                .expect("tx hash ok"),
                block_hash: H256::from_str(
                    "0x88e96d4537bea4d9c05d12549907b32561d3bf31f45aae734cdc119f13406cb6",
                )
                .expect("block hash ok"),
                from: H160::from_str("0x4648451b5F87FF8F0F7D622bD40574bb97E25980")
                    .expect("from ok"),
                to: Some(
                    H160::from_str("0x6B175474E89094C44Da98b954EedeAC495271d0F").expect("to ok"),
                ),
                index: 1,
            },
            _ => panic!("Block version not found"),
        }
    }

    fn get_sample_extraction(version: usize) -> ExtractionState {
        match version {
            1 => ExtractionState::new(
                "vm:test".to_string(),
                Chain::Ethereum,
                None,
                "cursor@420".as_bytes(),
            ),
            _ => panic!("Block version not found"),
        }
    }

    async fn send_write_message(
        tx: &mpsc::Sender<DBCacheMessage>,
        block: evm::Block,
        operations: Vec<WriteOp>,
    ) -> oneshot::Receiver<Result<(), StorageError>> {
        let (os_tx, os_rx) = oneshot::channel();
        let db_transaction = DBTransaction { block, operations, tx: os_tx };

        tx.send(DBCacheMessage::Write(db_transaction))
            .await
            .expect("Failed to send write message through mpsc channel");
        os_rx
    }

    async fn send_flush_message(
        tx: &mpsc::Sender<DBCacheMessage>,
    ) -> oneshot::Receiver<Result<(), StorageError>> {
        let (os_tx, os_rx) = oneshot::channel();
        tx.send(DBCacheMessage::Flush(os_tx))
            .await
            .expect("Failed to send flush message through mpsc channel");
        os_rx
    }

    //noinspection SpellCheckingInspection
    async fn setup_data(conn: &mut AsyncPgConnection) {
        let chain_id = db_fixtures::insert_chain(conn, "ethereum").await;
        let blk = db_fixtures::insert_blocks(conn, chain_id).await;
        let txn = db_fixtures::insert_txns(
            conn,
            &[
                (
                    // deploy c0
                    blk[0],
                    1i64,
                    "0xbb7e16d797a9e2fbc537e30f91ed3d27a254dd9578aa4c3af3e5f0d3e8130945",
                ),
                (
                    // change c0 state, deploy c2
                    blk[0],
                    2i64,
                    "0x794f7df7a3fe973f1583fbb92536f9a8def3a89902439289315326c04068de54",
                ),
                // ----- Block 01 LAST
                (
                    // deploy c1, delete c2
                    blk[1],
                    1i64,
                    "0x3108322284d0a89a7accb288d1a94384d499504fe7e04441b0706c7628dee7b7",
                ),
                (
                    // change c0 and c1 state
                    blk[1],
                    2i64,
                    "0x50449de1973d86f21bfafa7c72011854a7e33a226709dc3e2e4edcca34188388",
                ),
                // ----- Block 02 LAST
            ],
        )
        .await;
        let c0 = db_fixtures::insert_account(
            conn,
            "6B175474E89094C44Da98b954EedeAC495271d0F",
            "account0",
            chain_id,
            Some(txn[0]),
        )
        .await;
        db_fixtures::insert_account_balance(conn, 0, txn[0], c0).await;
        db_fixtures::insert_contract_code(conn, c0, txn[0], Bytes::from_str("C0C0C0").unwrap())
            .await;
        db_fixtures::insert_account_balance(conn, 100, txn[1], c0).await;
        db_fixtures::insert_slots(
            conn,
            c0,
            txn[1],
            "2020-01-01T00:00:00",
            &[(0, 1), (1, 5), (2, 1)],
        )
        .await;
        db_fixtures::insert_account_balance(conn, 101, txn[3], c0).await;
        db_fixtures::insert_slots(
            conn,
            c0,
            txn[3],
            "2020-01-01T01:00:00",
            &[(0, 2), (1, 3), (5, 25), (6, 30)],
        )
        .await;

        let c1 = db_fixtures::insert_account(
            conn,
            "73BcE791c239c8010Cd3C857d96580037CCdd0EE",
            "c1",
            chain_id,
            Some(txn[2]),
        )
        .await;
        db_fixtures::insert_account_balance(conn, 50, txn[2], c1).await;
        db_fixtures::insert_contract_code(conn, c1, txn[2], Bytes::from_str("C1C1C1").unwrap())
            .await;
        db_fixtures::insert_slots(conn, c1, txn[3], "2020-01-01T01:00:00", &[(0, 128), (1, 255)])
            .await;

        let c2 = db_fixtures::insert_account(
            conn,
            "94a3F312366b8D0a32A00986194053C0ed0CdDb1",
            "c2",
            chain_id,
            Some(txn[1]),
        )
        .await;
        db_fixtures::insert_account_balance(conn, 25, txn[1], c2).await;
        db_fixtures::insert_contract_code(conn, c2, txn[1], Bytes::from_str("C2C2C2").unwrap())
            .await;
        db_fixtures::insert_slots(conn, c2, txn[1], "2020-01-01T00:00:00", &[(1, 2), (2, 4)]).await;
        db_fixtures::delete_account(conn, c2, "2020-01-01T01:00:00").await;
    }
}
