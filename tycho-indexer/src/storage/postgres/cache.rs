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

                    // Notify that this transaction was correctly send to the db
                    new_db_tx
                        .tx
                        .send(Ok(()))
                        .expect("Should successfully notify sender");
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

pub fn new_cached_gateway(
    pool: Pool<AsyncPgConnection>,
) -> Result<(JoinHandle<()>, CachedGateway), StorageError> {
    todo!()
}

#[cfg(test)]
mod test {
    use crate::{
        extractor::{evm, evm::EVMStateGateway},
        models::{Chain, ExtractionState},
        storage::{
            postgres::{
                cache::{DBCacheMessage, DBCacheWriteExecutor, DBTransaction, WriteOp},
                db_fixtures, PostgresGateway,
            },
            BlockIdentifier, StorageError,
        },
    };
    use diesel_async::{
        pooled_connection::{deadpool::Pool, AsyncDieselConnectionManager},
        AsyncConnection, AsyncPgConnection,
    };
    use ethers::{prelude::H256, types::H160};
    use std::{str::FromStr, sync::Arc};
    use tokio::sync::{
        mpsc,
        oneshot::{self, error::TryRecvError},
    };

    async fn setup_gateway() -> (EVMStateGateway<AsyncPgConnection>, Pool<AsyncPgConnection>) {
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
        db_fixtures::insert_chain(&mut connection, "ethereum").await;

        let gateway = PostgresGateway::<
            evm::Block,
            evm::Transaction,
            evm::Account,
            evm::AccountUpdate,
        >::from_connection(&mut connection)
        .await;

        (Arc::new(gateway), pool)
    }

    #[tokio::test]
    async fn test_write_and_flush() {
        let (gateway, connection_pool) = setup_gateway().await;
        let (tx, rx) = mpsc::channel(10);

        let write_executor = DBCacheWriteExecutor::new(
            "ethereum".to_owned(),
            Chain::Ethereum,
            connection_pool.clone(),
            gateway.clone(),
            rx,
        );

        let handle = write_executor.run();
        let block = get_sample_block(1);

        // Send Write message
        let os_rx = send_write_message(&tx, block, vec![WriteOp::UpsertBlock(block)]).await;

        // Send Flush message
        let os_rx_flush = send_flush_message(&tx).await;

        os_rx
            .await
            .expect("Response from channel ok")
            .expect("DB transaction not sent");

        os_rx_flush
            .await
            .expect("Response from channel ok")
            .expect("DB transaction not flushed");
        handle.abort();

        // Assert that block_1 has been sent
        let block_id = BlockIdentifier::Number((Chain::Ethereum, 1));
        let mut connection = connection_pool
            .get()
            .await
            .expect("Failed to get a connection from the pool");
        let fetched_block = gateway
            .get_block(&block_id, &mut connection)
            .await
            .expect("Failed to fetch block");

        assert_eq!(fetched_block, block);
    }

    #[tokio::test]
    async fn test_writes_and_new_blocks() {
        let (gateway, connection_pool) = setup_gateway().await;
        let (tx, rx) = mpsc::channel(10);

        let write_executor = DBCacheWriteExecutor::new(
            "ethereum".to_owned(),
            Chain::Ethereum,
            connection_pool.clone(),
            gateway.clone(),
            rx,
        );

        let handle = write_executor.run();
        let block_1 = get_sample_block(1);
        let tx_1 = get_sample_transaction(1);
        let extraction_state_1 = get_sample_extraction(1);

        // Send first block message
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

        // Send second block message
        let block_2 = get_sample_block(2);
        let os_rx_2 = send_write_message(&tx, block_2, vec![WriteOp::UpsertBlock(block_2)]).await;

        os_rx_1
            .await
            .expect("Response from channel ok")
            .expect("DB transaction not sent");

        // Send third block message
        let block_3 = get_sample_block(3);
        let mut os_rx_3 =
            send_write_message(&tx, block_3, vec![WriteOp::UpsertBlock(block_3)]).await;

        os_rx_2
            .await
            .expect("Response from channel ok")
            .expect("DB transaction not sent");

        match os_rx_3.try_recv() {
            Err(TryRecvError::Empty) => (), // Correct, block_3 is still in pending db transactions
            Ok(_) => panic!("Expected the channel to be empty, but found a message"),
            Err(e) => panic!("Expected TryRecvError::Empty, found different error: {:?}", e),
        }
        handle.abort();

        // Assert that block_1 has been sent
        let mut connection = connection_pool
            .get()
            .await
            .expect("Failed to get a connection from the pool");

        let block_id = BlockIdentifier::Number((Chain::Ethereum, 1));
        let fetched_block = gateway
            .get_block(&block_id, &mut connection)
            .await
            .expect("Failed to fetch block");

        let block_id = BlockIdentifier::Number((Chain::Ethereum, 1));
        let fetched_tx = gateway
            .get_tx(&tx_1.hash.as_bytes().into(), &mut connection)
            .await
            .expect("Failed to fetch block");

        let fetched_extraction_state = gateway
            .get_state("vm:test", &Chain::Ethereum, &mut connection)
            .await
            .expect("Failed to fetch block");

        assert_eq!(fetched_block, block_1);
        assert_eq!(fetched_tx, tx_1);
        assert_eq!(fetched_extraction_state, extraction_state_1);
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
}
