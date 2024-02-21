use std::{
    num::NonZeroUsize,
    ops::{Deref, DerefMut},
    sync::Arc,
};

use diesel_async::{
    pooled_connection::deadpool::Pool, scoped_futures::ScopedFutureExt, AsyncPgConnection,
};
use lru::LruCache;
use tokio::{
    sync::{
        mpsc,
        oneshot::{self},
        Mutex,
    },
    task::JoinHandle,
};
use tracing::{debug, error, info, trace};

use crate::{
    extractor::evm::{
        self, AccountUpdate, ComponentBalance, ERC20Token, EVMStateGateway, ProtocolComponent,
        ProtocolStateDelta,
    },
    models::{Chain, ExtractionState},
    storage::{BlockIdentifier, BlockOrTimestamp, StorageError, TxHash},
};

/// Represents different types of database write operations.
#[derive(PartialEq, Clone, Debug)]
pub(crate) enum WriteOp {
    // Simply merge
    UpsertBlock(Vec<evm::Block>),
    // Simply merge
    UpsertTx(Vec<evm::Transaction>),
    // Simply keep last
    SaveExtractionState(ExtractionState),
    // Support saving a batch
    InsertContract(Vec<evm::Account>),
    // Simply merge
    UpdateContracts(Vec<(TxHash, AccountUpdate)>),
    // Simply merge
    InsertProtocolComponents(Vec<evm::ProtocolComponent>),
    // Simply merge
    InsertTokens(Vec<evm::ERC20Token>),
    // Simply merge
    InsertComponentBalances(Vec<evm::ComponentBalance>),
    // Simply merge
    UpsertProtocolState(Vec<(TxHash, ProtocolStateDelta)>),
}

impl WriteOp {
    fn variant_name(&self) -> &'static str {
        match self {
            WriteOp::UpsertBlock(_) => "UpsertBlock",
            WriteOp::UpsertTx(_) => "UpsertTx",
            WriteOp::SaveExtractionState(_) => "SaveExtractionState",
            WriteOp::InsertContract(_) => "InsertContract",
            WriteOp::UpdateContracts(_) => "UpdateContracts",
            WriteOp::InsertProtocolComponents(_) => "InsertProtocolComponents",
            WriteOp::InsertTokens(_) => "InsertTokens",
            WriteOp::InsertComponentBalances(_) => "InsertComponentBalances",
            WriteOp::UpsertProtocolState(_) => "UpsertProtocolState",
        }
    }

    fn order_key(&self) -> usize {
        match self {
            WriteOp::UpsertBlock(_) => 0,
            WriteOp::UpsertTx(_) => 1,
            WriteOp::InsertContract(_) => 2,
            WriteOp::UpdateContracts(_) => 3,
            WriteOp::InsertTokens(_) => 4,
            WriteOp::InsertProtocolComponents(_) => 5,
            WriteOp::InsertComponentBalances(_) => 6,
            WriteOp::UpsertProtocolState(_) => 7,
            WriteOp::SaveExtractionState(_) => 8,
        }
    }
}

#[derive(Debug)]
struct BlockRange {
    start: evm::Block,
    end: evm::Block,
}

impl BlockRange {
    fn new(start: &evm::Block, end: &evm::Block) -> Self {
        Self { start: *start, end: *end }
    }

    fn is_single_block(&self) -> bool {
        self.start.hash == self.end.hash
    }
}

/// Represents a transaction in the database, including the block information,
/// a list of operations to be performed, and a channel to send the result.
pub struct DBTransaction {
    block_range: BlockRange,
    size: usize,
    operations: Vec<WriteOp>,
    tx: oneshot::Sender<Result<(), StorageError>>,
}

impl DBTransaction {
    /// Batch changes of the same kind.
    ///
    /// The final insertion order is determined via `WriteOp::order_key` and is fixed for all
    /// transaction.
    ///
    /// PERF: Use an array instead of a vec since the order is static.
    fn add_operation(&mut self, op: WriteOp) -> Result<(), StorageError> {
        for existing_op in self.operations.iter_mut() {
            match (existing_op, &op) {
                (WriteOp::UpsertBlock(l), WriteOp::UpsertBlock(r)) => {
                    self.size += r.len();
                    l.extend(r);
                    return Ok(());
                }
                (WriteOp::UpsertTx(l), WriteOp::UpsertTx(r)) => {
                    self.size += r.len();
                    l.extend(r);
                    return Ok(());
                }
                (WriteOp::SaveExtractionState(l), WriteOp::SaveExtractionState(r)) => {
                    l.clone_from(r);
                    return Ok(());
                }
                (WriteOp::InsertContract(l), WriteOp::InsertContract(r)) => {
                    self.size += r.len();
                    l.extend(r.iter().cloned());
                    return Ok(());
                }
                (WriteOp::UpdateContracts(l), WriteOp::UpdateContracts(r)) => {
                    self.size += r.len();
                    l.extend(r.iter().cloned());
                    return Ok(());
                }
                (WriteOp::InsertProtocolComponents(l), WriteOp::InsertProtocolComponents(r)) => {
                    self.size += r.len();
                    l.extend(r.iter().cloned());
                    return Ok(());
                }
                (WriteOp::InsertTokens(l), WriteOp::InsertTokens(r)) => {
                    self.size += r.len();
                    l.extend(r.iter().cloned());
                    return Ok(());
                }
                (WriteOp::InsertComponentBalances(l), WriteOp::InsertComponentBalances(r)) => {
                    self.size += r.len();
                    l.extend(r.iter().cloned());
                    return Ok(());
                }
                (WriteOp::UpsertProtocolState(l), WriteOp::UpsertProtocolState(r)) => {
                    self.size += r.len();
                    l.extend(r.iter().cloned());
                    return Ok(());
                }
                _ => continue,
            }
        }
        // not quite accurate but currently all WriteOps are created with a single entry.
        self.size += 1;
        self.operations.push(op);
        Ok(())
    }
}

/// Represents different types of messages that can be sent to the DBCacheWriteExecutor.
pub enum DBCacheMessage {
    Write(DBTransaction),
}

/// Extractors can start transaction.
/// This will guarantee that a group of changes they provide is executed atomically.
///
/// The gateway keeps track of the blockchains progress.
/// A new transaction group finishes. This group has a block attached to it.
/// - If the block is old, we execute the transaction immediately.
/// - If the block is pending, we group the transaction with other transactions that finish before
///   we observe the next block.

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
pub struct DBCacheWriteExecutor {
    name: String,
    chain: Chain,
    pool: Pool<AsyncPgConnection>,
    state_gateway: EVMStateGateway<AsyncPgConnection>,
    persisted_block: Option<evm::Block>,
    msg_receiver: mpsc::Receiver<DBCacheMessage>,
}

impl DBCacheWriteExecutor {
    pub async fn new(
        name: String,
        chain: Chain,
        pool: Pool<AsyncPgConnection>,
        state_gateway: EVMStateGateway<AsyncPgConnection>,
        msg_receiver: mpsc::Receiver<DBCacheMessage>,
    ) -> Self {
        let mut conn = pool
            .get()
            .await
            .expect("pool should be connected");

        let persisted_block = match state_gateway
            .get_block(&BlockIdentifier::Latest(chain), &mut conn)
            .await
        {
            Ok(block) => Some(block),
            Err(_) => None,
        };

        tracing::debug!("Persisted block: {:?}", persisted_block);

        Self { name, chain, pool, state_gateway, persisted_block, msg_receiver }
    }

    /// Spawns a task to process incoming database messages (write requests or flush commands).
    pub fn run(mut self) -> JoinHandle<()> {
        tracing::info!("DBCacheWriteExecutor {} started!", self.name);
        tokio::spawn(async move {
            while let Some(message) = self.msg_receiver.recv().await {
                match message {
                    DBCacheMessage::Write(db_tx) => {
                        // Process the write transaction
                        self.write(db_tx).await;
                    }
                }
            }
        })
    }

    async fn write(&mut self, new_db_tx: DBTransaction) {
        debug!(block_range=?&new_db_tx.block_range, "Received new transaction");
        let mut conn = self
            .pool
            .get()
            .await
            .expect("pool should be connected");

        // If persisted block is not set we don't have data for this chain yet.
        if let Some(db_block) = self.persisted_block {
            // during sync we insert in batches of blocks.
            let syncing = !new_db_tx.block_range.is_single_block();

            // if we are not syncing we are not allowed to create a separate block range.
            if !syncing {
                let start = new_db_tx.block_range.start;
                // if we are advancing a block, while not syncing it must fit on top of the
                // persisted block.
                if start.number > db_block.number && start.parent_hash != db_block.hash {
                    error!(
                        block_range=?&new_db_tx.block_range,
                        persisted_block=?&db_block,
                        "Invalid block range encountered"
                    );
                    let _ = new_db_tx
                        .tx
                        .send(Err(StorageError::InvalidBlockRange()));
                    return;
                }
            }
        }

        let res = conn
            .build_transaction()
            .repeatable_read()
            .run(|conn| {
                async {
                    for op in new_db_tx.operations {
                        match self.execute_write_op(&op, conn).await {
                            Err(StorageError::DuplicateEntry(entity, id)) => {
                                // As this db transaction is old. It can contain
                                // already stored txs, we log the duplicate entry
                                // error and continue
                                debug!("Ignoring duplicate entry for {} with id {}", entity, id);
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
        let _ = new_db_tx.tx.send(res);
        info!(block_range=?&new_db_tx.block_range, "Transaction successfully committed to DB!");
    }

    /// Executes an operation.
    ///
    /// This function handles different types of write operations such as
    /// upserts, updates, and reverts, ensuring data consistency in the database.
    async fn execute_write_op(
        &mut self,
        operation: &WriteOp,
        conn: &mut AsyncPgConnection,
    ) -> Result<(), StorageError> {
        trace!(op=?operation, name="ExecuteWriteOp");
        match operation {
            WriteOp::UpsertBlock(block) => {
                self.state_gateway
                    .upsert_block(block, conn)
                    .await
            }
            WriteOp::UpsertTx(transaction) => {
                self.state_gateway
                    .upsert_tx(transaction, conn)
                    .await
            }
            WriteOp::SaveExtractionState(state) => {
                self.state_gateway
                    .save_state(state, conn)
                    .await
            }
            WriteOp::InsertContract(contracts) => {
                for contract in contracts.iter() {
                    self.state_gateway
                        .insert_contract(contract, conn)
                        .await?
                }
                Ok(())
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
            WriteOp::InsertProtocolComponents(components) => {
                let collected_components: Vec<&ProtocolComponent> = components.iter().collect();
                self.state_gateway
                    .add_protocol_components(collected_components.as_slice(), conn)
                    .await
            }
            WriteOp::InsertTokens(tokens) => {
                let collected_tokens: Vec<&ERC20Token> = tokens.iter().collect();
                self.state_gateway
                    .add_tokens(collected_tokens.as_slice(), conn)
                    .await
            }
            WriteOp::InsertComponentBalances(balances) => {
                let collected_balances: Vec<&evm::ComponentBalance> = balances.iter().collect();
                self.state_gateway
                    .add_component_balances(collected_balances.as_slice(), &self.chain, conn)
                    .await
            }
            WriteOp::UpsertProtocolState(deltas) => {
                let collected_changes: Vec<(TxHash, &ProtocolStateDelta)> = deltas
                    .iter()
                    .map(|(tx, update)| (tx.clone(), update))
                    .collect();
                let changes_slice = collected_changes.as_slice();
                self.state_gateway
                    .update_protocol_states(&self.chain, changes_slice, conn)
                    .await
            }
        }
    }
}

#[derive(Hash, Eq, PartialEq, Debug)]
struct RevertParameters {
    start_version: Option<BlockOrTimestamp>,
    end_version: BlockOrTimestamp,
}

type DeltasCache = LruCache<
    RevertParameters,
    (Vec<AccountUpdate>, Vec<ProtocolStateDelta>, Vec<ComponentBalance>),
>;

type OpenTx = (DBTransaction, oneshot::Receiver<Result<(), StorageError>>);

pub struct CachedGateway {
    // Can we batch multiple block in here without breaking things?
    // Assuming we are still syncing?

    // TODO: Remove Mutex. It is not needed but avoids changing the Extractor trait.
    open_tx: Arc<Mutex<Option<OpenTx>>>,
    tx: mpsc::Sender<DBCacheMessage>,
    pool: Pool<AsyncPgConnection>,
    state_gateway: EVMStateGateway<AsyncPgConnection>,
    lru_cache: Arc<Mutex<DeltasCache>>,
}

impl Clone for CachedGateway {
    fn clone(&self) -> Self {
        Self {
            // create a separate open tx state for new instances
            open_tx: Arc::new(Mutex::new(None)),
            tx: self.tx.clone(),
            pool: self.pool.clone(),
            state_gateway: self.state_gateway.clone(),
            lru_cache: self.lru_cache.clone(),
        }
    }
}

impl CachedGateway {
    // Accumulating transactions does not drop previous data nor are transactions nested.
    pub async fn start_transaction(&self, block: &evm::Block) {
        let mut open_tx = self.open_tx.lock().await;

        if let Some(tx) = open_tx.as_mut() {
            tx.0.block_range.end = *block;
        } else {
            let (tx, rx) = oneshot::channel();
            *open_tx = Some((
                DBTransaction {
                    block_range: BlockRange::new(block, block),
                    size: 0,
                    operations: vec![],
                    tx,
                },
                rx,
            ));
        }
    }

    async fn add_op(&self, op: WriteOp) -> Result<(), StorageError> {
        let mut open_tx = self.open_tx.lock().await;
        match open_tx.as_mut() {
            None => {
                Err(StorageError::Unexpected("Usage error: No transaction started".to_string()))
            }
            Some((tx, _)) => {
                tx.add_operation(op)?;
                Ok(())
            }
        }
    }

    pub async fn commit_transaction(&self, min_ops_batch_size: usize) -> Result<(), StorageError> {
        let mut open_tx = self.open_tx.lock().await;
        match open_tx.take() {
            None => {
                Err(StorageError::Unexpected("Usage error: Commit without transaction".to_string()))
            }
            Some((mut db_txn, rx)) => {
                if db_txn.size > min_ops_batch_size {
                    db_txn
                        .operations
                        .sort_by_key(|e| e.order_key());
                    debug!(
                        size = db_txn.size,
                        ops = ?db_txn
                            .operations
                            .iter()
                            .map(WriteOp::variant_name)
                            .collect::<Vec<_>>(),
                        "Submitting db operation batch!"
                    );
                    self.tx
                        .send(DBCacheMessage::Write(db_txn))
                        .await
                        .expect("Send message to receiver ok");
                    rx.await
                        .map_err(|_| StorageError::WriteCacheGoneAway())??;
                } else {
                    // if we are not ready to commit, give the OpenTx struct back.
                    *open_tx = Some((db_txn, rx));
                }
                Ok(())
            }
        }
    }

    #[allow(private_interfaces)]
    pub fn new(
        tx: mpsc::Sender<DBCacheMessage>,
        pool: Pool<AsyncPgConnection>,
        state_gateway: EVMStateGateway<AsyncPgConnection>,
    ) -> Self {
        CachedGateway {
            tx,
            open_tx: Arc::new(Mutex::new(None)),
            pool,
            state_gateway,
            lru_cache: Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(5).unwrap()))),
        }
    }
    pub async fn upsert_block(&self, new: &evm::Block) -> Result<(), StorageError> {
        self.add_op(WriteOp::UpsertBlock(vec![*new]))
            .await?;
        Ok(())
    }

    pub async fn upsert_tx(&self, new: &evm::Transaction) -> Result<(), StorageError> {
        self.add_op(WriteOp::UpsertTx(vec![*new]))
            .await?;
        Ok(())
    }

    pub async fn save_state(&self, new: &ExtractionState) -> Result<(), StorageError> {
        self.add_op(WriteOp::SaveExtractionState(new.clone()))
            .await?;
        Ok(())
    }

    pub async fn insert_contract(&self, new: &evm::Account) -> Result<(), StorageError> {
        self.add_op(WriteOp::InsertContract(vec![new.clone()]))
            .await?;
        Ok(())
    }

    pub async fn update_contracts(
        &self,
        new: &[(TxHash, AccountUpdate)],
    ) -> Result<(), StorageError> {
        self.add_op(WriteOp::UpdateContracts(new.to_owned()))
            .await?;
        Ok(())
    }

    pub async fn get_delta(
        &self,
        chain: &Chain,
        start_version: Option<&BlockOrTimestamp>,
        end_version: &BlockOrTimestamp,
    ) -> Result<(Vec<AccountUpdate>, Vec<ProtocolStateDelta>, Vec<ComponentBalance>), StorageError>
    {
        let mut lru_cache = self.lru_cache.lock().await;

        if start_version.is_none() {
            tracing::warn!("Get delta called with start_version = None, this might be a bug in one of the extractors")
        }

        // Construct a key for the LRU cache
        let key = RevertParameters {
            start_version: start_version.cloned(),
            end_version: end_version.clone(),
        };

        // Check if the delta is already in the LRU cache
        if let Some(delta) = lru_cache.get(&key) {
            tracing::debug!("Cached delta hit for {:?}", key);
            return Ok(delta.clone());
        }

        tracing::debug!("Cache didn't hit delta. Getting delta for {:?}", key);

        // Fetch the delta from the database
        let mut db = self.pool.get().await.unwrap();
        let accounts_delta = self
            .state_gateway
            .get_accounts_delta(chain, start_version, end_version, &mut db)
            .await?;
        let protocol_delta = self
            .state_gateway
            .get_protocol_states_delta(chain, start_version, end_version, &mut db)
            .await?;
        let balance_deltas = self
            .state_gateway
            .get_balance_deltas(chain, start_version, end_version, &mut db)
            .await?;

        // Insert the new delta into the LRU cache
        lru_cache
            .put(key, (accounts_delta.clone(), protocol_delta.clone(), balance_deltas.clone()));

        Ok((accounts_delta, protocol_delta, balance_deltas))
    }

    pub async fn update_protocol_states(
        &self,
        new: &[(TxHash, ProtocolStateDelta)],
    ) -> Result<(), StorageError> {
        self.add_op(WriteOp::UpsertProtocolState(new.to_owned()))
            .await?;
        Ok(())
    }

    pub async fn add_protocol_components(
        &self,
        new: &[evm::ProtocolComponent],
    ) -> Result<(), StorageError> {
        self.add_op(WriteOp::InsertProtocolComponents(Vec::from(new)))
            .await?;
        Ok(())
    }

    pub async fn add_tokens(&self, new: &[evm::ERC20Token]) -> Result<(), StorageError> {
        self.add_op(WriteOp::InsertTokens(Vec::from(new)))
            .await?;
        Ok(())
    }

    pub async fn add_component_balances(
        &self,
        new: &[evm::ComponentBalance],
    ) -> Result<(), StorageError> {
        self.add_op(WriteOp::InsertComponentBalances(Vec::from(new)))
            .await?;
        Ok(())
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
mod test_serial_db {
    use crate::storage::postgres::{db_fixtures, orm, testing::run_against_db, PostgresGateway};
    use ethers::{
        prelude::H256,
        types::{H160, U256},
    };
    use std::{collections::HashMap, str::FromStr};
    use tycho_types::Bytes;

    use crate::pb::tycho::evm::v1::ChangeType;

    use super::*;

    #[tokio::test]
    async fn test_write_and_flush() {
        run_against_db(|connection_pool| async move {
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

            let (tx, rx) = mpsc::channel(10);
            let write_executor = DBCacheWriteExecutor::new(
                "ethereum".to_owned(),
                Chain::Ethereum,
                connection_pool.clone(),
                gateway.clone(),
                rx,
            )
            .await;

            let handle = write_executor.run();

            // Send write block message
            let block = get_sample_block(1);
            let os_rx =
                send_write_message(&tx, block, vec![WriteOp::UpsertBlock(vec![block])]).await;
            os_rx
                .await
                .expect("Response from channel ok")
                .expect("Transaction cached");

            handle.abort();

            let block_id = BlockIdentifier::Number((Chain::Ethereum, 1));
            let fetched_block = gateway
                .get_block(&block_id, &mut connection)
                .await
                .expect("Failed to fetch extraction state");

            assert_eq!(fetched_block, block);
        })
        .await;
    }

    #[tokio::test]
    async fn test_writes_and_new_blocks() {
        run_against_db(|connection_pool| async move {
            let mut connection = connection_pool
                .get()
                .await
                .expect("Failed to get a connection from the pool");
            db_fixtures::insert_chain(&mut connection, "ethereum").await;
            db_fixtures::insert_protocol_system(&mut connection, "ambient".to_owned()).await;
            db_fixtures::insert_protocol_type(&mut connection, "ambient_pool", None, None, None)
                .await;
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

            let (tx, rx) = mpsc::channel(10);

            let write_executor = DBCacheWriteExecutor::new(
                "ethereum".to_owned(),
                Chain::Ethereum,
                connection_pool.clone(),
                gateway.clone(),
                rx,
            )
            .await;

            let handle = write_executor.run();

            // Send first block messages
            let block_1 = get_sample_block(1);
            let tx_1 = get_sample_transaction(1);
            let extraction_state_1 = get_sample_extraction(1);
            let usdc_address =
                H160::from_str("0xdAC17F958D2ee523a2206206994597C13D831ec7").unwrap();
            let token = ERC20Token::new(
                usdc_address,
                "USDT".to_string(),
                6,
                0,
                vec![Some(64), None],
                Chain::Ethereum,
                100,
            );
            let protocol_component_id = "ambient_USDT-USDC".to_owned();
            let protocol_component = ProtocolComponent {
                id: protocol_component_id.clone(),
                protocol_system: "ambient".to_string(),
                protocol_type_name: "ambient_pool".to_string(),
                chain: Default::default(),
                tokens: vec![usdc_address],
                contract_ids: vec![],
                change: ChangeType::Creation.into(),
                creation_tx: tx_1.hash,
                static_attributes: Default::default(),
                created_at: Default::default(),
            };
            let component_balance = ComponentBalance {
                token: usdc_address,
                balance_float: 0.0,
                balance: Bytes::from(&[0u8]),
                modify_tx: tx_1.hash,
                component_id: protocol_component_id.clone(),
            };
            let os_rx_1 = send_write_message(
                &tx,
                block_1,
                vec![
                    WriteOp::UpsertBlock(vec![block_1]),
                    WriteOp::UpsertTx(vec![tx_1]),
                    WriteOp::SaveExtractionState(extraction_state_1.clone()),
                    WriteOp::InsertTokens(vec![token]),
                    WriteOp::InsertProtocolComponents(vec![protocol_component]),
                    WriteOp::InsertComponentBalances(vec![component_balance]),
                ],
            )
            .await;
            os_rx_1
                .await
                .expect("Response from channel ok")
                .expect("Transaction cached");

            // Send second block messages
            let block_2 = get_sample_block(2);
            let attributes: HashMap<String, Bytes> =
                vec![("reserve1".to_owned(), Bytes::from(U256::from(1000)))]
                    .into_iter()
                    .collect();
            let protocol_state_delta = ProtocolStateDelta::new(protocol_component_id, attributes);
            let os_rx_2 = send_write_message(
                &tx,
                block_2,
                vec![
                    WriteOp::UpsertBlock(vec![block_2]),
                    WriteOp::UpsertProtocolState(vec![(
                        tx_1.hash.as_bytes().into(),
                        protocol_state_delta,
                    )]),
                ],
            )
            .await;
            os_rx_2
                .await
                .expect("Response from channel ok")
                .expect("Transaction cached");

            // Send third block messages
            let block_3 = get_sample_block(3);
            let os_rx_3 =
                send_write_message(&tx, block_3, vec![WriteOp::UpsertBlock(vec![block_3])]).await;
            os_rx_3
                .await
                .expect("Response from channel ok")
                .expect("Transaction cached");

            handle.abort();

            // Assert that transactions have been flushed
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
            let block_3 = get_sample_block(3);
            let fetched_block_3 = gateway
                .get_block(&block_id_3, &mut connection)
                .await
                .expect("Failed to fetch block");

            // Assert block 1 messages have been flushed
            assert_eq!(fetched_block_1, block_1);
            assert_eq!(fetched_tx, tx_1);
            assert_eq!(fetched_extraction_state, extraction_state_1);
            // Assert block 2 messages have been flushed
            assert_eq!(fetched_block_2, block_2);
            // Assert block 3 messages have been flushed
            assert_eq!(fetched_block_3, block_3);
        })
        .await
    }

    #[test_log::test(tokio::test)]
    async fn test_cached_gateway() {
        // Setup
        run_against_db(|connection_pool| async move {
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
            let (tx, rx) = mpsc::channel(10);

            let write_executor = DBCacheWriteExecutor::new(
                "ethereum".to_owned(),
                Chain::Ethereum,
                connection_pool.clone(),
                gateway.clone(),
                rx,
            )
            .await;

            let handle = write_executor.run();
            let cached_gw = CachedGateway::new(tx, connection_pool.clone(), gateway);

            // Send first block messages
            let block_1 = get_sample_block(1);
            let tx_1 = get_sample_transaction(1);
            cached_gw
                .start_transaction(&block_1)
                .await;
            cached_gw
                .upsert_block(&block_1)
                .await
                .expect("Upsert block 1 ok");
            cached_gw
                .upsert_tx(&tx_1)
                .await
                .expect("Upsert tx 1 ok");
            cached_gw
                .commit_transaction(0)
                .await
                .expect("committing tx failed");

            // Send second block messages
            let block_2 = get_sample_block(2);
            cached_gw
                .start_transaction(&block_2)
                .await;
            cached_gw
                .upsert_block(&block_2)
                .await
                .expect("Upsert block 2 ok");
            cached_gw
                .commit_transaction(0)
                .await
                .expect("committing tx failed");

            // Send third block messages
            let block_3 = get_sample_block(3);
            cached_gw
                .start_transaction(&block_3)
                .await;
            cached_gw
                .upsert_block(&block_3)
                .await
                .expect("Upsert block 3 ok");
            cached_gw
                .commit_transaction(0)
                .await
                .expect("committing tx failed");

            handle.abort();

            // Assert that messages from block 1,2 and 3 have been commited to the db.
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
                .expect("Failed to fetch block");

            // Assert block 1 messages have been flushed
            assert_eq!(fetched_block_1, block_1);
            assert_eq!(fetched_tx, tx_1);
            // Assert block 2 messages have been flushed
            assert_eq!(fetched_block_2, block_2);
            // Assert block 3 is still pending in cache
            assert_eq!(fetched_block_3, block_3);
        })
        .await;
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
        let db_transaction = DBTransaction {
            block_range: BlockRange::new(&block, &block),
            size: operations.len(),
            operations,
            tx: os_tx,
        };

        tx.send(DBCacheMessage::Write(db_transaction))
            .await
            .expect("Failed to send write message through mpsc channel");
        os_rx
    }

    //noinspection SpellCheckingInspection
    #[allow(dead_code)]
    async fn setup_data(conn: &mut AsyncPgConnection) {
        // set up blocks and txns
        let chain_id = db_fixtures::insert_chain(conn, "ethereum").await;
        let blk = db_fixtures::insert_blocks(conn, chain_id).await;
        let tx_hashes = [
            "0xbb7e16d797a9e2fbc537e30f91ed3d27a254dd9578aa4c3af3e5f0d3e8130945".to_string(),
            "0x794f7df7a3fe973f1583fbb92536f9a8def3a89902439289315326c04068de54".to_string(),
            "0x3108322284d0a89a7accb288d1a94384d499504fe7e04441b0706c7628dee7b7".to_string(),
            "0x50449de1973d86f21bfafa7c72011854a7e33a226709dc3e2e4edcca34188388".to_string(),
        ];

        let txn = db_fixtures::insert_txns(
            conn,
            &[
                (blk[0], 1i64, &tx_hashes[0]),
                (blk[0], 2i64, &tx_hashes[1]),
                // ----- Block 01 LAST
                (blk[1], 1i64, &tx_hashes[2]),
                (blk[1], 2i64, &tx_hashes[3]),
                // ----- Block 02 LAST
            ],
        )
        .await;

        // set up contract data
        let c0 = db_fixtures::insert_account(
            conn,
            "6B175474E89094C44Da98b954EedeAC495271d0F",
            "account0",
            chain_id,
            Some(txn[0]),
        )
        .await;
        db_fixtures::insert_account_balance(conn, 0, txn[0], Some("2020-01-01T00:00:00"), c0).await;
        db_fixtures::insert_contract_code(conn, c0, txn[0], Bytes::from_str("C0C0C0").unwrap())
            .await;
        db_fixtures::insert_account_balance(conn, 100, txn[1], Some("2020-01-01T01:00:00"), c0)
            .await;
        db_fixtures::insert_slots(conn, c0, txn[1], "2020-01-01T00:00:00", None, &[(2, 1, None)])
            .await;
        db_fixtures::insert_slots(
            conn,
            c0,
            txn[1],
            "2020-01-01T00:00:00",
            Some("2020-01-01T01:00:00"),
            &[(0, 1, None), (1, 5, None)],
        )
        .await;
        db_fixtures::insert_account_balance(conn, 101, txn[3], None, c0).await;
        db_fixtures::insert_slots(
            conn,
            c0,
            txn[3],
            "2020-01-01T01:00:00",
            None,
            &[(0, 2, Some(1)), (1, 3, Some(5)), (5, 25, None), (6, 30, None)],
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
        db_fixtures::insert_account_balance(conn, 50, txn[2], None, c1).await;
        db_fixtures::insert_contract_code(conn, c1, txn[2], Bytes::from_str("C1C1C1").unwrap())
            .await;
        db_fixtures::insert_slots(
            conn,
            c1,
            txn[3],
            "2020-01-01T01:00:00",
            None,
            &[(0, 128, None), (1, 255, None)],
        )
        .await;

        let c2 = db_fixtures::insert_account(
            conn,
            "94a3F312366b8D0a32A00986194053C0ed0CdDb1",
            "c2",
            chain_id,
            Some(txn[1]),
        )
        .await;
        db_fixtures::insert_account_balance(conn, 25, txn[1], None, c2).await;
        db_fixtures::insert_contract_code(conn, c2, txn[1], Bytes::from_str("C2C2C2").unwrap())
            .await;
        db_fixtures::insert_slots(
            conn,
            c2,
            txn[1],
            "2020-01-01T00:00:00",
            None,
            &[(1, 2, None), (2, 4, None)],
        )
        .await;
        db_fixtures::delete_account(conn, c2, "2020-01-01T01:00:00").await;

        // set up protocol state data
        let protocol_system_id =
            db_fixtures::insert_protocol_system(conn, "ambient".to_owned()).await;
        let protocol_type_id = db_fixtures::insert_protocol_type(
            conn,
            "Pool",
            Some(orm::FinancialType::Swap),
            None,
            Some(orm::ImplementationType::Custom),
        )
        .await;
        let protocol_component_id = db_fixtures::insert_protocol_component(
            conn,
            "state1",
            chain_id,
            protocol_system_id,
            protocol_type_id,
            txn[0],
            None,
            None,
        )
        .await;
        // protocol state for state1-reserve1
        db_fixtures::insert_protocol_state(
            conn,
            protocol_component_id,
            txn[0],
            "reserve1".to_owned(),
            Bytes::from(U256::from(1100)),
            None,
            Some(txn[2]),
        )
        .await;
    }
}
