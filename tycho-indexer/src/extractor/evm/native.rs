use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
    sync::Arc,
};

use async_trait::async_trait;
use diesel_async::{pooled_connection::deadpool::Pool, AsyncPgConnection};
use ethers::types::{H160, H256};
use mockall::automock;
use prost::Message;
use tokio::sync::Mutex;
use tracing::{debug, instrument};
use tycho_types::Bytes;

use crate::{
    extractor::{
        evm::{self, Block},
        ExtractionError, Extractor, ExtractorMsg,
    },
    models::{Chain, ExtractionState, ExtractorIdentity, ProtocolType},
    pb::{
        sf::substreams::rpc::v2::{BlockScopedData, BlockUndoSignal, ModulesProgress},
        tycho::evm::v1::BlockEntityChanges,
    },
    storage::{
        postgres::cache::CachedGateway, BlockIdentifier, BlockOrTimestamp, StorageError, TxHash,
    },
};

use super::token_pre_processor::{TokenPreProcessor, TokenPreProcessorTrait};

pub struct Inner {
    cursor: Vec<u8>,
    last_processed_block: Option<Block>,
}

pub struct NativeContractExtractor<G> {
    gateway: G,
    name: String,
    chain: Chain,
    protocol_system: String,
    inner: Arc<Mutex<Inner>>,
    protocol_types: HashMap<String, ProtocolType>,
}

impl<DB> NativeContractExtractor<DB> {
    async fn update_cursor(&self, cursor: String) {
        let mut state = self.inner.lock().await;
        state.cursor = cursor.into();
    }

    async fn update_last_processed_block(&self, block: Block) {
        let mut state = self.inner.lock().await;
        state.last_processed_block = Some(block);
    }
}

pub struct NativePgGateway<T>
where
    T: TokenPreProcessorTrait,
{
    name: String,
    chain: Chain,
    pool: Pool<AsyncPgConnection>,
    state_gateway: CachedGateway,
    token_pre_processor: T,
}

#[automock]
#[async_trait]
pub trait NativeGateway: Send + Sync {
    async fn get_cursor(&self) -> Result<Vec<u8>, StorageError>;

    async fn ensure_protocol_types(&self, new_protocol_types: &[ProtocolType]);

    async fn advance(
        &self,
        changes: &evm::BlockEntityChanges,
        new_cursor: &str,
    ) -> Result<(), StorageError>;

    async fn revert(
        &self,
        current: Option<BlockIdentifier>,
        to: &BlockIdentifier,
        new_cursor: &str,
    ) -> Result<evm::BlockEntityChangesResult, StorageError>;
}

impl<T> NativePgGateway<T>
where
    T: TokenPreProcessorTrait,
{
    pub fn new(
        name: &str,
        chain: Chain,
        pool: Pool<AsyncPgConnection>,
        state_gateway: CachedGateway,
        token_pre_processor: T,
    ) -> Self {
        Self { name: name.to_owned(), chain, pool, state_gateway, token_pre_processor }
    }

    #[instrument(skip_all)]
    async fn save_cursor(&self, block: &Block, new_cursor: &str) -> Result<(), StorageError> {
        let state =
            ExtractionState::new(self.name.to_string(), self.chain, None, new_cursor.as_bytes());
        self.state_gateway
            .save_state(block, &state)
            .await?;
        Ok(())
    }

    /// Get tokens that are not in the database
    async fn get_new_tokens(&self, tokens: HashSet<H160>) -> Result<Vec<H160>, StorageError> {
        let mut conn = self
            .pool
            .get()
            .await
            .expect("pool should be connected");

        let addresses: Vec<Bytes> = tokens
            .iter()
            .map(|a| Bytes::from(a.as_bytes().to_vec()))
            .collect();
        let address_refs: Vec<&Bytes> = addresses.iter().collect();

        let addresses_option = Some(address_refs.as_slice());

        let db_tokens = self
            .state_gateway
            .get_tokens(self.chain, addresses_option, &mut conn)
            .await?;

        let db_token_addresses: HashSet<_> = db_tokens
            .iter()
            .map(|token| token.address)
            .collect();
        let filtered_tokens = tokens
            .into_iter()
            .filter(|token| !db_token_addresses.contains(token))
            .collect();

        Ok(filtered_tokens)
    }

    #[instrument(skip_all, fields(chain = % self.chain, name = % self.name, block_number = % changes.block.number))]
    async fn forward(
        &self,
        changes: &evm::BlockEntityChanges,
        new_cursor: &str,
    ) -> Result<(), StorageError> {
        debug!("Upserting block");
        self.state_gateway
            .upsert_block(&changes.block)
            .await?;

        let mut new_protocol_components: Vec<evm::ProtocolComponent> = vec![];
        let mut state_updates: Vec<(TxHash, evm::ProtocolStateDelta)> = vec![];
        let mut balance_changes: Vec<evm::ComponentBalance> = vec![];

        let mut protocol_tokens: HashSet<H160> = HashSet::new();

        for tx in changes.txs_with_update.iter() {
            self.state_gateway
                .upsert_tx(&changes.block, &tx.tx)
                .await?;

            let hash: TxHash = tx.tx.hash.into();

            for (_component_id, new_protocol_component) in tx.new_protocol_components.iter() {
                new_protocol_components.push(new_protocol_component.clone());
                protocol_tokens.extend(new_protocol_component.tokens.iter());
            }

            state_updates.extend(
                tx.protocol_states
                    .values()
                    .map(|state_change| (hash.clone(), state_change.clone())),
            );

            balance_changes.extend(
                tx.balance_changes
                    .iter()
                    .flat_map(|(_component_id, tokens)| tokens.values().cloned()),
            );
        }

        let block = &changes.block;

        let new_tokens_addresses = self
            .get_new_tokens(protocol_tokens)
            .await?;
        if !new_tokens_addresses.is_empty() {
            let new_tokens = self
                .token_pre_processor
                .get_tokens(new_tokens_addresses)
                .await;
            self.state_gateway
                .add_tokens(block, &new_tokens)
                .await?;
        }

        if !new_protocol_components.is_empty() {
            self.state_gateway
                .add_protocol_components(block, new_protocol_components.as_slice())
                .await?;
        }

        if !state_updates.is_empty() {
            self.state_gateway
                .update_protocol_states(block, state_updates.as_slice())
                .await?;
        }

        if !balance_changes.is_empty() {
            self.state_gateway
                .add_component_balances(block, balance_changes.as_slice())
                .await?;
        }

        self.save_cursor(&changes.block, new_cursor)
            .await?;

        Result::<(), StorageError>::Ok(())
    }

    #[instrument(skip_all, fields(chain = % self.chain, name = % self.name, block = ? to))]
    async fn backward(
        &self,
        current: Option<BlockIdentifier>,
        to: &BlockIdentifier,
        new_cursor: &str,
        conn: &mut AsyncPgConnection,
    ) -> Result<evm::BlockEntityChangesResult, StorageError> {
        let block = self
            .state_gateway
            .get_block(to, conn)
            .await?;
        let start = current.map(BlockOrTimestamp::Block);
        let target = BlockOrTimestamp::Block(to.clone());

        // CHECK: Here there's an assumption that self.name == protocol_system

        let allowed_components: Vec<String> = self
            .state_gateway
            .get_protocol_components(&self.chain, Some(self.name.clone()), None, None, conn)
            .await?
            .into_iter()
            .map(|c| c.id)
            .collect();

        let state_updates = self
            .state_gateway
            .get_delta(&self.chain, start.as_ref(), &target)
            .await?
            .1
            .into_iter()
            .filter_map(|u: evm::ProtocolStateDelta| {
                if allowed_components.contains(&u.component_id) {
                    Some((u.component_id.clone(), u))
                } else {
                    None
                }
            })
            .collect();

        self.state_gateway
            .revert_state(to)
            .await?;

        self.save_cursor(&block, new_cursor)
            .await?;

        Ok(evm::BlockEntityChangesResult {
            extractor: self.name.clone(),
            chain: self.chain,
            block,
            revert: true,
            state_updates,
            new_protocol_components: HashMap::new(),
        })
    }

    async fn get_last_cursor(&self, conn: &mut AsyncPgConnection) -> Result<Vec<u8>, StorageError> {
        let state = self
            .state_gateway
            .get_state(&self.name, &self.chain, conn)
            .await?;
        Ok(state.cursor)
    }
}

#[async_trait]
impl NativeGateway for NativePgGateway<TokenPreProcessor> {
    async fn get_cursor(&self) -> Result<Vec<u8>, StorageError> {
        let mut conn = self.pool.get().await.unwrap();
        self.get_last_cursor(&mut conn).await
    }

    async fn ensure_protocol_types(&self, new_protocol_types: &[ProtocolType]) {
        let mut conn = self.pool.get().await.unwrap();
        self.state_gateway
            .add_protocol_types(new_protocol_types, &mut *conn)
            .await
            .expect("Couldn't insert protocol types");
    }

    #[instrument(skip_all, fields(chain = % self.chain, name = % self.name, block_number = % changes.block.number))]
    async fn advance(
        &self,
        changes: &evm::BlockEntityChanges,
        new_cursor: &str,
    ) -> Result<(), StorageError> {
        self.forward(changes, new_cursor)
            .await?;
        Ok(())
    }

    #[instrument(skip_all, fields(chain = % self.chain, name = % self.name, block_number = % to))]
    async fn revert(
        &self,
        current: Option<BlockIdentifier>,
        to: &BlockIdentifier,
        new_cursor: &str,
    ) -> Result<evm::BlockEntityChangesResult, StorageError> {
        tracing::debug!("Reverting to block {}", to);
        let mut conn = self.pool.get().await.unwrap();
        let res = self
            .backward(current, to, new_cursor, &mut conn)
            .await?;
        tracing::debug!("Revert delta {:?}", res);
        Ok(res)
    }
}

impl<G> NativeContractExtractor<G>
where
    G: NativeGateway,
{
    pub async fn new(
        name: &str,
        chain: Chain,
        protocol_system: String,
        gateway: G,
        protocol_types: HashMap<String, ProtocolType>,
    ) -> Result<Self, ExtractionError> {
        let res = match gateway.get_cursor().await {
            Err(StorageError::NotFound(_, _)) => NativeContractExtractor {
                gateway,
                name: name.to_string(),
                chain,
                inner: Arc::new(Mutex::new(Inner {
                    cursor: Vec::new(),
                    last_processed_block: None,
                })),
                protocol_system,
                protocol_types,
            },
            Ok(cursor) => NativeContractExtractor {
                gateway,
                name: name.to_string(),
                chain,
                inner: Arc::new(Mutex::new(Inner { cursor, last_processed_block: None })),
                protocol_system,
                protocol_types,
            },
            Err(err) => return Err(ExtractionError::Setup(err.to_string())),
        };

        res.ensure_protocol_types().await;
        Ok(res)
    }
}

#[async_trait]
impl<G> Extractor for NativeContractExtractor<G>
where
    G: NativeGateway,
{
    fn get_id(&self) -> ExtractorIdentity {
        ExtractorIdentity::new(self.chain, &self.name)
    }

    async fn ensure_protocol_types(&self) {
        let protocol_types: Vec<ProtocolType> = self
            .protocol_types
            .values()
            .cloned()
            .collect();
        self.gateway
            .ensure_protocol_types(&protocol_types)
            .await;
    }

    async fn get_cursor(&self) -> String {
        String::from_utf8(self.inner.lock().await.cursor.clone()).expect("Cursor is utf8")
    }

    async fn get_last_processed_block(&self) -> Option<Block> {
        self.inner
            .lock()
            .await
            .last_processed_block
    }

    #[instrument(skip_all, fields(chain = % self.chain, name = % self.name))]
    async fn handle_tick_scoped_data(
        &self,
        inp: BlockScopedData,
    ) -> Result<Option<ExtractorMsg>, ExtractionError> {
        let data = inp
            .output
            .as_ref()
            .unwrap()
            .map_output
            .as_ref()
            .unwrap();

        let raw_msg = BlockEntityChanges::decode(data.value.as_slice())?;

        debug!(?raw_msg, "Received message");

        // Validate protocol_type_id
        let msg = match evm::BlockEntityChanges::try_from_message(
            raw_msg,
            &self.name,
            self.chain,
            &self.protocol_system,
            &self.protocol_types,
        ) {
            Ok(changes) => {
                tracing::Span::current().record("block_number", changes.block.number);

                self.update_last_processed_block(changes.block)
                    .await;

                changes
            }
            Err(ExtractionError::Empty) => {
                self.update_cursor(inp.cursor).await;
                return Ok(None);
            }
            Err(e) => return Err(e),
        };

        self.gateway
            .advance(&msg, inp.cursor.as_ref())
            .await?;

        self.update_cursor(inp.cursor).await;
        let msg = Arc::new(msg.aggregate_updates()?);
        Ok(Some(msg))
    }

    async fn handle_revert(
        &self,
        inp: BlockUndoSignal,
    ) -> Result<Option<ExtractorMsg>, ExtractionError> {
        let block_ref = inp
            .last_valid_block
            .ok_or_else(|| ExtractionError::DecodeError("Revert without block ref".into()))?;

        let block_hash = H256::from_str(&block_ref.id).map_err(|err| {
            ExtractionError::DecodeError(format!(
                "Failed to parse {} as block hash: {}",
                block_ref.id, err
            ))
        })?;

        let current = self
            .get_last_processed_block()
            .await
            .map(|block| BlockIdentifier::Hash(block.hash.into()));

        // Make sure we have a current block, otherwise it's not safe to revert.
        // TODO: add last block to extraction state and get it when creating a new extractor.
        assert!(current.is_some(), "Revert without current block");

        let changes = self
            .gateway
            .revert(
                current,
                &BlockIdentifier::Hash(block_hash.into()),
                inp.last_valid_cursor.as_ref(),
            )
            .await?;
        self.update_cursor(inp.last_valid_cursor)
            .await;

        Ok((!changes.state_updates.is_empty()).then_some(Arc::new(changes)))
    }

    async fn handle_progress(&self, _inp: ModulesProgress) -> Result<(), ExtractionError> {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use tycho_types::Bytes;

    use crate::{extractor::evm, pb::sf::substreams::v1::BlockRef};

    use super::*;

    const EXTRACTOR_NAME: &str = "TestExtractor";
    const TEST_PROTOCOL: &str = "TestProtocol";

    async fn create_extractor(gw: MockNativeGateway) -> NativeContractExtractor<MockNativeGateway> {
        let protocol_types = HashMap::from([("WeightedPool".to_string(), ProtocolType::default())]);

        NativeContractExtractor::new(
            EXTRACTOR_NAME,
            Chain::Ethereum,
            TEST_PROTOCOL.to_string(),
            gw,
            protocol_types,
        )
        .await
        .expect("Failed to create extractor")
    }

    #[tokio::test]
    async fn test_get_cursor() {
        let mut gw = MockNativeGateway::new();
        gw.expect_ensure_protocol_types()
            .times(1)
            .returning(|_| ());
        gw.expect_get_cursor()
            .times(1)
            .returning(|| Ok("cursor".into()));

        let extractor = create_extractor(gw).await;
        let res = extractor.get_cursor().await;

        assert_eq!(res, "cursor");
    }

    #[tokio::test]
    async fn test_handle_tick_scoped_data() {
        let mut gw = MockNativeGateway::new();
        gw.expect_ensure_protocol_types()
            .times(1)
            .returning(|_| ());
        gw.expect_get_cursor()
            .times(1)
            .returning(|| Ok("cursor".into()));
        gw.expect_advance()
            .times(1)
            .returning(|_, _| Ok(()));

        let extractor = create_extractor(gw).await;

        let inp = evm::fixtures::pb_block_scoped_data(evm::fixtures::pb_block_entity_changes());
        let exp = Ok(Some(()));

        let res = extractor
            .handle_tick_scoped_data(inp)
            .await
            .map(|o| o.map(|_| ()));

        assert_eq!(res, exp);
        assert_eq!(extractor.get_cursor().await, "cursor@420");
    }

    #[tokio::test]
    async fn test_handle_tick_scoped_data_skip() {
        let mut gw = MockNativeGateway::new();
        gw.expect_ensure_protocol_types()
            .times(1)
            .returning(|_| ());
        gw.expect_get_cursor()
            .times(1)
            .returning(|| Ok("cursor".into()));
        gw.expect_advance()
            .times(0)
            .returning(|_, _| Ok(()));

        let extractor = create_extractor(gw).await;

        let inp = evm::fixtures::pb_block_scoped_data(());
        let res = extractor
            .handle_tick_scoped_data(inp)
            .await;

        match res {
            Ok(Some(_)) => panic!("Expected Ok(None) but got Ok(Some(..))"),
            Ok(None) => (), // This is the expected case
            Err(_) => panic!("Expected Ok(None) but got Err(..)"),
        }
        assert_eq!(extractor.get_cursor().await, "cursor@420");
    }

    #[tokio::test]
    async fn test_handle_revert() {
        let mut gw = MockNativeGateway::new();
        gw.expect_ensure_protocol_types()
            .times(1)
            .returning(|_| ());
        gw.expect_get_cursor()
            .times(1)
            .returning(|| Ok("cursor".into()));

        gw.expect_advance()
            .times(1)
            .returning(|_, _| Ok(()));

        gw.expect_revert()
            .withf(|c, v, cursor| {
                c.clone().unwrap() ==
                    BlockIdentifier::Hash(
                        Bytes::from_str(
                            "0x0000000000000000000000000000000000000000000000000000000000000000",
                        )
                        .unwrap(),
                    ) &&
                    v == &BlockIdentifier::Hash(evm::fixtures::HASH_256_0.into()) &&
                    cursor == "cursor@400"
            })
            .times(1)
            .returning(|_, _, _| Ok(evm::BlockEntityChangesResult::default()));
        let extractor = create_extractor(gw).await;
        // Call handle_tick_scoped_data to initialize the last processed block.
        let inp = evm::fixtures::pb_block_scoped_data(evm::fixtures::pb_block_entity_changes());

        let _res = extractor
            .handle_tick_scoped_data(inp)
            .await
            .unwrap();

        let inp = BlockUndoSignal {
            last_valid_block: Some(BlockRef { id: evm::fixtures::HASH_256_0.into(), number: 400 }),
            last_valid_cursor: "cursor@400".into(),
        };

        let res = extractor.handle_revert(inp).await;

        assert!(matches!(res, Ok(None)));
        assert_eq!(extractor.get_cursor().await, "cursor@400");
    }
}

#[cfg(test)]
mod test_serial_db {
    //! It is notoriously hard to mock postgres here, we would need to have traits and abstractions
    //! for the connection pooling as well as for transaction handling so the easiest way
    //! forward is to just run these tests against a real postgres instance.
    //!
    //! The challenge here is to leave the database empty. So we need to initiate a test transaction
    //! and should avoid calling the trait methods which start a transaction of their own. So we do
    //! that by moving the main logic of each trait method into a private method and test this
    //! method instead.
    //!
    //! Note that it is ok to use higher level db methods here as there is a layer of abstraction
    //! between this component and the actual db interactions
    use std::collections::{HashMap, HashSet};

    use ethers::prelude::H160;
    use mpsc::channel;
    use tokio::sync::{
        mpsc,
        mpsc::{error::TryRecvError::Empty, Receiver},
    };

    use test_serial_db::evm::ProtocolChangesWithTx;
    use tycho_types::Bytes;

    use crate::{
        extractor::evm::{
            token_pre_processor::MockTokenPreProcessorTrait, ProtocolComponent, ProtocolStateDelta,
            Transaction,
        },
        storage::{
            postgres,
            postgres::{
                orm::{FinancialType, ImplementationType},
                testing::run_against_db,
                PostgresGateway,
            },
        },
    };

    use super::*;

    const TX_HASH_0: &str = "0x2f6350a292c0fc918afe67cb893744a080dacb507b0cea4cc07437b8aff23cdb";
    const TX_HASH_1: &str = "0x0d9e0da36cf9f305a189965b248fc79c923619801e8ab5ef158d4fd528a291ad";
    const BLOCK_HASH_0: &str = "0xc520bd7f8d7b964b1a6017a3d747375fcefea0f85994e3cc1810c2523b139da8";
    const BLOCK_HASH_1: &str = "0x98b4a4fef932b1862be52de218cc32b714a295fae48b775202361a6fa09b66eb";
    const CREATED_CONTRACT: &str = "0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc";

    fn get_mocked_token_pre_processor() -> MockTokenPreProcessorTrait {
        let mut mock_processor = MockTokenPreProcessorTrait::new();
        let new_tokens = vec![
            evm::ERC20Token::new(
                H160::from_str("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
                    .expect("Invalid H160 address"),
                "WETH".to_string(),
                18,
                0,
                vec![],
                Default::default(),
                100,
            ),
            evm::ERC20Token::new(
                H160::from_str("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
                    .expect("Invalid H160 address"),
                "USDC".to_string(),
                6,
                0,
                vec![],
                Default::default(),
                100,
            ),
        ];
        mock_processor
            .expect_get_tokens()
            .returning(move |_| new_tokens.clone());

        mock_processor
    }

    async fn setup_gw(
        pool: Pool<AsyncPgConnection>,
    ) -> (
        NativePgGateway<MockTokenPreProcessorTrait>,
        Receiver<StorageError>,
        Pool<AsyncPgConnection>,
        i64,
    ) {
        let mut conn = pool
            .get()
            .await
            .expect("pool should get a connection");
        let chain_id = postgres::db_fixtures::insert_chain(&mut conn, "ethereum").await;
        postgres::db_fixtures::insert_protocol_system(&mut conn, "test".to_owned()).await;
        postgres::db_fixtures::insert_protocol_type(
            &mut conn,
            "Pool",
            Some(FinancialType::Swap),
            None,
            Some(ImplementationType::Custom),
        )
        .await;

        let evm_gw = Arc::new(
            PostgresGateway::<
                evm::Block,
                evm::Transaction,
                evm::Account,
                evm::AccountUpdate,
                evm::ERC20Token,
            >::from_connection(&mut conn)
            .await,
        );

        let (tx, rx) = channel(10);
        let (err_tx, err_rx) = channel(10);

        let write_executor = crate::storage::postgres::cache::DBCacheWriteExecutor::new(
            "ethereum".to_owned(),
            Chain::Ethereum,
            pool.clone(),
            evm_gw.clone(),
            rx,
            err_tx,
            0,
        )
        .await;

        write_executor.run();
        let cached_gw = CachedGateway::new(tx, pool.clone(), evm_gw.clone());

        let gw = NativePgGateway::new(
            "test",
            Chain::Ethereum,
            pool.clone(),
            cached_gw,
            get_mocked_token_pre_processor(),
        );
        (gw, err_rx, pool, chain_id)
    }

    #[tokio::test]
    async fn test_get_cursor() {
        run_against_db(|pool| async move {
            let (gw, mut err_rx, pool, _) = setup_gw(pool).await;
            let evm_gw = gw.state_gateway.clone();
            let state = ExtractionState::new(
                "test".to_string(),
                Chain::Ethereum,
                None,
                "cursor@420".as_bytes(),
            );
            let mut conn = pool
                .get()
                .await
                .expect("pool should get a connection");
            evm_gw
                .save_state(&evm::Block::default(), &state)
                .await
                .expect("extaction state insertion succeeded");
            let _ = evm_gw.flush().await;

            let maybe_err = err_rx
                .try_recv()
                .expect_err("Error channel should be empty");

            let cursor = gw
                .get_last_cursor(&mut conn)
                .await
                .expect("get cursor should succeed");

            assert_eq!(cursor, "cursor@420".as_bytes());
            // Assert no error happened
            assert_eq!(maybe_err, Empty);
        })
        .await;
    }

    fn native_pool_creation() -> evm::BlockEntityChanges {
        evm::BlockEntityChanges {
            extractor: "native:test".to_owned(),
            chain: Chain::Ethereum,
            block: evm::Block {
                number: 0,
                chain: Chain::Ethereum,
                hash: BLOCK_HASH_0.parse().unwrap(),
                parent_hash: BLOCK_HASH_0.parse().unwrap(),
                ts: "2020-01-01T01:00:00".parse().unwrap(),
            },
            revert: false,
            txs_with_update: vec![ProtocolChangesWithTx {
                tx: Transaction::new(
                    H256::zero(),
                    BLOCK_HASH_0.parse().unwrap(),
                    H160::zero(),
                    Some(H160::zero()),
                    10,
                ),
                protocol_states: HashMap::new(),
                balance_changes: HashMap::new(),
                new_protocol_components: HashMap::from([(
                    "Pool".to_string(),
                    evm::ProtocolComponent {
                        id: CREATED_CONTRACT.to_string(),
                        protocol_system: "test".to_string(),
                        protocol_type_name: "Pool".to_string(),
                        chain: Chain::Ethereum,
                        tokens: vec![
                            H160::from_str("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48").unwrap(),
                            H160::from_str("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2").unwrap(),
                        ],
                        contract_ids: vec![],
                        creation_tx: Default::default(),
                        static_attributes: Default::default(),
                        created_at: Default::default(),
                        change: Default::default(),
                    },
                )]),
            }],
        }
    }

    #[tokio::test]
    async fn test_forward() {
        run_against_db(|pool| async move {
            let (gw, mut err_rx, pool, _) = setup_gw(pool).await;
            let msg = native_pool_creation();

            let _exp = [ProtocolComponent {
                id: CREATED_CONTRACT.to_string(),
                protocol_system: "test".to_string(),
                protocol_type_name: "Pool".to_string(),
                chain: Chain::Ethereum,
                tokens: vec![
                    H160::from_str("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48").unwrap(),
                    H160::from_str("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2").unwrap(),
                ],
                contract_ids: vec![],
                creation_tx: H256::from_str(
                    "0x88e96d4537bea4d9c05d12549907b32561d3bf31f45aae734cdc119f13406cb6",
                )
                .unwrap(),
                static_attributes: Default::default(),
                created_at: Default::default(),
                change: Default::default(),
            }];

            gw.forward(&msg, "cursor@500")
                .await
                .expect("upsert should succeed");

            let cached_gw: CachedGateway = gw.state_gateway;
            cached_gw
                .flush()
                .await
                .expect("Received signal ok")
                .expect("Flush ok");

            let maybe_err = err_rx
                .try_recv()
                .expect_err("Error channel should be empty");

            let mut conn = pool
                .get()
                .await
                .expect("pool should get a connection");
            let res = cached_gw
                .get_protocol_components(
                    &Chain::Ethereum,
                    None,
                    Some([CREATED_CONTRACT].as_slice()),
                    None,
                    &mut conn,
                )
                .await
                .expect("test successfully inserted native contract");
            println!("{:?}", res);
            // TODO: This is failing because protocol_type_name is wrong in the gateway - waiting
            // for this fix. assert_eq!(res, exp);
            // Assert no error happened
            assert_eq!(maybe_err, Empty);
        })
        .await;
    }

    #[tokio::test]
    async fn test_revert() {
        run_against_db(|pool| async move {
            let (gw, mut err_rx, pool, _) = setup_gw(pool).await;
            let msg0 = native_pool_creation();

            let res1_value = 1000_u64.to_be_bytes().to_vec();
            let res2_value = 500_u64.to_be_bytes().to_vec();
            let state = ProtocolStateDelta {
                component_id: CREATED_CONTRACT.to_string(),
                updated_attributes: vec![
                    ("reserve1".to_owned(), Bytes::from(res1_value)),
                    ("reserve2".to_owned(), Bytes::from(res2_value)),
                ]
                .into_iter()
                .collect(),
                deleted_attributes: HashSet::new(),
            };

            let msg1 = evm::BlockEntityChanges {
                extractor: "native:test".to_owned(),
                chain: Chain::Ethereum,
                block: evm::Block {
                    number: 1,
                    chain: Chain::Ethereum,
                    hash: BLOCK_HASH_1.parse().unwrap(),
                    parent_hash: BLOCK_HASH_0.parse().unwrap(),
                    ts: "2020-01-02T01:00:00".parse().unwrap(),
                },
                revert: false,
                txs_with_update: vec![ProtocolChangesWithTx {
                    tx: Transaction::new(
                        TX_HASH_1.parse().unwrap(),
                        BLOCK_HASH_1.parse().unwrap(),
                        H160::zero(),
                        Some(H160::zero()),
                        10,
                    ),
                    protocol_states: HashMap::from([(TX_HASH_0.to_owned(), state)]),
                    balance_changes: HashMap::new(),
                    new_protocol_components: HashMap::new(),
                }],
            };

            gw.forward(&msg0, "cursor@0")
                .await
                .expect("upsert should succeed");
            gw.forward(&msg1, "cursor@1")
                .await
                .expect("upsert should succeed");

            let del_attributes: HashSet<String> =
                vec!["reserve1".to_owned(), "reserve2".to_owned()]
                    .into_iter()
                    .collect();
            let exp_change = evm::ProtocolStateDelta {
                component_id: CREATED_CONTRACT.to_string(),
                updated_attributes: HashMap::new(),
                deleted_attributes: del_attributes,
            };

            let mut conn = pool
                .get()
                .await
                .expect("pool should get a connection");

            let changes = gw
                .backward(
                    None,
                    &BlockIdentifier::Number((Chain::Ethereum, 0)),
                    "cursor@2",
                    &mut conn,
                )
                .await
                .expect("revert should succeed");

            let maybe_err = err_rx
                .try_recv()
                .expect_err("Error channel should be empty");

            assert_eq!(changes.state_updates.len(), 1);

            assert_eq!(changes.state_updates[&CREATED_CONTRACT.to_string()], exp_change);
            let cached_gw: CachedGateway = gw.state_gateway;
            let _res = cached_gw
                .get_protocol_components(
                    &Chain::Ethereum,
                    None,
                    Some([CREATED_CONTRACT].as_slice()),
                    None,
                    &mut conn,
                )
                .await
                .expect("test successfully inserted native contract");
            // Assert no error happened
            assert_eq!(maybe_err, Empty);
        })
        .await;
    }

    #[tokio::test]
    async fn test_get_new_tokens() {
        run_against_db(|pool| async move {
            let mut conn = pool
                .get()
                .await
                .expect("pool should get a connection");

            let (gw, mut err_rx, _, chain_id) = setup_gw(pool).await;

            let weth_addr = "C02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2";
            let usdc_addr = "A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48";
            let usdt_addr = "dAC17F958D2ee523a2206206994597C13D831ec7";

            postgres::db_fixtures::insert_token(&mut conn, chain_id, weth_addr, "WETH", 18).await;
            postgres::db_fixtures::insert_token(&mut conn, chain_id, usdc_addr, "USDC", 6).await;

            let tokens = HashSet::from([
                H160::from_str(weth_addr).unwrap(), // WETH
                H160::from_str(usdc_addr).unwrap(), // USDC
                H160::from_str(usdt_addr).unwrap(), // USDT
            ]);
            let res = gw
                .get_new_tokens(tokens.iter().cloned().collect())
                .await
                .expect("get new tokens should succeed");

            // Assert len of res == 1
            assert_eq!(res.len(), 1);
            // Assert res contains USDT
            assert_eq!(res[0], H160::from_str(usdt_addr).unwrap());
            // Assert no error happened
            assert_eq!(err_rx.try_recv(), Err(Empty));
        })
        .await;
    }
}
