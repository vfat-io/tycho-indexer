use std::{collections::HashMap, str::FromStr, sync::Arc};

use async_trait::async_trait;
use diesel_async::{pooled_connection::deadpool::Pool, AsyncPgConnection};
use ethers::prelude::H256;
use mockall::automock;
use prost::Message;
use tokio::sync::Mutex;
use tracing::{debug, instrument};

use crate::{
    extractor::{evm, evm::Block, ExtractionError, Extractor, ExtractorMsg},
    models::{Chain, ExtractionState, ExtractorIdentity, ProtocolType},
    pb::{
        sf::substreams::rpc::v2::{BlockScopedData, BlockUndoSignal, ModulesProgress},
        tycho::evm::v1::BlockEntityChanges,
    },
    storage::{
        postgres::cache::CachedGateway, BlockIdentifier, BlockOrTimestamp, StorageError, TxHash,
    },
};

// TODO: Use the same Inner as AmbientExtractor
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
        let cursor_bytes: Vec<u8> = cursor.into();
        let mut state = self.inner.lock().await;
        state.cursor = cursor_bytes;
    }

    async fn update_last_processed_block(&self, block: Block) {
        let mut state = self.inner.lock().await;
        state.last_processed_block = Some(block);
    }
}

pub struct NativePgGateway {
    name: String,
    chain: Chain,
    pool: Pool<AsyncPgConnection>,
    state_gateway: CachedGateway,
}

#[automock]
#[async_trait]
pub trait NativeGateway: Send + Sync {
    async fn get_cursor(&self) -> Result<Vec<u8>, StorageError>;

    async fn ensure_protocol_types(&self, new_protocol_types: &[ProtocolType]);

    async fn upsert_contract(
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

impl NativePgGateway {
    pub fn new(
        name: &str,
        chain: Chain,
        pool: Pool<AsyncPgConnection>,
        state_gateway: CachedGateway,
    ) -> Self {
        Self { name: name.to_owned(), chain, pool, state_gateway }
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

        for tx in changes.state_updates.iter() {
            self.state_gateway
                .upsert_tx(&changes.block, &tx.tx)
                .await?;

            let hash: TxHash = tx.tx.hash.into();

            for (_component_id, new_protocol_component) in tx.new_protocol_components.iter() {
                new_protocol_components.push(new_protocol_component.clone());
            }

            for (_component_id, state_change) in tx.protocol_states.iter() {
                state_updates.push((hash.clone(), state_change.clone()));
            }

            for (_component_id, tokens) in tx.balance_changes.iter() {
                for tvl_change in tokens.values() {
                    balance_changes.push(tvl_change.clone());
                }
            }
        }

        let block = &changes.block;

        self.state_gateway
            .add_protocol_components(block, new_protocol_components.as_slice())
            .await?;
        self.state_gateway
            .update_protocol_states(block, state_updates.as_slice())
            .await?;
        self.state_gateway
            .add_component_balances(block, balance_changes.as_slice())
            .await?;

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
            .get_protocol_components(&self.chain, Some(self.name.clone()), None, conn)
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
            // TODO: Map new_protocol_components
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
impl NativeGateway for NativePgGateway {
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
    async fn upsert_contract(
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
        let mut conn = self.pool.get().await.unwrap();
        let res = self
            .backward(current, to, new_cursor, &mut conn)
            .await?;
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
        let _data = inp
            .output
            .as_ref()
            .unwrap()
            .map_output
            .as_ref()
            .unwrap();

        let raw_msg = BlockEntityChanges::decode(_data.value.as_slice())?;

        debug!(?raw_msg, "Received message");

        // Validate protocol_type_id
        let msg = match evm::BlockEntityChanges::try_from_message(
            raw_msg,
            &self.name,
            self.chain,
            self.protocol_system.clone(),
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
            .upsert_contract(&msg, inp.cursor.as_ref())
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
    use crate::{extractor::evm, pb::sf::substreams::v1::BlockRef};

    use super::*;

    use std::collections::HashMap;
    use tycho_types::Bytes;

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
        gw.expect_upsert_contract()
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

    // TODO: What is this testing?
    // #[tokio::test]
    // async fn test_handle_tick_scoped_data_skip() {
    //     let mut gw = MockNativeGateway::new();
    //     gw.expect_ensure_protocol_types()
    //         .times(1)
    //         .returning(|_| ());
    //     gw.expect_get_cursor()
    //         .times(1)
    //         .returning(|| Ok("cursor".into()));
    //     gw.expect_upsert_contract()
    //         .times(0)
    //         .returning(|_, _| Ok(()));

    //     let protocol_types = HashMap::from([("WeightedPool".to_string(),
    // ProtocolType::default())]);     let extractor = create_extractor(gw).await;

    //     let inp = evm::fixtures::pb_block_scoped_data(());
    //     let res = extractor
    //         .handle_tick_scoped_data(inp)
    //         .await;

    //     assert_eq!(res, Ok(None));
    //     assert_eq!(extractor.get_cursor().await, "cursor@420");
    // }

    #[tokio::test]
    async fn test_handle_revert() {
        let mut gw = MockNativeGateway::new();
        gw.expect_ensure_protocol_types()
            .times(1)
            .returning(|_| ());
        gw.expect_get_cursor()
            .times(1)
            .returning(|| Ok("cursor".into()));

        gw.expect_upsert_contract()
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

    // THALES TESTS

    // #[tokio::test]
    // async fn test_update_cursor() {
    //     let extractor = create_extractor();
    //     let new_cursor = "new_cursor".to_string();

    //     // Update the cursor.
    //     extractor
    //         .update_cursor(new_cursor.clone())
    //         .await;

    //     // Lock the state to retrieve the cursor.
    //     let state = extractor.inner.lock().await;

    //     // Check if the cursor has been updated correctly.
    //     assert_eq!(state.cursor, new_cursor.into_bytes());
    // }

    // #[tokio::test]
    // async fn test_update_last_processed_block() {
    //     let extractor = create_extractor();
    //     // pub struct Block {
    //     //     pub number: u64,
    //     //     pub hash: H256,
    //     //     pub parent_hash: H256,
    //     //     pub chain: Chain,
    //     //     pub ts: NaiveDateTime,
    //     // }

    //     let new_block = Block {
    //         number: 1,
    //         hash: H256::zero(),
    //         parent_hash: H256::zero(),
    //         chain: Chain::Ethereum,
    //         ts: "2020-01-01T01:00:00"
    //             .parse()
    //             .expect("Invalid timestamp"),
    //     };

    //     // Update the last processed block.
    //     extractor
    //         .update_last_processed_block(new_block.clone())
    //         .await;

    //     // Lock the state to retrieve the last processed block.
    //     let state = extractor.inner.lock().await;

    //     // Check if the last processed block has been updated correctly.
    //     assert!(state.last_processed_block.is_some());
    //     assert_eq!(state.last_processed_block.unwrap(), new_block);
    // }
}

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
    use crate::storage::{
        postgres,
        postgres::{db_fixtures, testing::run_against_db, PostgresGateway},
        ChangeType, ContractId,
    };
    use ethers::types::U256;
    use mpsc::channel;
    use std::collections::{HashMap, HashSet};
    use actix_web::web::post;
    use ethers::prelude::H160;
    use test_serial_db::evm::ProtocolChangesWithTx;
    use tokio::sync::{
        mpsc,
        mpsc::{error::TryRecvError::Empty, Receiver},
    };
    use tycho_types::Bytes;
    use crate::extractor::evm::ambient::AmbientPgGateway;
    use crate::extractor::evm::{ProtocolComponent, ProtocolStateDelta};
    use crate::storage::postgres::orm::{FinancialType, ImplementationType};
    use crate::storage::postgres::schema::chain::dsl::chain;

    use super::*;

    const TX_HASH_0: &str = "0x2f6350a292c0fc918afe67cb893744a080dacb507b0cea4cc07437b8aff23cdb";
    const TX_HASH_1: &str = "0x0d9e0da36cf9f305a189965b248fc79c923619801e8ab5ef158d4fd528a291ad";
    const BLOCK_HASH_0: &str = "0x98b4a4fef932b1862be52de218cc32b714a295fae48b775202361a6fa09b66eb";
    const CREATED_CONTRACT: &str = "0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc";

    async fn setup_gw(
        pool: Pool<AsyncPgConnection>,
    ) -> (NativePgGateway, Receiver<StorageError>, Pool<AsyncPgConnection>) {
        let mut conn = pool
            .get()
            .await
            .expect("pool should get a connection");
        let chain_id = postgres::db_fixtures::insert_chain(&mut conn, "ethereum").await;
        postgres::db_fixtures::insert_protocol_system(&mut conn, "test".to_owned()).await;
        postgres::db_fixtures::insert_protocol_type(&mut conn, "Pool", Some(FinancialType::Swap), None, Some(ImplementationType::Custom)).await;

        // TODO: Implement token insertion logic to prevent needing this.
        postgres::db_fixtures::insert_token(&mut conn, chain_id, "A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", "USDC", 6).await;
        postgres::db_fixtures::insert_token(&mut conn, chain_id, "C02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", "WETH", 18).await;

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
        );

        let handle = write_executor.run();
        let cached_gw = CachedGateway::new(tx, pool.clone(), evm_gw.clone());

        let gw = NativePgGateway::new("native:test", Chain::Ethereum, pool.clone(), cached_gw);
        (gw, err_rx, pool)
    }

    #[tokio::test]
    async fn test_get_cursor() {
        run_against_db(|pool| async move {
            let (gw, mut err_rx, pool) = setup_gw(pool).await;
            let evm_gw = gw.state_gateway.clone();
            let state = ExtractionState::new(
                "native:test".to_string(),
                Chain::Ethereum,
                None,
                "cursor@420".as_bytes(),
            );
            let mut conn = pool
                .get()
                .await
                .expect("pool should get a connection");
            evm_gw
                .save_state(&state, &mut conn)
                .await
                .expect("extaction state insertion succeeded");

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
            block: evm::Block::default(),
            revert: false,
            state_updates: vec![ProtocolChangesWithTx {
                tx: evm::fixtures::transaction01(),
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
            new_protocol_components: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn test_upsert_contract() {
        run_against_db(|pool| async move {
            let (gw, mut err_rx, pool) = setup_gw(pool).await;
            let msg = native_pool_creation();

            let exp = [ProtocolComponent {
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
                    &mut conn,
                )
                .await
                .expect("test successfully inserted native contract");
            println!("{:?}", res);
            assert_eq!(res, exp);
            // Assert no error happened
            assert_eq!(maybe_err, Empty);
        })
            .await;
    }

    #[tokio::test]
    async fn test_revert() {
        run_against_db(|pool| async move {
            let mut conn = pool
                .get()
                .await
                .expect("pool should get a connection");
            postgres::db_fixtures::insert_chain(&mut conn, "ethereum").await;
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
            let (err_tx, mut err_rx) = channel(10);

            let write_executor = crate::storage::postgres::cache::DBCacheWriteExecutor::new(
                "ethereum".to_owned(),
                Chain::Ethereum,
                pool.clone(),
                evm_gw.clone(),
                rx,
                err_tx,
            );

            let handle = write_executor.run();
            let cached_gw = CachedGateway::new(tx, pool.clone(), evm_gw.clone());

            let gw = NativePgGateway::new("native::test", Chain::Ethereum, pool.clone(), cached_gw);

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
                    hash: BLOCK_HASH_0.parse().unwrap(),
                    parent_hash: H256::zero(),
                    ts: "2020-01-01T01:00:00".parse().unwrap(),
                },
                revert: false,
                state_updates: vec![ProtocolChangesWithTx {
                    tx: evm::fixtures::transaction01(),
                    protocol_states: HashMap::from([(TX_HASH_0.to_owned(), state)]),
                    balance_changes: HashMap::new(),
                    new_protocol_components: HashMap::new(),
                }],
                new_protocol_components: HashMap::new(),
            };

            gw.forward(&msg0, "cursor@0")
                .await
                .expect("upsert should succeed");
            gw.forward(&msg1, "cursor@1")
                .await
                .expect("upsert should succeed");


            let del_attributes: HashSet<String> = vec!["reserve1".to_owned(), "reserve2".to_owned()]
                .into_iter()
                .collect();
            let exp_change = evm::ProtocolStateDelta {
                component_id: CREATED_CONTRACT.to_string(),
                updated_attributes: HashMap::new(),
                deleted_attributes: del_attributes,
            };

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
            let res = cached_gw
                .get_protocol_components(
                    &Chain::Ethereum,
                    None,
                    Some([CREATED_CONTRACT].as_slice()),
                    &mut conn,
                )
                .await
                .expect("test successfully inserted native contract");
            // Assert no error happened
            assert_eq!(maybe_err, Empty);
        })
            .await;
    }
}
