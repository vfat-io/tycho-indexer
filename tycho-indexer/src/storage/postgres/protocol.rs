#![allow(unused_variables)]
#![allow(unused_imports)]

use async_trait::async_trait;
use std::{collections::HashMap, hash::Hash};

use diesel::prelude::*;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use ethers::types::{transaction, Transaction};
use tracing::warn;

use crate::{
    extractor::evm::{ERC20Token, ProtocolComponent, ProtocolState, ProtocolStateDelta},
    hex_bytes::Bytes,
    models::{Chain, ProtocolType},
    storage::{
        postgres::{
            orm,
            orm::{Account, NewAccount, Token},
            schema,
            schema::chain,
            PostgresGateway,
        },
        Address, BlockIdentifier, BlockOrTimestamp, ContractDelta, ContractId, ProtocolGateway,
        StorableBlock, StorableContract, StorableProtocolComponent, StorableProtocolState,
        StorableProtocolType, StorableToken, StorableTransaction, StorageError,
        StorageError::DecodeError,
        TxHash, Version,
    },
};

impl<B, TX, A, D, T> PostgresGateway<B, TX, A, D, T>
where
    B: StorableBlock<orm::Block, orm::NewBlock, i64>,
    TX: StorableTransaction<orm::Transaction, orm::NewTransaction, i64>,
    D: ContractDelta + From<A>,
    A: StorableContract<orm::Contract, orm::NewContract, i64>,
    T: StorableToken<orm::Token, orm::NewToken, i64>,
{
    /// Decodes a ProtocolStates database result. Combines all matching protocol state db entities
    /// and returns a list containing one ProtocolState per component.
    fn _decode_protocol_states(
        &self,
        result: Result<Vec<(orm::ProtocolState, String, orm::Transaction)>, diesel::result::Error>,
        context: &str,
    ) -> Result<Vec<ProtocolState>, StorageError> {
        match result {
            Ok(data_vec) => {
                let mut protocol_states = Vec::new();
                let (states_data, latest_tx): (
                    HashMap<String, Vec<orm::ProtocolState>>,
                    Option<orm::Transaction>,
                ) = data_vec.into_iter().fold(
                    (HashMap::new(), None),
                    |(mut states, latest_tx), data| {
                        states
                            .entry(data.1)
                            .or_insert_with(Vec::new)
                            .push(data.0);
                        let transaction = data.2;
                        let latest_tx = match latest_tx {
                            Some(latest)
                                if latest.block_id < transaction.block_id ||
                                    (latest.block_id == transaction.block_id &&
                                        latest.index < transaction.index) =>
                            {
                                Some(transaction)
                            }
                            None => Some(transaction),
                            _ => latest_tx,
                        };
                        (states, latest_tx)
                    },
                );
                for (component_id, states) in states_data {
                    let tx_hash = latest_tx
                        .as_ref()
                        .map(|tx| &tx.hash)
                        .ok_or(StorageError::DecodeError("Modify tx hash not found".to_owned()))?;
                    let protocol_state =
                        ProtocolState::from_storage(states, component_id, tx_hash)?;
                    protocol_states.push(protocol_state);
                }
                Ok(protocol_states)
            }
            Err(err) => Err(StorageError::from_diesel(err, "ProtocolStates", context, None)),
        }
    }
}

#[async_trait]
impl<B, TX, A, D, T> ProtocolGateway for PostgresGateway<B, TX, A, D, T>
where
    B: StorableBlock<orm::Block, orm::NewBlock, i64>,
    TX: StorableTransaction<orm::Transaction, orm::NewTransaction, i64>,
    D: ContractDelta + From<A>,
    A: StorableContract<orm::Contract, orm::NewContract, i64>,
    T: StorableToken<orm::Token, orm::NewToken, i64>,
{
    type DB = AsyncPgConnection;
    type Token = T;
    type ProtocolState = ProtocolState;
    type ProtocolStateDelta = ProtocolStateDelta;
    type ProtocolType = ProtocolType;
    type ProtocolComponent = ProtocolComponent;

    async fn get_protocol_components(
        &self,
        chain: &Chain,
        system: Option<String>,
        ids: Option<&[&str]>,
    ) -> Result<Vec<ProtocolComponent>, StorageError> {
        todo!()
    }

    async fn add_protocol_components(
        &self,
        new: &[&Self::ProtocolComponent],
        conn: &mut Self::DB,
    ) -> Result<(), StorageError> {
        use super::schema::protocol_component::dsl::*;
        let mut values: Vec<orm::NewProtocolComponent> = Vec::with_capacity(new.len());
        let tx_hashes: Vec<TxHash> = new
            .iter()
            .map(|pc| pc.creation_tx.into())
            .collect();
        let tx_hash_id_mapping: HashMap<TxHash, i64> =
            orm::Transaction::id_by_hash(&tx_hashes, conn)
                .await
                .unwrap();

        for pc in new {
            let txh = tx_hash_id_mapping
                .get::<TxHash>(&pc.creation_tx.into())
                .ok_or(StorageError::DecodeError("TxHash not found".to_string()))?;

            let new_pc = pc
                .to_storage(
                    self.get_chain_id(&pc.chain),
                    self.get_protocol_system_id(&pc.protocol_system.to_string()),
                    txh.to_owned(),
                    pc.created_at,
                )
                .unwrap();
            values.push(new_pc);
        }

        diesel::insert_into(protocol_component)
            .values(&values)
            .on_conflict((chain_id, protocol_system_id, external_id))
            .do_nothing()
            .execute(conn)
            .await
            .map_err(|err| StorageError::from_diesel(err, "ProtocolComponent", "", None))
            .unwrap();

        Ok(())
    }

    async fn upsert_protocol_type(
        &self,
        new: &Self::ProtocolType,
        conn: &mut Self::DB,
    ) -> Result<(), StorageError> {
        use super::schema::protocol_type::dsl::*;

        let values: orm::NewProtocolType = new.to_storage();

        diesel::insert_into(protocol_type)
            .values(&values)
            .on_conflict(name)
            .do_update()
            .set(&values)
            .execute(conn)
            .await
            .map_err(|err| StorageError::from_diesel(err, "ProtocolType", &values.name, None))?;

        Ok(())
    }

    // Gets all protocol states from the db filtered by chain, component ids and/or protocol system.
    // The filters are applied in the following order: component ids, protocol system, chain. If
    // component ids are provided, the protocol system filter is ignored. The chain filter is
    // always applied.
    async fn get_protocol_states(
        &self,
        chain: &Chain,
        at: Option<Version>,
        system: Option<String>,
        ids: Option<&[&str]>,
        conn: &mut Self::DB,
    ) -> Result<Vec<Self::ProtocolState>, StorageError> {
        let chain_db_id = self.get_chain_id(chain);
        let version_ts = match &at {
            Some(version) => Some(version.to_ts(conn).await?),
            None => None,
        };

        match (ids, system) {
            (Some(ids), Some(system)) => {
                warn!("Both protocol IDs and system were provided. System will be ignored.");
                self._decode_protocol_states(
                    orm::ProtocolState::by_id(ids, chain_db_id, version_ts, conn).await,
                    ids.join(",").as_str(),
                )
            }
            (Some(ids), _) => self._decode_protocol_states(
                orm::ProtocolState::by_id(ids, chain_db_id, version_ts, conn).await,
                ids.join(",").as_str(),
            ),
            (_, Some(system)) => self._decode_protocol_states(
                orm::ProtocolState::by_protocol_system(
                    system.clone(),
                    chain_db_id,
                    version_ts,
                    conn,
                )
                .await,
                system.to_string().as_str(),
            ),
            _ => self._decode_protocol_states(
                orm::ProtocolState::by_chain(chain_db_id, version_ts, conn).await,
                chain.to_string().as_str(),
            ),
        }
    }

    async fn update_protocol_state(
        &self,
        chain: Chain,
        new: &[(TxHash, ProtocolStateDelta)],
        conn: &mut Self::DB,
    ) {
        todo!()
    }

    async fn get_tokens(
        &self,
        chain: Chain,
        addresses: Option<&[&Address]>,
        conn: &mut Self::DB,
    ) -> Result<Vec<Self::Token>, StorageError> {
        use super::schema::{account::dsl::*, token::dsl::*};

        let mut query = token
            .inner_join(account)
            .select((token::all_columns(), schema::account::chain_id, schema::account::address))
            .into_boxed();

        if let Some(addrs) = addresses {
            query = query.filter(schema::account::address.eq_any(addrs));
        }

        let results = query
            .order(schema::token::symbol.asc())
            .load::<(orm::Token, i64, Address)>(conn)
            .await
            .map_err(|err| StorageError::from_diesel(err, "Token", &chain.to_string(), None))?;

        let tokens: Result<Vec<Self::Token>, StorageError> = results
            .into_iter()
            .map(|(orm_token, chain_id_, address_)| {
                let chain = self.get_chain(&chain_id_);
                let contract_id = ContractId::new(chain, address_);

                Self::Token::from_storage(orm_token, contract_id)
                    .map_err(|err| StorageError::DecodeError(err.to_string()))
            })
            .collect();
        tokens
    }

    async fn add_tokens(
        &self,
        tokens: &[&Self::Token],
        conn: &mut Self::DB,
    ) -> Result<(), StorageError> {
        let titles: Vec<String> = tokens
            .iter()
            .map(|token| format!("{}_{}", token.chain(), token.symbol()))
            .collect();

        let addresses: Vec<_> = tokens
            .iter()
            .map(|token| token.address().as_bytes().to_vec())
            .collect();

        let new_accounts: Vec<NewAccount> = tokens
            .iter()
            .zip(titles.iter())
            .zip(addresses.iter())
            .map(|((token, title), address)| {
                let chain_id = self.get_chain_id(&token.chain());
                NewAccount {
                    title,
                    address,
                    chain_id,
                    creation_tx: None,
                    created_at: None,
                    deleted_at: None,
                }
            })
            .collect();

        diesel::insert_into(schema::account::table)
            .values(&new_accounts)
            // .on_conflict(..).do_nothing() is necessary to ignore updating duplicated entries
            .on_conflict((schema::account::address, schema::account::chain_id))
            .do_nothing()
            .execute(conn)
            .await
            .map_err(|err| StorageError::from_diesel(err, "Account", "batch", None))?;

        let accounts: Vec<Account> = schema::account::table
            .filter(schema::account::address.eq_any(addresses))
            .select(Account::as_select())
            .get_results::<Account>(conn)
            .await
            .map_err(|err| StorageError::from_diesel(err, "Account", "retrieve", None))?;

        let account_map: HashMap<(Vec<u8>, i64), i64> = accounts
            .iter()
            .map(|account| ((account.address.clone().to_vec(), account.chain_id), account.id))
            .collect();

        let new_tokens: Vec<orm::NewToken> = tokens
            .iter()
            .map(|token| {
                let token_chain_id = self.get_chain_id(&token.chain());
                let account_key = (token.address().as_ref().to_vec(), token_chain_id);

                let account_id = *account_map
                    .get(&account_key)
                    .expect("Account ID not found");

                token.to_storage(account_id)
            })
            .collect();

        diesel::insert_into(schema::token::table)
            .values(&new_tokens)
            // .on_conflict(..).do_nothing() is necessary to ignore updating duplicated entries
            .on_conflict(schema::token::account_id)
            .do_nothing()
            .execute(conn)
            .await
            .map_err(|err| StorageError::from_diesel(err, "Token", "batch", None))?;

        Ok(())
    }

    async fn get_state_delta(
        &self,
        chain: &Chain,
        system: Option<String>,
        id: Option<&[&str]>,
        start_version: Option<&BlockOrTimestamp>,
        end_version: &BlockOrTimestamp,
        conn: &mut Self::DB,
    ) -> Result<ProtocolStateDelta, StorageError> {
        todo!()
    }

    async fn revert_protocol_state(
        &self,
        to: &BlockIdentifier,
        conn: &mut Self::DB,
    ) -> Result<(), StorageError> {
        todo!()
    }

    async fn _get_or_create_protocol_system_id(
        &self,
        new: String,
        conn: &mut Self::DB,
    ) -> Result<i64, StorageError> {
        use super::schema::protocol_system::dsl::*;

        let existing_entry = protocol_system
            .filter(name.eq(new.to_string().clone()))
            .first::<orm::ProtocolSystem>(conn)
            .await;

        if let Ok(entry) = existing_entry {
            return Ok(entry.id);
        } else {
            let new_entry = orm::NewProtocolSystem { name: new.to_string() };

            let inserted_protocol_system = diesel::insert_into(protocol_system)
                .values(&new_entry)
                .get_result::<orm::ProtocolSystem>(conn)
                .await
                .map_err(|err| {
                    StorageError::from_diesel(err, "ProtocolSystem", &new.to_string(), None)
                })?;
            Ok(inserted_protocol_system.id)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{extractor::evm, storage::ChangeType};
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
    use diesel_async::AsyncConnection;
    use ethers::{prelude::H160, types::U256};
    use hex::FromHex;
    use rstest::rstest;
    use serde_json::{json, Value};

    use crate::{
        models,
        models::{FinancialType, ImplementationType},
        storage::postgres::{db_fixtures, orm, schema, schema::token::dsl::token, PostgresGateway},
    };

    use super::*;
    use ethers::prelude::H256;
    use std::{collections::HashMap, str::FromStr};

    type EVMGateway = PostgresGateway<
        evm::Block,
        evm::Transaction,
        evm::Account,
        evm::AccountUpdate,
        evm::ERC20Token,
    >;

    const WETH: &str = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2";
    const USDC: &str = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48";
    const USDT: &str = "0xdAC17F958D2ee523a2206206994597C13D831ec7";

    async fn setup_db() -> AsyncPgConnection {
        let db_url = std::env::var("DATABASE_URL").unwrap();
        let mut conn = AsyncPgConnection::establish(&db_url)
            .await
            .unwrap();
        conn.begin_test_transaction()
            .await
            .unwrap();

        conn
    }

    /// This sets up the data needed to test the gateway. The setup is structured such that each
    /// protocol state's historical changes are kept together this makes it easy to reason about
    /// that change an account should have at each version Please not that if you change
    /// something here, also update the state fixtures right below, which contain protocol states
    /// at each version.
    async fn setup_data(conn: &mut AsyncPgConnection) {
        let chain_id = db_fixtures::insert_chain(conn, "ethereum").await;
        let blk = db_fixtures::insert_blocks(conn, chain_id).await;
        let txn = db_fixtures::insert_txns(
            conn,
            &[
                (
                    blk[0],
                    1i64,
                    "0xbb7e16d797a9e2fbc537e30f91ed3d27a254dd9578aa4c3af3e5f0d3e8130945",
                ),
                (
                    blk[0],
                    2i64,
                    "0x794f7df7a3fe973f1583fbb92536f9a8def3a89902439289315326c04068de54",
                ),
                // ----- Block 01 LAST
                (
                    blk[1],
                    1i64,
                    "0x3108322284d0a89a7accb288d1a94384d499504fe7e04441b0706c7628dee7b7",
                ),
                (
                    blk[1],
                    2i64,
                    "0x50449de1973d86f21bfafa7c72011854a7e33a226709dc3e2e4edcca34188388",
                ),
                // ----- Block 02 LAST
            ],
        )
        .await;
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
        )
        .await;

        // protocol state for state1-reserve1
        db_fixtures::insert_protocol_state(
            conn,
            protocol_component_id,
            txn[0],
            "reserve1".to_owned(),
            Bytes::from(U256::from(1100)),
            Some(txn[2]),
        )
        .await;

        // protocol state for state1-reserve2
        db_fixtures::insert_protocol_state(
            conn,
            protocol_component_id,
            txn[0],
            "reserve2".to_owned(),
            Bytes::from(U256::from(500)),
            None,
        )
        .await;

        // protocol state update for state1-reserve1
        db_fixtures::insert_protocol_state(
            conn,
            protocol_component_id,
            txn[2],
            "reserve1".to_owned(),
            Bytes::from(U256::from(1000)),
            None,
        )
        .await;

        // insert tokens
        let weth_id =
            db_fixtures::insert_token(conn, chain_id, WETH.trim_start_matches("0x"), "WETH", 18)
                .await;
        let usdc_id =
            db_fixtures::insert_token(conn, chain_id, USDC.trim_start_matches("0x"), "USDC", 6)
                .await;
    }

    fn protocol_state() -> ProtocolState {
        let attributes: HashMap<String, Bytes> = vec![
            ("reserve1".to_owned(), Bytes::from(U256::from(1000))),
            ("reserve2".to_owned(), Bytes::from(U256::from(500))),
        ]
        .into_iter()
        .collect();
        ProtocolState::new(
            "state1".to_owned(),
            attributes,
            "0x3108322284d0a89a7accb288d1a94384d499504fe7e04441b0706c7628dee7b7"
                .parse()
                .unwrap(),
        )
    }

    #[rstest]
    #[case::by_chain(None, None)]
    #[case::by_system(Some("ambient".to_string()), None)]
    #[case::by_ids(None, Some(vec!["state1"]))]
    #[tokio::test]
    async fn test_get_protocol_states(
        #[case] system: Option<String>,
        #[case] ids: Option<Vec<&str>>,
    ) {
        let mut conn = setup_db().await;
        setup_data(&mut conn).await;

        let expected = vec![protocol_state()];

        let gateway = EVMGateway::from_connection(&mut conn).await;

        let result = gateway
            .get_protocol_states(&Chain::Ethereum, None, system, ids.as_deref(), &mut conn)
            .await
            .unwrap();

        assert_eq!(result, expected)
    }

    #[tokio::test]
    async fn test_get_protocol_states_at() {
        let mut conn = setup_db().await;
        setup_data(&mut conn).await;

        let gateway = EVMGateway::from_connection(&mut conn).await;

        let mut protocol_state = protocol_state();
        let attributes: HashMap<String, Bytes> = vec![
            ("reserve1".to_owned(), Bytes::from(U256::from(1100))),
            ("reserve2".to_owned(), Bytes::from(U256::from(500))),
        ]
        .into_iter()
        .collect();
        protocol_state.attributes = attributes;
        protocol_state.modify_tx =
            "0xbb7e16d797a9e2fbc537e30f91ed3d27a254dd9578aa4c3af3e5f0d3e8130945"
                .parse()
                .unwrap();
        let expected = vec![protocol_state];

        let result = gateway
            .get_protocol_states(
                &Chain::Ethereum,
                Some(Version::from_block_number(Chain::Ethereum, 1)),
                None,
                None,
                &mut conn,
            )
            .await
            .unwrap();

        assert_eq!(result, expected)
    }

    #[tokio::test]
    async fn test_get_or_create_protocol_system_id() {
        let mut conn = setup_db().await;
        let gw = EVMGateway::from_connection(&mut conn).await;

        let first_id = gw
            ._get_or_create_protocol_system_id("ambient".to_string(), &mut conn)
            .await
            .unwrap();

        let second_id = gw
            ._get_or_create_protocol_system_id("ambient".to_string(), &mut conn)
            .await
            .unwrap();
        assert!(first_id > 0);
        assert_eq!(first_id, second_id);
    }

    #[tokio::test]
    async fn test_add_protocol_type() {
        let mut conn = setup_db().await;
        let gw = EVMGateway::from_connection(&mut conn).await;

        let d = NaiveDate::from_ymd_opt(2015, 6, 3).unwrap();
        let t = NaiveTime::from_hms_milli_opt(12, 34, 56, 789).unwrap();
        let dt = NaiveDateTime::new(d, t);

        let protocol_type = models::ProtocolType {
            name: "Protocol".to_string(),
            financial_type: FinancialType::Debt,
            attribute_schema: Some(json!({"attribute": "schema"})),
            implementation: ImplementationType::Custom,
        };

        gw.upsert_protocol_type(&protocol_type, &mut conn)
            .await
            .unwrap();

        let inserted_data = schema::protocol_type::table
            .filter(schema::protocol_type::name.eq("Protocol"))
            .select(schema::protocol_type::all_columns)
            .first::<orm::ProtocolType>(&mut conn)
            .await
            .unwrap();

        assert_eq!(inserted_data.name, "Protocol".to_string());
        assert_eq!(inserted_data.financial_type, orm::FinancialType::Debt);
        assert_eq!(inserted_data.attribute_schema, Some(json!({"attribute": "schema"})));
        assert_eq!(inserted_data.implementation, orm::ImplementationType::Custom);

        let updated_protocol_type = models::ProtocolType {
            name: "Protocol".to_string(),
            financial_type: FinancialType::Leverage,
            attribute_schema: Some(json!({"attribute": "another_schema"})),
            implementation: ImplementationType::Vm,
        };

        gw.upsert_protocol_type(&updated_protocol_type, &mut conn)
            .await
            .unwrap();

        let newly_inserted_data = schema::protocol_type::table
            .filter(schema::protocol_type::name.eq("Protocol"))
            .select(schema::protocol_type::all_columns)
            .load::<orm::ProtocolType>(&mut conn)
            .await
            .unwrap();

        assert_eq!(newly_inserted_data.len(), 1);
        assert_eq!(newly_inserted_data[0].name, "Protocol".to_string());
        assert_eq!(newly_inserted_data[0].financial_type, orm::FinancialType::Leverage);
        assert_eq!(
            newly_inserted_data[0].attribute_schema,
            Some(json!({"attribute": "another_schema"}))
        );
        assert_eq!(newly_inserted_data[0].implementation, orm::ImplementationType::Vm);
    }

    #[tokio::test]
    async fn test_get_tokens() {
        let mut conn = setup_db().await;
        setup_data(&mut conn).await;
        let gw = EVMGateway::from_connection(&mut conn).await;

        // get all tokens (no address filter)
        let tokens = gw
            .get_tokens(Chain::Ethereum, None, &mut conn)
            .await
            .unwrap();
        assert_eq!(tokens.len(), 2);

        // get weth and usdc
        let tokens = gw
            .get_tokens(Chain::Ethereum, Some(&[&WETH.into(), &USDC.into()]), &mut conn)
            .await
            .unwrap();
        assert_eq!(tokens.len(), 2);

        // get weth
        let tokens = gw
            .get_tokens(Chain::Ethereum, Some(&[&WETH.into()]), &mut conn)
            .await
            .unwrap();
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].symbol, "WETH".to_string());
        assert_eq!(tokens[0].decimals, 18);
    }

    #[tokio::test]
    async fn test_add_tokens() {
        let mut conn = setup_db().await;
        setup_data(&mut conn).await;
        let gw = EVMGateway::from_connection(&mut conn).await;

        // Insert one new token (USDT) and an existing token (WETH)
        let weth_symbol = "WETH".to_string();
        let old_token = db_fixtures::get_token_by_symbol(&mut conn, weth_symbol.clone()).await;
        let old_account = &orm::Account::by_address(
            &Bytes::from_str(WETH.trim_start_matches("0x")).expect("address ok"),
            &mut conn,
        )
        .await
        .unwrap()[0];

        let usdt_symbol = "USDT".to_string();
        let tokens = [
            &ERC20Token {
                address: H160::from_str(USDT).unwrap(),
                symbol: usdt_symbol.clone(),
                decimals: 6,
                tax: 0,
                gas: vec![Some(64), None],
                chain: Chain::Ethereum,
            },
            &ERC20Token {
                address: H160::from_str(WETH).unwrap(),
                symbol: weth_symbol.clone(),
                decimals: 18,
                tax: 0,
                gas: vec![Some(100), None],
                chain: Chain::Ethereum,
            },
        ];

        gw.add_tokens(&tokens, &mut conn)
            .await
            .unwrap();

        let inserted_token = db_fixtures::get_token_by_symbol(&mut conn, usdt_symbol.clone()).await;
        assert_eq!(inserted_token.symbol, usdt_symbol);
        assert_eq!(inserted_token.decimals, 6);
        let inserted_account = &orm::Account::by_address(
            &Bytes::from_str(USDT.trim_start_matches("0x")).expect("address ok"),
            &mut conn,
        )
        .await
        .unwrap()[0];
        assert_eq!(inserted_account.id, inserted_token.account_id);
        assert_eq!(inserted_account.title, "ethereum_USDT".to_string());

        // make sure nothing changed on WETH (ids included)
        let new_token = db_fixtures::get_token_by_symbol(&mut conn, weth_symbol.clone()).await;
        assert_eq!(new_token, old_token);
        let new_account = &orm::Account::by_address(
            &Bytes::from_str(WETH.trim_start_matches("0x")).expect("address ok"),
            &mut conn,
        )
        .await
        .unwrap()[0];
        assert_eq!(new_account, old_account);
        assert!(inserted_account.id > new_account.id);
    }

    #[tokio::test]
    async fn test_add_protocol_components() {
        let mut conn = setup_db().await;
        setup_data(&mut conn).await;
        let gw = EVMGateway::from_connection(&mut conn).await;
        let protocol_type_id_1 =
            db_fixtures::insert_protocol_type(&mut conn, "Test_Type_1", None, None, None).await;
        let protocol_type_id_2 =
            db_fixtures::insert_protocol_type(&mut conn, "Test_Type_2", None, None, None).await;
        let protocol_system = "ambient".to_string();
        let chain = Chain::Ethereum;
        let original_component = ProtocolComponent {
            id: "test_contract_id".to_string(),
            protocol_system,
            protocol_type_id: protocol_type_id_1.to_string(),
            chain,
            tokens: vec![],
            contract_ids: vec![],
            static_attributes: HashMap::new(),
            change: ChangeType::Creation,
            creation_tx: H256::from_str(
                "0xbb7e16d797a9e2fbc537e30f91ed3d27a254dd9578aa4c3af3e5f0d3e8130945",
            )
            .unwrap(),
            created_at: Default::default(),
        };

        let result = gw
            .add_protocol_components(&[&original_component.clone()], &mut conn)
            .await;

        assert!(result.is_ok());

        let inserted_data = schema::protocol_component::table
            .filter(schema::protocol_component::external_id.eq("test_contract_id".to_string()))
            .select(orm::ProtocolComponent::as_select())
            .first::<orm::ProtocolComponent>(&mut conn)
            .await;

        assert!(inserted_data.is_ok());
        let inserted_data: orm::ProtocolComponent = inserted_data.unwrap();
        assert_eq!(
            original_component.protocol_type_id,
            inserted_data
                .protocol_type_id
                .to_string()
        );
        assert_eq!(
            original_component.protocol_type_id,
            inserted_data
                .protocol_type_id
                .to_string()
        );
        assert_eq!(
            gw.get_protocol_system_id(
                &original_component
                    .protocol_system
                    .to_string()
            ),
            inserted_data.protocol_system_id
        );
        assert_eq!(gw.get_chain_id(&original_component.chain), inserted_data.chain_id);
        assert_eq!(original_component.id, inserted_data.external_id);
    }
}
