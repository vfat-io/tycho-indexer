#![allow(unused_variables)]
#![allow(unused_imports)]

use async_trait::async_trait;
use std::{collections::HashMap, hash::Hash, str::FromStr};

use diesel::prelude::*;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use ethers::types::{transaction, Transaction, H256};
use mockall::Any;
use tracing::warn;

use crate::{
    extractor::evm::{ProtocolComponent, ProtocolState, ProtocolStateDelta},
    hex_bytes::Bytes,
    models::{Chain, ProtocolType},
    storage::{
        postgres::{orm, schema, PostgresGateway},
        Address, BlockIdentifier, BlockOrTimestamp, ChainGateway, ContractDelta, ProtocolGateway,
        StorableBlock, StorableContract, StorableProtocolComponent, StorableProtocolState,
        StorableProtocolType, StorableToken, StorableTransaction, StorageError, TxHash, Version,
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
        conn: &mut Self::DB,
    ) -> Result<Vec<ProtocolComponent>, StorageError> {
        use super::schema::{protocol_component::dsl::*, transaction::dsl::*};
        let chain_id_value = self.get_chain_id(chain);

        let mut query = protocol_component
            .inner_join(transaction.on(creation_tx.eq(schema::transaction::id)))
            .select((orm::ProtocolComponent::as_select(), hash))
            .into_boxed();

        match (system, ids) {
            (Some(ps), None) => {
                let protocol_system = self.get_protocol_system_id(&ps);
                query = query.filter(
                    chain_id
                        .eq(chain_id_value)
                        .and(protocol_system_id.eq(protocol_system)),
                );
            }
            (None, Some(external_ids)) => {
                query = query.filter(
                    chain_id
                        .eq(chain_id_value)
                        .and(external_id.eq_any(external_ids)),
                );
            }
            (Some(ps), Some(external_ids)) => {
                let protocol_system = self.get_protocol_system_id(&ps);
                query = query.filter(
                    chain_id.eq(chain_id_value).and(
                        external_id
                            .eq_any(external_ids)
                            .and(protocol_system_id.eq(protocol_system)),
                    ),
                );
            }
            (_, _) => {
                query = query.filter(chain_id.eq(chain_id_value));
            }
        }

        let orm_protocol_components = query
            .load::<(orm::ProtocolComponent, TxHash)>(conn)
            .await?;

        orm_protocol_components
            .into_iter()
            .map(|(pc, tx_hash)| {
                let ps = self.get_protocol_system(&pc.protocol_system_id);
                ProtocolComponent::from_storage(
                    pc,
                    vec![],
                    vec![],
                    chain.to_owned(),
                    &ps,
                    tx_hash.into(),
                )
            })
            .collect::<Result<Vec<ProtocolComponent>, StorageError>>()
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
        address: Option<&[&Address]>,
        conn: &mut Self::DB,
    ) -> Result<Vec<Self::Token>, StorageError> {
        todo!()
    }

    async fn add_tokens(
        &self,
        chain: Chain,
        token: &[&Self::Token],
        conn: &mut Self::DB,
    ) -> Result<(), StorageError> {
        todo!()
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
    use crate::{
        extractor::{evm, evm::ContractId},
        storage::ChangeType,
    };
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
    use diesel_async::AsyncConnection;
    use ethers::types::U256;
    use rstest::rstest;
    use serde_json::{json, Value};

    use crate::{
        models,
        models::{FinancialType, ImplementationType},
        storage::postgres::{db_fixtures, orm, schema, PostgresGateway},
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
    async fn setup_data(conn: &mut AsyncPgConnection) -> Vec<String> {
        let chain_id = db_fixtures::insert_chain(conn, "ethereum").await;
        let chain_id_sn = db_fixtures::insert_chain(conn, "starknet").await;
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
        let protocol_system_id_ambient =
            db_fixtures::insert_protocol_system(conn, "ambient".to_owned()).await;
        let protocol_system_id_zz =
            db_fixtures::insert_protocol_system(conn, "zigzag".to_owned()).await;
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
            protocol_system_id_ambient,
            protocol_type_id,
            txn[0],
        )
        .await;

        db_fixtures::insert_protocol_component(
            conn,
            "state2",
            chain_id_sn,
            protocol_system_id_zz,
            protocol_type_id,
            txn[1],
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
        tx_hashes.to_vec()
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
            id: ContractId("test_contract_id".to_string()),
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
        assert_eq!(original_component.id.0, inserted_data.external_id);
    }
    #[rstest]
    #[case::get_one(Some("zigzag".to_string()))]
    #[case::get_none(Some("ambient".to_string()))]
    #[tokio::test]
    async fn test_get_protocol_components_with_system_only(#[case] system: Option<String>) {
        let mut conn = setup_db().await;
        let tx_hashes = setup_data(&mut conn).await;
        let gw = EVMGateway::from_connection(&mut conn).await;

        let chain = Chain::Starknet;

        let result = gw
            .get_protocol_components(&chain, system.clone(), None, &mut conn)
            .await;

        assert!(result.is_ok());

        match system.unwrap().as_str() {
            "zigzag" => {
                let components = result.unwrap();
                assert_eq!(components.len(), 1);

                let pc = &components[0];
                assert_eq!(pc.id, ContractId("state2".to_string()));
                assert_eq!(pc.protocol_system, "zigzag");
                assert_eq!(pc.chain, Chain::Starknet);
                assert_eq!(pc.creation_tx, H256::from_str(tx_hashes.get(1).unwrap()).unwrap());
            }
            "ambient" => {
                let components = result.unwrap();
                assert_eq!(components.len(), 0)
            }
            _ => {}
        }
    }

    #[rstest]
    #[case::get_one("state1".to_string())]
    #[case::get_none("state2".to_string())]
    #[tokio::test]
    async fn test_get_protocol_components_with_external_id_only(#[case] external_id: String) {
        let mut conn = setup_db().await;
        let tx_hashes = setup_data(&mut conn).await;
        let gw = EVMGateway::from_connection(&mut conn).await;

        let temp_ids_array = [external_id.as_str()];
        let ids = Some(temp_ids_array.as_slice());
        let chain = Chain::Ethereum;

        let result = gw
            .get_protocol_components(&chain, None, ids, &mut conn)
            .await;

        match external_id.as_str() {
            "state1" => {
                let components = result.unwrap();
                assert_eq!(components.len(), 1);

                let pc = &components[0];
                assert_eq!(pc.id, ContractId(external_id.to_string()));
                assert_eq!(pc.protocol_system, "ambient");
                assert_eq!(pc.chain, Chain::Ethereum);
                assert_eq!(pc.creation_tx, H256::from_str(&tx_hashes[0].to_string()).unwrap());
            }
            "state2" => {
                let components = result.unwrap();
                assert_eq!(components.len(), 0)
            }
            _ => {}
        }
    }

    #[tokio::test]
    async fn test_get_protocol_components_with_system_and_ids() {
        let mut conn = setup_db().await;
        let tx_hashes = setup_data(&mut conn).await;
        let gw = EVMGateway::from_connection(&mut conn).await;

        let system = "ambient".to_string();
        let ids = Some(["state1", "state2"].as_slice());
        let chain = Chain::Ethereum;
        let result = gw
            .get_protocol_components(&chain, Some(system), ids, &mut conn)
            .await;

        let components = result.unwrap();
        assert_eq!(components.len(), 1);

        let pc = &components[0];
        assert_eq!(pc.id, ContractId("state1".to_string()));
        assert_eq!(pc.protocol_system, "ambient");
        assert_eq!(pc.chain, Chain::Ethereum);
        assert_eq!(pc.creation_tx, H256::from_str(&tx_hashes[0].to_string()).unwrap());
    }

    #[rstest]
    #[case::get_one(Chain::Ethereum, 0)]
    #[case::get_none(Chain::Starknet, 1)]
    #[tokio::test]
    async fn test_get_protocol_components_with_chain_filter(#[case] chain: Chain, #[case] i: i64) {
        let mut conn = setup_db().await;
        let tx_hashes = setup_data(&mut conn).await;
        let gw = EVMGateway::from_connection(&mut conn).await;

        let result = gw
            .get_protocol_components(&chain, None, None, &mut conn)
            .await;

        let components = result.unwrap();
        assert_eq!(components.len(), 1);

        let pc = &components[0];
        assert_eq!(pc.id, ContractId(format!("state{}", i + 1).to_string()));
        assert_eq!(pc.chain, chain);
        let i_usize: usize = i as usize;
        assert_eq!(pc.creation_tx, H256::from_str(&tx_hashes[i_usize].to_string()).unwrap());
    }
}
