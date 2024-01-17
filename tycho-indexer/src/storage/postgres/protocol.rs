#![allow(unused_variables)]
#![allow(unused_imports)]

use async_trait::async_trait;
use std::collections::HashMap;

use diesel::prelude::*;
use diesel_async::{AsyncPgConnection, RunQueryDsl};

use crate::{
    extractor::evm::{ProtocolComponent, ProtocolState},
    models::{Chain, ProtocolSystem, ProtocolType},
    storage::{
        postgres::{orm, PostgresGateway},
        Address, BlockIdentifier, BlockOrTimestamp, ContractDelta, ProtocolGateway, StorableBlock,
        StorableContract, StorableToken, StorableTransaction, StorageError, TxHash, Version,
    },
};

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
    type ProtocolType = ProtocolType;
    type ProtocolComponent = ProtocolComponent;

    async fn get_components(
        &self,
        chain: &Chain,
        system: Option<ProtocolSystem>,
        ids: Option<&[&str]>,
    ) -> Result<Vec<ProtocolComponent>, StorageError> {
        todo!()
    }

    async fn upsert_components(
        &self,
        new: &[&Self::ProtocolComponent],
        conn: &mut Self::DB,
    ) -> Result<(), StorageError> {
        use super::schema::protocol_component::dsl::*;
        let mut values: Vec<orm::NewProtocolComponent> = vec![];
        //let values: Vec<NewProtocolComponent> = new
        //    .into_iter()
        //    .map(|pc| async {
        //        pc.to_storage(
        //            self.get_chain_id(&pc.chain),
        //            self._get_or_create_protocol_system_id(pc.protocol_system, conn)
        //                .await?,
        //           Default::default(),
        //      )
        //  })
        //  .collect::<Vec<_>>();

        println!("a");
        for pc in new {
            let new_pc = pc
                .to_storage(self.get_chain_id(&pc.chain), 0, Default::default())
                .unwrap();
            values.push(new_pc);
        }
        println!("b");
        diesel::insert_into(protocol_component)
            .values(&values)
            .on_conflict((chain_id, protocol_system_id, external_id))
            .do_update()
            .set(values.get(0).unwrap())
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

    async fn get_states(
        &self,
        chain: &Chain,
        at: Option<Version>,
        system: Option<ProtocolSystem>,
        id: Option<&[&str]>,
    ) -> Result<Vec<ProtocolState>, StorageError> {
        let block_chain_id = self.get_chain_id(chain);
        todo!()
    }

    async fn update_state(&self, chain: Chain, new: &[(TxHash, ProtocolState)], db: &mut Self::DB) {
        todo!()
    }

    async fn get_tokens(
        &self,
        chain: Chain,
        address: Option<&[&Address]>,
    ) -> Result<Vec<Self::Token>, StorageError> {
        todo!()
    }

    async fn add_tokens(&self, chain: Chain, token: &[&Self::Token]) -> Result<(), StorageError> {
        todo!()
    }

    async fn get_state_delta(
        &self,
        chain: &Chain,
        system: Option<ProtocolSystem>,
        id: Option<&[&str]>,
        start_version: Option<&BlockOrTimestamp>,
        end_version: &BlockOrTimestamp,
        conn: &mut Self::DB,
    ) -> Result<ProtocolState, StorageError> {
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
        new: ProtocolSystem,
        conn: &mut Self::DB,
    ) -> Result<i64, StorageError> {
        use super::schema::protocol_system::dsl::*;
        let new_system = orm::ProtocolSystemType::from(new);

        let existing_entry = protocol_system
            .filter(name.eq(new_system.clone()))
            .first::<orm::ProtocolSystem>(conn)
            .await;

        if let Ok(entry) = existing_entry {
            return Ok(entry.id);
        } else {
            let new_entry = orm::NewProtocolSystem { name: new_system };

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
    use serde_json::json;

    use crate::{
        extractor::evm,
        models,
        models::{FinancialType, ImplementationType},
        storage::postgres::{orm, schema, PostgresGateway},
    };

    use super::*;
    use std::collections::HashMap;

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

    #[tokio::test]
    async fn test_get_or_create_protocol_system_id() {
        let mut conn = setup_db().await;
        let gw = EVMGateway::from_connection(&mut conn).await;

        let protocol_system_id = gw
            ._get_or_create_protocol_system_id(ProtocolSystem::Ambient, &mut conn)
            .await
            .unwrap();
        assert_eq!(protocol_system_id, 1);

        let protocol_system_id = gw
            ._get_or_create_protocol_system_id(ProtocolSystem::Ambient, &mut conn)
            .await
            .unwrap();
        assert_eq!(protocol_system_id, 1);
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
    async fn test_upsert_components() {
        let mut conn = setup_db().await;

        let gw = EVMGateway::from_connection(&mut conn).await;
        gw.chain_id_cache
            .map_enum
            .lock()
            .unwrap()
            .insert(1, Chain::Ethereum);

        gw.chain_id_cache
            .map_id
            .lock()
            .unwrap()
            .insert(Chain::Ethereum, 1);
        // Define test data
        let protocol_system = ProtocolSystem::default(); // Replace with actual test data
        let chain = Chain::default(); // Replace with actual test data
        let new_component = ProtocolComponent {
            id: ContractId("test_contract_id".to_string()),
            protocol_system: protocol_system.clone(),
            protocol_type_id: "1".to_string(),
            chain: chain.clone(),
            tokens: vec![],
            contract_ids: vec![],
            static_attributes: HashMap::new(),
            change: ChangeType::Creation,
        };

        // Call the function under test
        let result = gw
            .upsert_components(&[&new_component.clone()], &mut conn)
            .await;

        // Assert the result
        assert!(result.is_ok());

        // Optionally, you can query the database to verify the inserted data
        use crate::storage::postgres::schema::protocol_component::dsl::*;
        //let inserted_data = protocol_component
        //    .filter(external_id.eq("test_contract_id"))
        //    .first::<ProtocolComponent>(&mut conn)
        //    .optional()
        //    .await;

        // Assert that the data was inserted as expected
        //assert!(inserted_data.is_ok());
        //let inserted_data = inserted_data.unwrap();
        //assert_eq!(Some(new_component), inserted_data);
    }
}
