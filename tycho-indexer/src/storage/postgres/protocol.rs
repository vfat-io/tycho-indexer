#![allow(unused_variables)]
#![allow(unused_imports)]

use async_trait::async_trait;
use diesel::prelude::*;
use diesel_async::{AsyncPgConnection, RunQueryDsl};

use crate::{
    extractor::evm::ProtocolState,
    models::{Chain, ProtocolSystem},
    storage::{
        postgres::{
            orm,
            orm::{NewProtocolType, ProtocolType},
            PostgresGateway,
        },
        Address, BlockIdentifier, BlockOrTimestamp, ContractDelta, ProtocolGateway, StorableBlock,
        StorableContract, StorableProtocolType, StorableToken, StorableTransaction, StorageError,
        TxHash, Version,
    },
};

#[async_trait]
impl<B, TX, A, D, T, PT> ProtocolGateway for PostgresGateway<B, TX, A, D, T, PT>
where
    B: StorableBlock<orm::Block, orm::NewBlock, i64>,
    TX: StorableTransaction<orm::Transaction, orm::NewTransaction, i64>,
    D: ContractDelta + From<A>,
    A: StorableContract<orm::Contract, orm::NewContract, i64>,
    T: StorableToken<orm::Token, orm::NewToken, i64>,
    PT: StorableProtocolType<orm::ProtocolType, orm::NewProtocolType, i64>,
{
    type DB = AsyncPgConnection;
    type Token = T;
    type ProtocolState = ProtocolState;
    type ProtocolType = PT;

    // TODO: uncomment to implement in ENG 2049
    // async fn get_components(
    //     &self,
    //     chain: &Chain,
    //     system: Option<ProtocolSystem>,
    //     ids: Option<&[&str]>,
    // ) -> Result<Vec<ProtocolComponent>, StorageError> {
    //     todo!()
    // }

    // TODO: uncomment to implement in ENG 2049
    // async fn upsert_components(&self, new: &[&ProtocolComponent]) -> Result<(), StorageError> {
    //     todo!()
    // }

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
}

#[cfg(test)]
mod test {
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};

    use diesel_async::AsyncConnection;
    use serde_json::json;

    use crate::{
        extractor::{
            evm,
            evm::{FinancialType, ImplementationType},
        },
        storage::postgres::{
            orm,
            orm::{FinancialProtocolType, ProtocolImplementationType},
            schema, PostgresGateway,
        },
    };

    use super::*;

    type EVMGateway = PostgresGateway<
        evm::Block,
        evm::Transaction,
        evm::Account,
        evm::AccountUpdate,
        evm::ERC20Token,
        evm::ProtocolType,
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
    async fn test_add_protocol_type() {
        let mut conn = setup_db().await;
        let gw = EVMGateway::from_connection(&mut conn).await;

        let d = NaiveDate::from_ymd_opt(2015, 6, 3).unwrap();
        let t = NaiveTime::from_hms_milli_opt(12, 34, 56, 789).unwrap();
        let dt = NaiveDateTime::new(d, t);

        let protocol_type = evm::ProtocolType {
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
        assert_eq!(inserted_data.financial_type, FinancialProtocolType::Debt);
        assert_eq!(inserted_data.attribute_schema, Some(json!({"attribute": "schema"})));
        assert_eq!(inserted_data.implementation, ProtocolImplementationType::Custom);

        let updated_protocol_type = evm::ProtocolType {
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
        assert_eq!(newly_inserted_data[0].financial_type, FinancialProtocolType::Leverage);
        assert_eq!(
            newly_inserted_data[0].attribute_schema,
            Some(json!({"attribute": "another_schema"}))
        );
        assert_eq!(newly_inserted_data[0].implementation, ProtocolImplementationType::Vm);
    }
}
