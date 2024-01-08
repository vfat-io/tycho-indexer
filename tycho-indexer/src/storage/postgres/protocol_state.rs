#![allow(unused_variables)]

use async_trait::async_trait;
use diesel::IntoSql;
use diesel_async::{AsyncPgConnection, RunQueryDsl};

use crate::{
    extractor::evm::ProtocolState,
    models::{Chain, ProtocolSystem},
    storage::{
        postgres::{orm, orm::NewProtocolSystem, schema::block::dsl::block, PostgresGateway},
        Address, BlockIdentifier, BlockOrTimestamp, ChainGateway, ContractDelta, ProtocolGateway,
        StorableBlock, StorableContract, StorableToken, StorableTransaction, StorageError, TxHash,
        Version,
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

    async fn _get_or_create_protocol_system_id(&self, new: ProtocolSystem, conn: &mut Self::DB) {
        use super::schema::protocol_system::dsl::*;

        let new_protocol_system = NewProtocolSystem { name: new.to_string() };
        let a = diesel::insert_into(protocol_system)
            .values(&new_protocol_system)
            .on_conflict_do_nothing()
            .execute(conn)
            .await
            .map_err(|err| {
                StorageError::from_diesel(err, "ProtocolSystem", &new.to_string(), None)
            });
        let b = 1;
        let c = protocol_system
            .load::<ProtocolSystem>(&conn)
            .expect("Error loading ProtocolSystem");
        let b = 1;
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use diesel_async::AsyncConnection;
    use ethers::types::{H160, H256};

    use crate::{extractor::evm, models::Chain, storage::postgres::db_fixtures};

    use super::*;

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
            .await;

        // assert_eq!(protocol_system_id, 1);
    }
}
