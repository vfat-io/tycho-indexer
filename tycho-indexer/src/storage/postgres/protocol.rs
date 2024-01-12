#![allow(unused_variables)]

use async_trait::async_trait;

use diesel::prelude::*;
use diesel_async::{AsyncPgConnection, RunQueryDsl};

use crate::{
    extractor::evm::ProtocolState,
    models::{Chain, ProtocolSystem},
    storage::{
        postgres::{
            orm,
            orm::{NewProtocolSystemType, ProtocolSystemType, ProtocolSystemTypeEnum},
            PostgresGateway,
        },
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

    async fn _get_or_create_protocol_system_id(
        &self,
        new: ProtocolSystem,
        conn: &mut Self::DB,
    ) -> Result<i64, StorageError> {
        use super::schema::protocol_system_type::dsl::*;
        let new_system = ProtocolSystemTypeEnum::from(new);

        let existing_entry = protocol_system_type
            .filter(protocol_enum.eq(new_system.clone()))
            .first::<ProtocolSystemType>(conn)
            .await;

        if let Ok(entry) = existing_entry {
            return Ok(entry.id);
        } else {
            let new_entry = NewProtocolSystemType { protocol_enum: new_system };

            let inserted_protocol_system = diesel::insert_into(protocol_system_type)
                .values(&new_entry)
                .get_result::<ProtocolSystemType>(conn)
                .await
                .map_err(|err| {
                    StorageError::from_diesel(err, "ProtocolSystemEnum", &new.to_string(), None)
                })?;
            Ok(inserted_protocol_system.id)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::extractor::evm;
    use diesel_async::AsyncConnection;

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
}
