#![allow(unused_variables)]

use std::collections::HashMap;

use async_trait::async_trait;
use diesel::prelude::*;
use diesel_async::{AsyncPgConnection, RunQueryDsl};

use crate::{
    extractor::evm::ProtocolState,
    hex_bytes::Bytes,
    models::{Chain, ProtocolSystem},
    storage::{
        postgres::{orm, schema, PostgresGateway},
        Address, BlockIdentifier, BlockOrTimestamp, ContractDelta, ProtocolGateway, StorableBlock,
        StorableContract, StorableToken, StorableTransaction, StorageError, TxHash, Version,
    },
};

// Gateway helper functions:
async fn decode_protocol_states(
    result: Result<Vec<orm::ProtocolState>, diesel::result::Error>,
    context: &str,
    conn: &mut AsyncPgConnection,
) -> Result<Vec<ProtocolState>, StorageError> {
    match result {
        Ok(states) => {
            let mut protocol_states = Vec::new();
            for state in states {
                let component_id = schema::protocol_component::table
                    .filter(schema::protocol_component::id.eq(state.protocol_component_id))
                    .select(schema::protocol_component::external_id)
                    .first::<String>(conn)
                    .await
                    .expect("Failed to find matching protocol component in db");
                let tx_hash = schema::transaction::table
                    .filter(schema::transaction::id.eq(state.modify_tx))
                    .select(schema::transaction::hash)
                    .first::<Bytes>(conn)
                    .await
                    .expect("Failed to find matching protocol component in db");
                let protocol_state = ProtocolState {
                    component_id,
                    updated_attributes: match state.state {
                        Some(val) => serde_json::from_value(val).map_err(|err| {
                            StorageError::DecodeError(format!(
                                "Failed to deserialize state attribute: {}",
                                err
                            ))
                        })?,
                        None => HashMap::new(),
                    },
                    deleted_attributes: HashMap::new(),
                    modify_tx: tx_hash.into(),
                };
                protocol_states.push(protocol_state);
            }
            Ok(protocol_states)
        }
        Err(err) => Err(StorageError::from_diesel(err, "ProtocolStates", context, None)),
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

    // Gets all protocol states from the db. A separate protocol state is returned for every state
    // update.
    async fn get_states(
        &self,
        chain: &Chain,
        at: Option<Version>,
        system: Option<ProtocolSystem>,
        ids: Option<&[&str]>,
        conn: &mut Self::DB,
    ) -> Result<Vec<Self::ProtocolState>, StorageError> {
        let chain_db_id = self.get_chain_id(chain);

        match (ids, system) {
            (Some(ids), _) => {
                decode_protocol_states(
                    orm::ProtocolState::by_id(ids, chain_db_id, conn).await,
                    ids.join(",").as_str(),
                    conn,
                )
                .await
            }
            (_, Some(system)) => {
                decode_protocol_states(
                    orm::ProtocolState::by_protocol_system(&system, chain_db_id, conn).await,
                    system.to_string().as_str(),
                    conn,
                )
                .await
            }
            _ => {
                decode_protocol_states(
                    orm::ProtocolState::by_chain(chain_db_id, conn).await,
                    chain.to_string().as_str(),
                    conn,
                )
                .await
            }
        }
    }

    async fn update_state(
        &self,
        chain: Chain,
        new: &[(TxHash, ProtocolState)],
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
    //! Tests for PostgresGateway's ProtocolGateway methods
    //!
    //! The tests below test the functionality using the concrete EVM types.

    use crate::extractor::evm;
    use crate::storage::postgres::orm::{FinancialProtocolType, ProtocolImplementationType};
    use crate::storage::postgres::{db_fixtures, PostgresGateway};
    use diesel_async::AsyncConnection;
    use rstest::rstest;

    use super::*;

    type EVMGateway = PostgresGateway<
        evm::Block,
        evm::Transaction,
        evm::Account,
        evm::AccountUpdate,
        evm::ERC20Token,
    >;

    async fn setup_db() -> AsyncPgConnection {
        let db_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
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
        let protocol_system_id = db_fixtures::insert_protocol_system(conn, "Ambient").await;
        let protocol_type_id = db_fixtures::insert_protocol_type(
            conn,
            "Pool",
            FinancialProtocolType::Swap,
            ProtocolImplementationType::Custom,
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
        db_fixtures::insert_protocol_state(conn, protocol_component_id, txn[0]).await;
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_states() {
        let mut conn = setup_db().await;
        setup_data(&mut conn).await;
        let expected = vec![ProtocolState::new(
            "state1".to_owned(),
            HashMap::new(),
            "0xbb7e16d797a9e2fbc537e30f91ed3d27a254dd9578aa4c3af3e5f0d3e8130945"
                .parse()
                .unwrap(),
        )];

        let gateway = EVMGateway::from_connection(&mut conn).await;

        let result = gateway
            .get_states(&Chain::Ethereum, None, None, None, &mut conn)
            .await
            .unwrap();

        assert_eq!(result, expected)
    }
}
