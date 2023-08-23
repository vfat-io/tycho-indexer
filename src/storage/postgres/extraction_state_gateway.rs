use super::{
    Chain, ExtractionState, ExtractionStateGateway, PostgresGateway, StorableBlock,
    StorableTransaction, StorageError,
};
use crate::storage::orm;
use crate::storage::schema;
use async_trait::async_trait;
use diesel::prelude::*;
use diesel::ExpressionMethods;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};

#[async_trait]
impl<B, TX> ExtractionStateGateway for PostgresGateway<B, TX>
where
    B: StorableBlock<orm::Block, orm::NewBlock> + Send + Sync + 'static,
    TX: StorableTransaction<orm::Transaction, orm::NewTransaction, Vec<u8>, i64>
        + Send
        + Sync
        + 'static,
{
    type DB = AsyncPgConnection;

    async fn get_state(
        &self,
        name: &str,
        chain: Chain,
        conn: &mut Self::DB,
    ) -> Result<Option<ExtractionState>, StorageError> {
        let block_chain_id = self.get_chain_id(chain);

        match orm::ExtractionState::get_for_extractor(name, block_chain_id, conn).await {
            Ok(Some(orm_state)) => {
                let state = ExtractionState {
                    name: orm_state.name,
                    chain,
                    attributes: orm_state.attributes.into(),
                    cursor: orm_state.cursor.unwrap_or_default(),
                };
                Ok(Some(state))
            }
            Ok(None) => Ok(None), // No matching entry in the DB
            Err(err) => Err(StorageError::from_diesel(
                err,
                "ExtractionState",
                name,
                None,
            )),
        }
    }

    async fn save_state(
        &self,
        state: &ExtractionState,
        conn: &mut Self::DB,
    ) -> Result<(), StorageError> {
        let block_chain_id = self.get_chain_id(state.chain);
        match orm::ExtractionState::get_for_extractor(&state.name, block_chain_id, conn).await {
            Ok(Some(_)) => {
                let update_form = orm::ExtractionStateForm {
                    attributes: Some(&state.attributes),
                    cursor: Some(&state.cursor),
                    modified_ts: chrono::Utc::now().naive_utc(),
                };
                diesel::update(schema::extraction_state::dsl::extraction_state)
                    .filter(schema::extraction_state::name.eq(&state.name))
                    .filter(schema::extraction_state::chain_id.eq(block_chain_id))
                    .set(&update_form)
                    .execute(conn)
                    .await
                    .map_err(|err| {
                        StorageError::from_diesel(err, "ExtractionState", &state.name, None)
                    })?;
            }
            Ok(None) => {
                // No matching entry in the DB
                let orm_state = orm::NewExtractionState {
                    name: &state.name,
                    version: "0.1.0",
                    chain_id: block_chain_id,
                    attributes: Some(&state.attributes),
                    cursor: Some(&state.cursor),
                    modified_ts: chrono::Utc::now().naive_utc(),
                };
                diesel::insert_into(schema::extraction_state::dsl::extraction_state)
                    .values(&orm_state)
                    .execute(conn)
                    .await
                    .map_err(|err| {
                        StorageError::from_diesel(err, "ExtractionState", &state.name, None)
                    })?;
            }
            Err(err) => {
                return Err(StorageError::from_diesel(
                    err,
                    "ExtractionState",
                    &state.name,
                    None,
                ))
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::extractor::evm;
    async fn setup_db() -> AsyncPgConnection {
        // Creates a DB connecton
        // Creates a chain entry in the DB
        // Creates a ExtractionState entry in the DB
        let db_url = std::env::var("DATABASE_URL").unwrap();
        let mut conn = AsyncPgConnection::establish(&db_url).await.unwrap();
        conn.begin_test_transaction().await.unwrap();
        let chain_id: i64 = diesel::insert_into(schema::chain::table)
            .values(schema::chain::name.eq("ethereum"))
            .returning(schema::chain::id)
            .get_result(&mut conn)
            .await
            .unwrap();
        let extractor_name = "setup_extractor";

        let gateway = get_dgw(&mut conn).await;
        let state = ExtractionState {
            name: extractor_name.to_owned(),
            chain: Chain::Ethereum,
            attributes: serde_json::json!({"test": "test"}),
            cursor: Vec::new(),
        };

        gateway.save_state(&state, &mut conn).await.unwrap();
        conn
    }

    async fn get_dgw(
        conn: &mut AsyncPgConnection,
    ) -> PostgresGateway<evm::Block, evm::Transaction> {
        PostgresGateway::<evm::Block, evm::Transaction>::from_connection(conn).await
    }

    #[tokio::test]
    async fn test_save_new_state() {
        let mut conn = setup_db().await;
        let extractor_name = "test_extractor";

        let gateway = get_dgw(&mut conn).await;
        let state = ExtractionState {
            name: extractor_name.to_owned(),
            chain: Chain::Ethereum,
            attributes: serde_json::json!({"test": "test"}),
            cursor: "10".as_bytes().to_vec(),
        };

        // Save the state using the gateway
        gateway.save_state(&state, &mut conn).await.unwrap();

        let query_res: orm::ExtractionState = schema::extraction_state::table
            .filter(schema::extraction_state::name.eq(extractor_name))
            .limit(1)
            .select(orm::ExtractionState::as_select())
            .load(&mut conn)
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap();

        assert_eq!(&query_res.name, extractor_name);
        assert_eq!(&query_res.version, "0.1.0");
        assert_eq!(&query_res.cursor.unwrap(), "10".as_bytes());
    }

    #[tokio::test]
    async fn test_update_state() {
        // Adds a ExtractionState to the DB
        let mut conn = setup_db().await;
        let gateway = get_dgw(&mut conn).await;
        let extractor_name = "updating_extractor";

        // Update the state using the gateway
        let mut state = ExtractionState {
            name: extractor_name.to_owned(),
            chain: Chain::Ethereum,
            attributes: serde_json::json!({"test": "test"}),
            cursor: "10".as_bytes().to_vec(),
        };
        gateway.save_state(&state, &mut conn).await.unwrap();
        assert_eq!(
            gateway
                .get_state(extractor_name, Chain::Ethereum, &mut conn)
                .await
                .unwrap()
                .unwrap()
                .cursor,
            "10".as_bytes().to_vec()
        );

        state = ExtractionState {
            name: extractor_name.to_owned(),
            chain: Chain::Ethereum,
            attributes: serde_json::json!({"test": "test"}),
            cursor: "20".as_bytes().to_vec(),
        };
        gateway.save_state(&state, &mut conn).await.unwrap();
        assert_eq!(
            gateway
                .get_state(extractor_name, Chain::Ethereum, &mut conn)
                .await
                .unwrap()
                .unwrap()
                .cursor,
            "20".as_bytes().to_vec()
        );
    }

    #[tokio::test]
    async fn test_get_state() {
        // Adds a ExtractionState to the DB
        let mut conn = setup_db().await;
        let gateway = get_dgw(&mut conn).await;
        let extractor_name = "setup_extractor";

        // Get the state using the gateway
        let state = gateway
            .get_state(extractor_name, Chain::Ethereum, &mut conn)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(state.name, extractor_name);
        assert_eq!(state.chain, Chain::Ethereum);
    }
}
