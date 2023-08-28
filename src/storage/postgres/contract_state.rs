use async_trait::async_trait;
use ethers::types::U256;

use crate::{
    extractor::evm::AccountUpdate,
    storage::{
        BlockOrTimestamp, ContractId, ContractStateGateway, StorableBlock, StorableTransaction,
        Version,
    },
};

use super::*;

#[async_trait]
impl<B, TX> ContractStateGateway for PostgresGateway<B, TX>
where
    B: StorableBlock<orm::Block, orm::NewBlock> + Send + Sync + 'static,
    TX: StorableTransaction<orm::Transaction, orm::NewTransaction, Vec<u8>, i64>
        + Send
        + Sync
        + 'static,
{
    type DB = AsyncPgConnection;
    type ContractState = AccountUpdate;
    type Slot = U256;
    type Value = U256;

    async fn get_contract(
        &mut self,
        id: ContractId,
        at: Option<Version>,
    ) -> Result<Self::ContractState, StorageError> {
        Err(StorageError::NotFound(
            "ContractState".to_owned(),
            "id".to_owned(),
        ))
    }

    async fn add_contract(&mut self, new: Self::ContractState) -> Result<(), StorageError> {
        Ok(())
    }

    async fn delete_contract(
        &self,
        id: ContractId,
        at_tx: Option<&[u8]>,
    ) -> Result<(), StorageError> {
        Ok(())
    }

    async fn get_contract_slots(
        &self,
        id: ContractId,
        at: Option<Version>,
    ) -> Result<HashMap<Self::Slot, Self::Value>, StorageError> {
        Err(StorageError::NotFound(
            "ContractState".to_owned(),
            "id".to_owned(),
        ))
    }

    async fn upsert_slots(
        &self,
        id: ContractId,
        modify_tx: &[u8],
        slots: HashMap<Self::Slot, Self::Value>,
    ) -> Result<(), StorageError> {
        Ok(())
    }

    async fn get_slots_delta(
        &self,
        id: ContractId,
        start_version: Option<BlockOrTimestamp>,
        end_version: Option<BlockOrTimestamp>,
    ) -> Result<HashMap<Self::Slot, Self::Value>, StorageError> {
        Err(StorageError::NotFound(
            "ContractState".to_owned(),
            "id".to_owned(),
        ))
    }

    async fn revert_contract_state(
        &self,
        id: ContractId,
        to: BlockOrTimestamp,
    ) -> Result<(), StorageError> {
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
        // Creates a ExtractionState entry in the DB named "setup_extractor"
        let db_url = std::env::var("DATABASE_URL").unwrap();
        let mut conn = AsyncPgConnection::establish(&db_url).await.unwrap();
        conn.begin_test_transaction().await.unwrap();
        let chain_id: i64 = diesel::insert_into(schema::chain::table)
            .values(schema::chain::name.eq("ethereum"))
            .returning(schema::chain::id)
            .get_result(&mut conn)
            .await
            .unwrap();

        // TODO

        conn
    }

    #[tokio::test]
    async fn test_get_contract() {
        let expected = AccountUpdate::new(
            "setup_extractor".to_owned(),
            Chain::Ethereum,
            H160::zero(),
            HashMap::new(),
            Some(U256::zero()),
            None,
            None,
        );

        let mut conn = setup_db().await;
        let mut gateway =
            PostgresGateway::<evm::Block, evm::Transaction>::from_connection(&mut conn).await;
        let id = ContractId(Chain::Ethereum, vec![]);
        let actual = gateway.get_contract(id, None).await.unwrap();

        assert_eq!(expected, actual);
    }

    #[tokio::test]
    async fn test_add_contract() {
        let mut conn = setup_db().await;
        let mut gateway =
            PostgresGateway::<evm::Block, evm::Transaction>::from_connection(&mut conn).await;
        let new = AccountUpdate::new(
            "setup_extractor".to_owned(),
            Chain::Ethereum,
            H160::random(),
            HashMap::new(),
            Some(U256::zero()),
            None,
            None,
        );

        let res = gateway
            .add_contract(new)
            .await
            .expect("Succesful insertion");
    }

    #[tokio::test]
    async fn test_delete_contract() {}

    #[tokio::test]
    async fn test_upsert_contract() {}
}
