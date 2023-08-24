use async_trait::async_trait;
use diesel::prelude::*;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use ethers::types::{H160, H256};

use super::orm;
use super::schema;
use super::PostgresGateway;
use crate::extractor::evm;
use crate::models::Chain;
use crate::storage::{
    BlockIdentifier, ChainGateway, StorableBlock, StorableTransaction, StorageError,
};

#[async_trait]
impl<B, TX> ChainGateway for PostgresGateway<B, TX>
where
    B: StorableBlock<orm::Block, orm::NewBlock> + Send + Sync + 'static,
    TX: StorableTransaction<orm::Transaction, orm::NewTransaction, Vec<u8>, i64>
        + Send
        + Sync
        + 'static,
{
    type DB = AsyncPgConnection;
    type Block = B;
    type Transaction = TX;

    async fn add_block(&self, new: Self::Block, conn: &mut Self::DB) -> Result<(), StorageError> {
        use super::schema::block::dsl::*;
        let block_chain_id = self.get_chain_id(new.chain());
        let new_block = new.to_storage(block_chain_id);

        // assumes that block with the same hash will not appear with different values
        diesel::insert_into(block)
            .values(&new_block)
            .on_conflict_do_nothing()
            .execute(conn)
            .await
            .map_err(|err| {
                StorageError::from_diesel(err, "Block", &hex::encode(new_block.hash), None)
            })?;
        Ok(())
    }

    async fn get_block(
        &self,
        block_id: BlockIdentifier,
        conn: &mut Self::DB,
    ) -> Result<Self::Block, StorageError> {
        // taking a reference here is necessary, to not move block_id
        // so it can be used in the map_err closure later on. It would
        // be better if BlockIdentifier was copy though (complicates lifetimes).
        let orm_block = match &block_id {
            BlockIdentifier::Number((chain, number)) => {
                orm::Block::by_number(*chain, *number, conn).await
            }

            BlockIdentifier::Hash(block_hash) => {
                orm::Block::by_hash(block_hash.as_slice(), conn).await
            }
        }
        .map_err(|err| StorageError::from_diesel(err, "Block", &block_id.to_string(), None))?;
        let chain = self.get_chain(orm_block.chain_id);
        Ok(B::from_storage(orm_block, chain))
    }

    async fn add_tx(
        &self,
        new: Self::Transaction,
        conn: &mut Self::DB,
    ) -> Result<(), StorageError> {
        use super::schema::transaction::dsl::*;

        let block_hash = new.block_hash();
        let parent_block = schema::block::table
            .filter(schema::block::hash.eq(&block_hash))
            .select(schema::block::id)
            .first::<i64>(conn)
            .await
            .map_err(|err| {
                StorageError::from_diesel(
                    err,
                    "Transaction",
                    &hex::encode(block_hash.as_slice()),
                    Some("Block".to_owned()),
                )
            })?;

        let orm_new: orm::NewTransaction = new.to_storage(parent_block);

        // assumes that tx with the same hash will not appear with different values
        diesel::insert_into(transaction)
            .values(&orm_new)
            .on_conflict_do_nothing()
            .execute(conn)
            .await
            .map_err(|err| {
                StorageError::from_diesel(err, "Transaction", &hex::encode(orm_new.hash), None)
            })?;
        Ok(())
    }

    async fn get_tx(
        &self,
        hash: &[u8],
        conn: &mut Self::DB,
    ) -> Result<Self::Transaction, StorageError> {
        let hash = Vec::from(hash);
        schema::transaction::table
            .inner_join(schema::block::table)
            .filter(schema::transaction::hash.eq(&hash))
            .select((orm::Transaction::as_select(), orm::Block::as_select()))
            .first::<(orm::Transaction, orm::Block)>(conn)
            .await
            .map(|(orm_tx, block)| TX::from_storage(orm_tx, block.hash))
            .map_err(|err| StorageError::from_diesel(err, "Transaction", &hex::encode(&hash), None))
    }
}

impl StorableBlock<orm::Block, orm::NewBlock> for evm::Block {
    fn from_storage(val: orm::Block, chain: Chain) -> Self {
        evm::Block {
            number: val.number as u64,
            hash: H256::from_slice(val.hash.as_slice()),
            parent_hash: H256::from_slice(val.parent_hash.as_slice()),
            chain,
            ts: val.ts,
        }
    }

    fn to_storage(&self, chain_id: i64) -> orm::NewBlock {
        orm::NewBlock {
            hash: Vec::from(self.hash.as_bytes()),
            parent_hash: Vec::from(self.parent_hash.as_bytes()),
            chain_id,
            main: false,
            number: self.number as i64,
            ts: self.ts,
        }
    }

    fn chain(&self) -> Chain {
        self.chain
    }
}

impl StorableTransaction<orm::Transaction, orm::NewTransaction, Vec<u8>, i64> for evm::Transaction {
    fn from_storage(val: orm::Transaction, block_hash: Vec<u8>) -> Self {
        Self {
            hash: H256::from_slice(&val.hash),
            block_hash: H256::from_slice(&block_hash),
            from: H160::from_slice(&val.from),
            to: H160::from_slice(&val.to),
            index: val.index as u64,
        }
    }

    fn to_storage(&self, block_id: i64) -> orm::NewTransaction {
        orm::NewTransaction {
            hash: Vec::from(self.hash.as_bytes()),
            block_id,
            from: Vec::from(self.from.as_bytes()),
            to: Vec::from(self.to.as_bytes()),
            index: self.index as i64,
        }
    }

    fn block_hash(&self) -> Vec<u8> {
        Vec::from(self.block_hash.as_bytes())
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use chrono::NaiveDateTime;

    use super::*;

    async fn setup_db() -> AsyncPgConnection {
        let db_url = std::env::var("DATABASE_URL").unwrap();
        let mut conn = AsyncPgConnection::establish(&db_url).await.unwrap();
        conn.begin_test_transaction().await.unwrap();
        let chain_id: i64 = diesel::insert_into(schema::chain::table)
            .values(schema::chain::name.eq("ethereum"))
            .returning(schema::chain::id)
            .get_result(&mut conn)
            .await
            .unwrap();

        let block_records = vec![
            (
                schema::block::hash.eq(Vec::from(
                    H256::from_str(
                        "0x88e96d4537bea4d9c05d12549907b32561d3bf31f45aae734cdc119f13406cb6",
                    )
                    .unwrap()
                    .as_bytes(),
                )),
                schema::block::parent_hash.eq(Vec::from(
                    H256::from_str(
                        "0xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3",
                    )
                    .unwrap()
                    .as_bytes(),
                )),
                schema::block::number.eq(1),
                schema::block::ts.eq("2022-11-01T08:00:00"
                    .parse::<chrono::NaiveDateTime>()
                    .expect("timestamp")),
                schema::block::chain_id.eq(chain_id),
            ),
            (
                schema::block::hash.eq(Vec::from(
                    H256::from_str(
                        "0xb495a1d7e6663152ae92708da4843337b958146015a2802f4193a410044698c9",
                    )
                    .unwrap()
                    .as_bytes(),
                )),
                schema::block::parent_hash.eq(Vec::from(
                    H256::from_str(
                        "0x88e96d4537bea4d9c05d12549907b32561d3bf31f45aae734cdc119f13406cb6",
                    )
                    .unwrap()
                    .as_bytes(),
                )),
                schema::block::number.eq(2),
                schema::block::ts.eq("2022-11-01T09:00:00"
                    .parse::<chrono::NaiveDateTime>()
                    .unwrap()),
                schema::block::chain_id.eq(chain_id),
            ),
        ];
        let block_ids: Vec<i64> = diesel::insert_into(schema::block::table)
            .values(&block_records)
            .returning(schema::block::id)
            .get_results(&mut conn)
            .await
            .unwrap();

        let tx_data = vec![(
            schema::transaction::hash.eq(Vec::from(
                H256::from_str(
                    "0xbb7e16d797a9e2fbc537e30f91ed3d27a254dd9578aa4c3af3e5f0d3e8130945",
                )
                .unwrap()
                .as_bytes(),
            )),
            schema::transaction::from.eq(Vec::from(
                H160::from_str("0x4648451b5F87FF8F0F7D622bD40574bb97E25980")
                    .unwrap()
                    .as_bytes(),
            )),
            schema::transaction::to.eq(Vec::from(
                H160::from_str("0x6B175474E89094C44Da98b954EedeAC495271d0F")
                    .unwrap()
                    .as_bytes(),
            )),
            schema::transaction::index.eq(1),
            schema::transaction::block_id.eq(block_ids[0]),
        )];
        diesel::insert_into(schema::transaction::table)
            .values(&tx_data)
            .execute(&mut conn)
            .await
            .unwrap();
        conn
    }

    fn block(hash: &str) -> evm::Block {
        evm::Block {
            number: 2,
            hash: H256::from_str(hash).unwrap(),
            parent_hash: H256::from_str(
                "0x88e96d4537bea4d9c05d12549907b32561d3bf31f45aae734cdc119f13406cb6",
            )
            .unwrap(),
            chain: Chain::Ethereum,
            ts: NaiveDateTime::parse_from_str("2022-11-01T09:00:00", "%Y-%m-%dT%H:%M:%S").unwrap(),
        }
    }

    #[tokio::test]
    async fn test_get_block() {
        let mut conn = setup_db().await;
        let gw = PostgresGateway::<evm::Block, evm::Transaction>::from_connection(&mut conn).await;
        let exp = block("0xb495a1d7e6663152ae92708da4843337b958146015a2802f4193a410044698c9");
        let block_id = BlockIdentifier::Number((Chain::Ethereum, 2));

        let block = gw.get_block(block_id, &mut conn).await.unwrap();

        assert_eq!(block, exp);
    }

    #[tokio::test]
    async fn test_add_block() {
        let mut conn = setup_db().await;
        let gw = PostgresGateway::<evm::Block, evm::Transaction>::from_connection(&mut conn).await;
        let block = block("0xbadbabe000000000000000000000000000000000000000000000000000000000");

        gw.add_block(block, &mut conn).await.unwrap();
        let retrieved_block = gw
            .get_block(
                BlockIdentifier::Hash(Vec::from(block.hash.as_bytes())),
                &mut conn,
            )
            .await
            .unwrap();

        assert_eq!(retrieved_block, block);
    }

    #[tokio::test]
    async fn test_upsert_block() {
        let mut conn = setup_db().await;
        let gw = PostgresGateway::<evm::Block, evm::Transaction>::from_connection(&mut conn).await;
        let block = evm::Block {
            number: 1,
            hash: H256::from_str(
                "0x88e96d4537bea4d9c05d12549907b32561d3bf31f45aae734cdc119f13406cb6",
            )
            .unwrap(),
            parent_hash: H256::from_str(
                "0xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3",
            )
            .unwrap(),
            chain: Chain::Ethereum,
            ts: NaiveDateTime::parse_from_str("2022-11-01T08:00:00", "%Y-%m-%dT%H:%M:%S").unwrap(),
        };

        gw.add_block(block, &mut conn).await.unwrap();
        let retrieved_block = gw
            .get_block(
                BlockIdentifier::Hash(Vec::from(block.hash.as_bytes())),
                &mut conn,
            )
            .await
            .unwrap();

        assert_eq!(retrieved_block, block);
    }

    fn transaction(hash: &str) -> evm::Transaction {
        evm::Transaction {
            hash: H256::from_str(hash).expect("tx hash ok"),
            block_hash: H256::from_str(
                "0x88e96d4537bea4d9c05d12549907b32561d3bf31f45aae734cdc119f13406cb6",
            )
            .expect("block hash ok"),
            from: H160::from_str("0x4648451b5f87ff8f0f7d622bd40574bb97e25980").expect("from ok"),
            to: H160::from_str("0x6b175474e89094c44da98b954eedeac495271d0f").expect("to ok"),
            index: 1,
        }
    }

    #[tokio::test]
    async fn test_get_tx() {
        let mut conn = setup_db().await;
        let gw = PostgresGateway::<evm::Block, evm::Transaction>::from_connection(&mut conn).await;
        let exp = transaction("0xbb7e16d797a9e2fbc537e30f91ed3d27a254dd9578aa4c3af3e5f0d3e8130945");

        let tx = gw.get_tx(exp.hash.as_bytes(), &mut conn).await.unwrap();

        assert_eq!(tx, exp);
    }

    #[tokio::test]
    async fn test_add_tx() {
        let mut conn = setup_db().await;
        let gw = PostgresGateway::<evm::Block, evm::Transaction>::from_connection(&mut conn).await;
        let mut tx =
            transaction("0xbadbabe000000000000000000000000000000000000000000000000000000000");
        tx.block_hash =
            H256::from_str("0xb495a1d7e6663152ae92708da4843337b958146015a2802f4193a410044698c9")
                .unwrap();

        gw.add_tx(tx, &mut conn).await.unwrap();
        let retrieved_tx = gw.get_tx(tx.hash.as_bytes(), &mut conn).await.unwrap();

        assert_eq!(tx, retrieved_tx);
    }

    #[tokio::test]
    async fn test_upsert_tx() {
        let mut conn = setup_db().await;
        let gw = PostgresGateway::<evm::Block, evm::Transaction>::from_connection(&mut conn).await;
        let tx = evm::Transaction {
            hash: H256::from_str(
                "0xbb7e16d797a9e2fbc537e30f91ed3d27a254dd9578aa4c3af3e5f0d3e8130945",
            )
            .expect("tx hash ok"),
            block_hash: H256::from_str(
                "0x88e96d4537bea4d9c05d12549907b32561d3bf31f45aae734cdc119f13406cb6",
            )
            .expect("block hash ok"),
            from: H160::from_str("0x4648451b5F87FF8F0F7D622bD40574bb97E25980").expect("from ok"),
            to: H160::from_str("0x6B175474E89094C44Da98b954EedeAC495271d0F").expect("to ok"),
            index: 1,
        };

        gw.add_tx(tx, &mut conn).await.unwrap();
        let retrieved_tx = gw.get_tx(tx.hash.as_bytes(), &mut conn).await.unwrap();

        assert_eq!(tx, retrieved_tx);
    }
}
