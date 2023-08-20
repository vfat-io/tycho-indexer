use std::collections::HashMap;
use std::error::Error;
use std::marker::PhantomData;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use diesel::prelude::*;
use diesel_async::pooled_connection::bb8::{Pool, PooledConnection};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use ethers::types::{H160, H256};
use tokio::sync::RwLock;

use super::{BlockIdentifier, ChainGateway, StorableBlock, StorableTransaction};
use crate::extractor::evm;
use crate::models::Chain;
use crate::storage::schema;
use crate::storage::{orm, StorageError};

struct ChainIdCache {
    map_chain_id: HashMap<Chain, i64>,
    map_id_chain: HashMap<i64, Chain>,
}

impl ChainIdCache {
    fn get_id(&self, chain: Chain) -> i64 {
        *self
            .map_chain_id
            .get(&chain)
            .unwrap_or_else(|| panic!("Unexpected cache miss for chain {}", chain.to_string()))
    }

    fn get_chain(&self, id: i64) -> Chain {
        *self.map_id_chain.get(&id).unwrap_or_else(|| {
            panic!(
                "Unexpected cache miss for id {}, {:?}",
                id, self.map_id_chain
            )
        })
    }

    async fn init(&mut self, conn: &mut AsyncPgConnection) {
        use super::schema::chain::dsl::*;
        let results: Vec<(i64, String)> = chain
            .select((id, name))
            .load(conn)
            .await
            .expect("Failed to load chain ids!");
        for (id_, name_) in results {
            let chain_ = Chain::from(name_);
            self.map_chain_id.insert(chain_, id_);
            self.map_id_chain.insert(id_, chain_);
        }
    }
}

impl From<diesel::result::Error> for StorageError {
    fn from(value: diesel::result::Error) -> Self {
        // Only rollback errors should arrive here
        // we never expect these.
        StorageError::Unexpected(format!("DieselRollbackError: {}", value))
    }
}
impl StorageError {
    fn from_diesel(
        err: diesel::result::Error,
        entity: &str,
        id: &str,
        fetch_args: Option<String>,
    ) -> StorageError {
        let err_string = err.to_string();
        match err {
            diesel::result::Error::DatabaseError(
                diesel::result::DatabaseErrorKind::UniqueViolation,
                details,
            ) => {
                if let Some(col) = details.column_name() {
                    if col == "id" {
                        return StorageError::DuplicateEntry(entity.to_owned(), id.to_owned());
                    }
                }
                StorageError::Unexpected(err_string)
            }
            diesel::result::Error::NotFound => {
                if let Some(related_entitiy) = fetch_args {
                    return StorageError::NoRelatedEntity(
                        entity.to_owned(),
                        id.to_owned(),
                        related_entitiy,
                    );
                }
                StorageError::NotFound(entity.to_owned(), id.to_owned())
            }
            _ => StorageError::Unexpected(err_string),
        }
    }
}

pub struct PostgresGateway<B, TX> {
    pool: Pool<AsyncPgConnection>,
    chain_id_cache: Arc<RwLock<ChainIdCache>>,
    _phantom_block: PhantomData<B>,
    _phantom_tx: PhantomData<TX>,
}

impl<B, TX> PostgresGateway<B, TX>
where
    B: StorableBlock<orm::Block, orm::NewBlock> + Send + Sync + 'static,
    TX: StorableTransaction<orm::Transaction, orm::NewTransaction, Vec<u8>, i64>
        + Send
        + Sync
        + 'static,
{
    pub async fn new(connection_string: &str) -> Self {
        let config =
            AsyncDieselConnectionManager::<diesel_async::AsyncPgConnection>::new(connection_string);
        let pool = Pool::builder()
            .build(config)
            .await
            .expect("Failed to build connection pool!");
        let cache = ChainIdCache {
            map_chain_id: HashMap::new(),
            map_id_chain: HashMap::new(),
        };

        let gw = Self {
            pool,
            chain_id_cache: Arc::new(RwLock::new(cache)),
            _phantom_block: PhantomData,
            _phantom_tx: PhantomData,
        };
        gw.init_chain_id_cache(None).await;
        gw
    }

    #[cfg(test)]
    async fn with_mocked_connection(
        connection_string: &str,
        connection: &mut AsyncPgConnection,
    ) -> Self {
        let gw = Self::new(connection_string).await;
        // init chain id cache (again) using a mocked connection
        gw.init_chain_id_cache(Some(connection)).await;
        gw
    }

    async fn init_chain_id_cache(&self, conn: Option<&mut AsyncPgConnection>) {
        if let Some(conn) = conn {
            self.chain_id_cache.write().await.init(conn).await;
        } else {
            let mut conn = self.get_connection().await;
            self.chain_id_cache.write().await.init(&mut conn).await;
        };
    }

    async fn get_chain_id(&self, chain: Chain) -> i64 {
        self.chain_id_cache.read().await.get_id(chain)
    }

    async fn get_chain(&self, id: i64) -> Chain {
        self.chain_id_cache.read().await.get_chain(id)
    }

    async fn get_connection(&self) -> PooledConnection<AsyncPgConnection> {
        self.pool
            .get()
            .await
            .expect("Failed to get connection from pool!")
    }

    async fn insert_one_block(
        &self,
        new: B,
        conn: &mut AsyncPgConnection,
    ) -> Result<(), StorageError> {
        use super::schema::block::dsl::*;
        let block_chain_id = self.get_chain_id(new.chain()).await;
        let new_block = new.to_storage(block_chain_id);
        conn.transaction(|conn| {
            async move {
                diesel::insert_into(block)
                    .values(&new_block)
                    .execute(conn)
                    .await
                    .map_err(|err| {
                        StorageError::from_diesel(err, "Block", &hex::encode(new_block.hash), None)
                    })
            }
            .scope_boxed()
        })
        .await?;
        Ok(())
    }

    async fn fetch_one_block(
        &self,
        block_id: BlockIdentifier,
        conn: &mut AsyncPgConnection,
    ) -> Result<B, StorageError> {
        conn.transaction::<_, StorageError, _>(|conn| {
            async move {
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
                .map_err(|err| {
                    StorageError::from_diesel(err, "Block", &block_id.to_string(), None)
                })?;
                let chain = self.get_chain(orm_block.chain_id).await;
                Ok(B::from_storage(orm_block, chain))
            }
            .scope_boxed()
        })
        .await
    }

    async fn insert_one_tx(
        &self,
        new: TX,
        conn: &mut AsyncPgConnection,
    ) -> Result<(), StorageError> {
        use super::schema::transaction::dsl::*;
        conn.transaction::<_, StorageError, _>(|conn| {
            async move {
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

                diesel::insert_into(transaction)
                    .values(&orm_new)
                    .execute(conn)
                    .await
                    .map_err(|err| {
                        StorageError::from_diesel(
                            err,
                            "Transaction",
                            &hex::encode(orm_new.hash),
                            None,
                        )
                    })
            }
            .scope_boxed()
        })
        .await?;
        Ok(())
    }

    async fn fetch_one_tx(
        &self,
        hash: &[u8],
        conn: &mut AsyncPgConnection,
    ) -> Result<TX, StorageError> {
        let hash = Vec::from(hash);
        conn.transaction(|conn| {
            async move {
                schema::transaction::table
                    .inner_join(schema::block::table)
                    .filter(schema::transaction::hash.eq(&hash))
                    .select((orm::Transaction::as_select(), orm::Block::as_select()))
                    .first::<(orm::Transaction, orm::Block)>(conn)
                    .await
                    .map(|(orm_tx, block)| TX::from_storage(orm_tx, block.hash))
                    .map_err(|err| {
                        StorageError::from_diesel(err, "Transaction", &hex::encode(&hash), None)
                    })
            }
            .scope_boxed()
        })
        .await
    }
}

#[async_trait]
impl<B, TX> ChainGateway for PostgresGateway<B, TX>
where
    B: StorableBlock<orm::Block, orm::NewBlock> + Send + Sync + 'static,
    TX: StorableTransaction<orm::Transaction, orm::NewTransaction, Vec<u8>, i64>
        + Send
        + Sync
        + 'static,
{
    type Block = B;
    type Transaction = TX;

    async fn add_block(&self, new: Self::Block) -> Result<(), StorageError> {
        let mut conn = self.get_connection().await;
        self.insert_one_block(new, &mut conn).await
    }

    async fn get_block(&self, block_id: BlockIdentifier) -> Result<Self::Block, StorageError> {
        let mut conn = self.get_connection().await;
        self.fetch_one_block(block_id, &mut conn).await
    }

    async fn add_tx(&self, new: Self::Transaction) -> Result<(), StorageError> {
        let mut conn = self.get_connection().await;

        self.insert_one_tx(new, &mut conn).await
    }

    async fn get_tx(&self, hash: &[u8]) -> Result<Self::Transaction, StorageError> {
        let mut conn = self.get_connection().await;
        self.fetch_one_tx(hash, &mut conn).await
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
        let gw = PostgresGateway::<evm::Block, evm::Transaction>::with_mocked_connection(
            "postgres://postgres:mypassword@localhost:5432/tycho_indexer_0",
            &mut conn,
        )
        .await;
        let exp = block("0xb495a1d7e6663152ae92708da4843337b958146015a2802f4193a410044698c9");
        let block_id = BlockIdentifier::Number((Chain::Ethereum, 2));

        let block = gw.fetch_one_block(block_id, &mut conn).await.unwrap();

        assert_eq!(block, exp);
    }

    #[tokio::test]
    async fn test_add_block() {
        let mut conn = setup_db().await;
        let gw = PostgresGateway::<evm::Block, evm::Transaction>::with_mocked_connection(
            "postgres://postgres:mypassword@localhost:5432/tycho_indexer_0",
            &mut conn,
        )
        .await;
        let block = block("0xbadbabe000000000000000000000000000000000000000000000000000000000");

        gw.insert_one_block(block, &mut conn).await.unwrap();
        let retrieved_block = gw
            .fetch_one_block(
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
        let gw = PostgresGateway::<evm::Block, evm::Transaction>::with_mocked_connection(
            "postgres://postgres:mypassword@localhost:5432/tycho_indexer_0",
            &mut conn,
        )
        .await;
        let exp = transaction("0xbb7e16d797a9e2fbc537e30f91ed3d27a254dd9578aa4c3af3e5f0d3e8130945");

        let tx = gw
            .fetch_one_tx(exp.hash.as_bytes(), &mut conn)
            .await
            .unwrap();

        assert_eq!(tx, exp);
    }

    #[tokio::test]
    async fn test_add_tx() {
        let mut conn = setup_db().await;
        let gw = PostgresGateway::<evm::Block, evm::Transaction>::with_mocked_connection(
            "postgres://postgres:mypassword@localhost:5432/tycho_indexer_0",
            &mut conn,
        )
        .await;
        let mut tx =
            transaction("0xbadbabe000000000000000000000000000000000000000000000000000000000");
        tx.block_hash =
            H256::from_str("0xb495a1d7e6663152ae92708da4843337b958146015a2802f4193a410044698c9")
                .unwrap();

        gw.insert_one_tx(tx, &mut conn).await.unwrap();
        let retrieved_tx = gw
            .fetch_one_tx(tx.hash.as_bytes(), &mut conn)
            .await
            .unwrap();

        assert_eq!(tx, retrieved_tx);
    }
}
