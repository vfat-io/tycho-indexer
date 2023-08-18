use std::collections::HashMap;
use std::error::Error;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::RwLock;

use anyhow::anyhow;
use async_trait::async_trait;
use diesel::prelude::*;
use diesel::r2d2::ConnectionManager;
use diesel::r2d2::Pool;
use diesel::r2d2::PooledConnection;
use ethers::types::H160;
use ethers::types::H256;

use super::BlockIdentifier;
use super::ChainGateway;
use super::StorableBlock;
use super::StorableTransaction;
use crate::extractor::evm;
use crate::models::Chain;
use crate::storage::orm;
use crate::storage::schema;

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
        *self
            .map_id_chain
            .get(&id)
            .unwrap_or_else(|| panic!("Unexpected cache miss for id {}", id))
    }

    fn init(&mut self, conn: &mut PgConnection) {
        use super::schema::chain::dsl::*;
        let results: Vec<(i64, String)> = chain
            .select((id, name))
            .load(conn)
            .expect("Failed to load chain ids!");
        for (id_, name_) in results {
            let chain_ = Chain::from(name_);
            self.map_chain_id.insert(chain_, id_);
            self.map_id_chain.insert(id_, chain_);
        }
    }

    fn initialised(&self) -> bool {
        !self.map_chain_id.is_empty()
    }
}

pub struct PostgresGateway<B, TX> {
    pool: Pool<ConnectionManager<PgConnection>>,
    chain_id_cache: Arc<RwLock<ChainIdCache>>,
    _phantom_block: PhantomData<B>,
    _phantom_tx: PhantomData<TX>,
}

impl<B, TX> PostgresGateway<B, TX>
where
    B: StorableBlock<orm::Block, orm::NewBlock> + Send + Sync,
    TX: StorableTransaction<orm::Transaction, orm::NewTransaction, Vec<u8>, i64> + Send + Sync,
{
    pub fn new(connection_string: &str) -> Self {
        let manager = ConnectionManager::<PgConnection>::new(connection_string);
        let pool = Pool::builder()
            .build(manager)
            .expect("Failed to create pool.");

        Self {
            pool,
            chain_id_cache: Arc::new(RwLock::new(ChainIdCache {
                map_chain_id: HashMap::new(),
                map_id_chain: HashMap::new(),
            })),
            _phantom_block: PhantomData,
            _phantom_tx: PhantomData,
        }
    }

    fn get_chain_id(&self, chain: Chain) -> i64 {
        if let Ok(cache_read) = self.chain_id_cache.read() {
            if cache_read.initialised() {
                return cache_read.get_id(chain);
            }
        }
        let pool = self.pool.clone();
        let conn = &mut pool.get().unwrap();
        // If this point is reached, either read failed or cache was not initialised.
        let mut cache_write = self
            .chain_id_cache
            .write()
            .expect("Failed to acquire ChainIdCache write reference!");
        cache_write.init(conn);
        cache_write.get_id(chain)
    }

    fn get_chain(&self, id: i64) -> Chain {
        if let Ok(cache_read) = self.chain_id_cache.read() {
            if cache_read.initialised() {
                return cache_read.get_chain(id);
            }
        }
        let pool = self.pool.clone();
        let conn = &mut pool.get().unwrap();
        // If this point is reached, either read failed or cache was not initialised.
        let mut cache_write = self
            .chain_id_cache
            .write()
            .expect("Failed to acquire ChainIdCache write reference!");
        cache_write.init(conn);
        cache_write.get_chain(id)
    }

    fn get_connection(&self) -> PooledConnection<ConnectionManager<PgConnection>> {
        let pool = self.pool.clone();
        pool.get().expect("Failed to get connection from pool!")
    }

    async fn insert_one_block(
        &self,
        new: B,
        conn: &mut PgConnection,
    ) -> Result<(), Box<dyn Error>> {
        use super::schema::block::dsl::*;
        let new_block = new.to_storage(self.get_chain_id(new.chain()));
        conn.transaction(|conn| diesel::insert_into(block).values(&new_block).execute(conn))
            .map_err(|err| -> Box<dyn std::error::Error> { anyhow!(err.to_string()).into() })?;
        Ok(())
    }

    async fn fetch_one_block(
        &self,
        block_id: BlockIdentifier<'_>,
        conn: &mut PgConnection,
    ) -> Result<B, Box<dyn Error>> {
        conn.transaction(|conn| {
            let query_result = match block_id {
                BlockIdentifier::Number((chain, number)) => {
                    orm::Block::by_number(chain, number, conn)
                }

                BlockIdentifier::Hash(block_hash) => orm::Block::by_hash(block_hash, conn),
            };
            query_result.map(|orm_block| {
                let chain = self.get_chain(orm_block.chain_id);
                B::from_storage(orm_block, chain)
            })
        })
        .map_err(|err| anyhow!(err).into())
    }

    async fn insert_one_tx(&self, new: TX, conn: &mut PgConnection) -> Result<(), Box<dyn Error>> {
        use super::schema::transaction::dsl::*;
        conn.transaction(|conn| {
            let block_hash = new.block_hash();
            let parent_block = schema::block::table
                .filter(schema::block::hash.eq(&block_hash))
                .select(schema::block::id)
                .first::<i64>(conn)
                .map_err(|err| -> Box<dyn Error> { anyhow!(err.to_string()).into() })?;

            let orm_new: orm::NewTransaction = new.to_storage(parent_block);

            diesel::insert_into(transaction)
                .values(&orm_new)
                .execute(conn)
                .map_err(|err| -> Box<dyn Error> { anyhow!(err.to_string()).into() })
        })?;
        Ok(())
    }

    async fn fetch_one_tx(
        &self,
        hash: &[u8],
        conn: &mut PgConnection,
    ) -> Result<TX, Box<dyn Error>> {
        conn.transaction(|conn| {
            schema::transaction::table
                .inner_join(schema::block::table)
                .filter(schema::transaction::hash.eq(hash))
                .select((orm::Transaction::as_select(), orm::Block::as_select()))
                .first::<(orm::Transaction, orm::Block)>(conn)
                .map(|(orm_tx, block)| TX::from_storage(orm_tx, block.hash))
        })
        .map_err(|err| -> Box<dyn Error> { anyhow!(err).into() })
    }
}

#[async_trait]
impl<B, TX> ChainGateway for PostgresGateway<B, TX>
where
    B: StorableBlock<orm::Block, orm::NewBlock> + Send + Sync,
    TX: StorableTransaction<orm::Transaction, orm::NewTransaction, Vec<u8>, i64> + Send + Sync,
{
    type Block = B;
    type Transaction = TX;

    async fn add_block(&self, new: Self::Block) -> Result<(), Box<dyn Error>> {
        let mut conn = self.get_connection();
        self.insert_one_block(new, &mut conn).await
    }

    async fn get_block(
        &self,
        block_id: BlockIdentifier<'_>,
    ) -> Result<Self::Block, Box<dyn Error>> {
        let mut conn = self.get_connection();
        self.fetch_one_block(block_id, &mut conn).await
    }

    async fn add_tx(&self, new: Self::Transaction) -> Result<(), Box<dyn Error>> {
        let mut conn = self.get_connection();

        self.insert_one_tx(new, &mut conn).await
    }

    async fn get_tx(&self, hash: &[u8]) -> Result<Self::Transaction, Box<dyn Error>> {
        let mut conn = self.get_connection();
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
    use diesel::sql_query;
    use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};
    pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations/");

    use super::*;

    fn setup_db() -> PgConnection {
        let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
        let mut conn = PgConnection::establish(&database_url)
            .unwrap_or_else(|_| panic!("Error connecting to {}", database_url));
        conn.run_pending_migrations(MIGRATIONS).unwrap();
        sql_query(
            r#"
        INSERT INTO "chain"("name") 
        VALUES 
            ('ethereum'), 
            ('starknet'), 
            ('zksync');
        "#,
        )
        .execute(&mut conn)
        .expect("Setup of chain table failed");

        sql_query(
        r#"
        INSERT INTO block ("hash", "parent_hash", "number", "ts", "chain_id") 
        VALUES 
            -- ethereum blocks
            (E'\\x88e96d4537bea4d9c05d12549907b32561d3bf31f45aae734cdc119f13406cb6', E'\\xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3', 1, TIMESTAMP '2022-11-01 08:00:00', 1),
            (E'\\xb495a1d7e6663152ae92708da4843337b958146015a2802f4193a410044698c9', E'\\x88e96d4537bea4d9c05d12549907b32561d3bf31f45aae734cdc119f13406cb6', 2, TIMESTAMP '2022-11-01 09:00:00', 1),
            (E'\\x3d6122660cc824376f11ee842f83addc3525e2dd6756b9bcf0affa6aa88cf741', E'\\xb495a1d7e6663152ae92708da4843337b958146015a2802f4193a410044698c9', 3, TIMESTAMP '2022-11-01 10:00:00', 1), 
            (E'\\x23adf5a3be0f5235b36941bcb29b62504278ec5b9cdfa277b992ba4a7a3cd3a2', E'\\x3d6122660cc824376f11ee842f83addc3525e2dd6756b9bcf0affa6aa88cf741', 4, TIMESTAMP '2022-11-01 11:00:00', 1);
        "#,
        )
        .execute(&mut conn)
        .expect("Setup of block table failed");

        sql_query(
            r#"
        INSERT INTO "transaction" ("hash", "from", "to", "index", "block_id")
        VALUES
            (E'\\xbb7e16d797a9e2fbc537e30f91ed3d27a254dd9578aa4c3af3e5f0d3e8130945', E'\\x4648451b5F87FF8F0F7D622bD40574bb97E25980', E'\\x6B175474E89094C44Da98b954EedeAC495271d0F', 1, 1);
        "#,
        )
        .execute(&mut conn)
        .expect("Setup of transaction table failed");

        conn
    }

    fn teardown_db(mut conn: PgConnection) {
        conn.revert_all_migrations(MIGRATIONS).unwrap();
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
        let mut conn = setup_db();

        let gw = PostgresGateway::<evm::Block, evm::Transaction>::new(
            "postgres://postgres:mypassword@localhost:5432/tycho_indexer_0",
        );
        let exp = block("0xb495a1d7e6663152ae92708da4843337b958146015a2802f4193a410044698c9");
        let block_id = BlockIdentifier::Number((Chain::Ethereum, 2));

        let block = gw.fetch_one_block(block_id, &mut conn).await.unwrap();

        assert_eq!(block, exp);

        teardown_db(conn)
    }

    #[tokio::test]
    async fn test_add_block() {
        let mut conn = setup_db();

        let gw = PostgresGateway::<evm::Block, evm::Transaction>::new(
            "postgres://postgres:mypassword@localhost:5432/tycho_indexer_0",
        );
        let block = block("0xbadbabe000000000000000000000000000000000000000000000000000000000");

        gw.insert_one_block(block, &mut conn).await.unwrap();
        let retrieved_block = gw
            .get_block(BlockIdentifier::Hash(block.hash.as_bytes()))
            .await
            .unwrap();

        assert_eq!(retrieved_block, block);

        teardown_db(conn);
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
        let mut conn = setup_db();
        let gw = PostgresGateway::<evm::Block, evm::Transaction>::new(
            "postgres://postgres:mypassword@localhost:5432/tycho_indexer_0",
        );
        let exp = transaction("0xbb7e16d797a9e2fbc537e30f91ed3d27a254dd9578aa4c3af3e5f0d3e8130945");

        let tx = gw
            .fetch_one_tx(exp.hash.as_bytes(), &mut conn)
            .await
            .unwrap();

        assert_eq!(tx, exp);
        teardown_db(conn);
    }

    #[tokio::test]
    async fn test_add_tx() {
        let mut conn = setup_db();
        let gw = PostgresGateway::<evm::Block, evm::Transaction>::new(
            "postgres://postgres:mypassword@localhost:5432/tycho_indexer_0",
        );
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
        teardown_db(conn);
    }
}
