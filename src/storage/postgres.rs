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

impl<B, TX> PostgresGateway<B, TX> {
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
        use super::schema::block::dsl::*;
        let pool = self.pool.clone();
        let conn = &mut pool.get().unwrap();

        let orm_new = new.to_storage(self.get_chain_id(new.chain()));
        diesel::insert_into(block)
            .values(&orm_new)
            .execute(conn)
            .map_err(|err| -> Box<dyn std::error::Error> { anyhow!(err.to_string()).into() })?;
        Ok(())
    }

    async fn get_block(
        &self,
        block_id: BlockIdentifier<'_>,
    ) -> Result<Self::Block, Box<dyn Error>> {
        let pool = self.pool.clone();
        let conn = &mut pool.get().unwrap();

        match block_id {
            BlockIdentifier::Number((chain, block_no)) => schema::block::table
                .inner_join(schema::chain::table)
                .filter(schema::block::id.eq(block_no))
                .filter(schema::chain::name.eq(chain.to_string()))
                .select(orm::Block::as_select())
                .first::<orm::Block>(conn)
                .map(|orm_block| {
                    let chain = self.get_chain(orm_block.chain_id);
                    Self::Block::from_storage(orm_block, chain)
                })
                .map_err(|err| anyhow!(err).into()),

            BlockIdentifier::Hash(block_hash) => schema::block::table
                .filter(schema::block::hash.eq(block_hash))
                .select(orm::Block::as_select())
                .first::<orm::Block>(conn)
                .map(|orm_block| {
                    let chain = self.get_chain(orm_block.chain_id);
                    Self::Block::from_storage(orm_block, chain)
                })
                .map_err(|err| anyhow!(err).into()),
        }
    }

    async fn add_tx(&self, new: Self::Transaction) -> Result<(), Box<dyn Error>> {
        use super::schema::transaction::dsl::*;

        let pool = self.pool.clone();
        let conn = &mut pool.get().unwrap();

        let orm_new: orm::NewTransaction = new.to_storage(0);

        // TODO: we need to get the block identiy here first
        // this should be defined on a transaction trait:
        // let block_hash = new.block_hash() -> Into<Vec<u8>>

        diesel::insert_into(transaction)
            .values(&orm_new)
            .execute(conn)
            .map_err(|err| -> Box<dyn std::error::Error> { anyhow!(err.to_string()).into() })?;
        Ok(())
    }

    async fn get_tx(&self, hash: &[u8]) -> Result<Self::Transaction, Box<dyn Error>> {
        let pool = self.pool.clone();
        let conn = &mut pool.get().unwrap();
        // TODO: transaction should be chain scoped as well.
        // TODO: we need to record the block identity somewhere within the transaction.
        //  How should we do that?
        // we need a way to create a transaction object given some object and a block identifier
        // also that is something a transaction trait has to define.
        schema::transaction::table
            .filter(schema::transaction::dsl::hash.eq(hash))
            .select(orm::Transaction::as_select())
            .first::<orm::Transaction>(conn)
            .map(|orm_tx| Self::Transaction::from_storage(orm_tx, Vec::new()))
            .map_err(|err| anyhow!(err).into())
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
        todo!()
    }

    fn to_storage(&self, block_id: i64) -> orm::NewTransaction {
        todo!()
    }

    fn block_hash(&self) -> Vec<u8> {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use chrono::NaiveDateTime;
    use ethers::abi::AbiDecode;

    use super::ChainGateway;
    use super::*;

    #[tokio::test]
    async fn test_add_block() {
        let gw = PostgresGateway::<evm::Block, evm::Transaction>::new(
            "postgres://postgres:mypassword@localhost:5432/tycho_indexer_0",
        );
        let exp = evm::Block {
            number: 2,
            hash: H256::decode_hex(
                "0xb495a1d7e6663152ae92708da4843337b958146015a2802f4193a410044698c9",
            )
            .unwrap(),
            parent_hash: H256::decode_hex(
                "0x88e96d4537bea4d9c05d12549907b32561d3bf31f45aae734cdc119f13406cb6",
            )
            .unwrap(),
            chain: Chain::Ethereum,
            ts: NaiveDateTime::parse_from_str("2022-11-01T09:00:00", "%Y-%m-%dT%H:%M:%S").unwrap(),
        };
        let block_id = BlockIdentifier::Number((Chain::Ethereum, 2));

        let block = gw.get_block(block_id).await.unwrap();

        assert_eq!(block, exp);
    }
}
