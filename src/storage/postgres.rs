use std::error::Error;
use std::marker::PhantomData;

use anyhow::anyhow;
use async_trait::async_trait;
use diesel::prelude::*;
use diesel::r2d2::ConnectionManager;
use diesel::r2d2::Pool;
use diesel::sql_types::Bool;

use crate::models::Chain;
use crate::storage::orm;
use crate::storage::schema;

use super::BlockIdentifier;
use super::ChainGateway;

pub struct PostgresGateway<B, TX> {
    pool: Pool<ConnectionManager<PgConnection>>,
    default_chain: Chain,
    _phantom_block: PhantomData<B>,
    _phantom_tx: PhantomData<TX>,
}

#[async_trait]
impl<B, TX> ChainGateway for PostgresGateway<B, TX>
where
    B: Send + Sync + From<orm::Block>,
    TX: Send + Sync + From<orm::Transaction>,
{
    type Block = B;
    type Transaction = TX;

    async fn add_block(&self, new: Self::Block) -> Result<(), Box<dyn Error>> {
        todo!()
    }
    async fn get_block(
        &self,
        block_id: BlockIdentifier<'_>,
    ) -> Result<Self::Block, Box<dyn Error>> {
        let pool = self.pool.clone();
        let conn = &mut pool.get().unwrap();

        let chain_name: String = self.default_chain.into();

        match block_id {
            BlockIdentifier::Number(block_no) => schema::block::table
                .inner_join(schema::chain::table)
                .filter(schema::block::id.eq(block_no))
                .filter(schema::chain::name.eq(chain_name))
                .select(orm::Block::as_select())
                .first::<orm::Block>(conn)
                .map(|orm_block| orm_block.into())
                .map_err(|err| anyhow!(err).into()),
            BlockIdentifier::Hash(block_hash) => schema::block::table
                .inner_join(schema::chain::table)
                .filter(schema::block::hash.eq(block_hash))
                .filter(schema::chain::name.eq(chain_name))
                .select(orm::Block::as_select())
                .first::<orm::Block>(conn)
                .map(|orm_block| orm_block.into())
                .map_err(|err| anyhow!(err).into()),
        }
    }
    async fn add_tx(&self, new: Self::Transaction) -> Result<(), Box<dyn Error>> {
        todo!()
    }
    async fn get_tx(&self, hash: &[u8]) -> Result<Self::Transaction, Box<dyn Error>> {
        todo!()
    }
}
