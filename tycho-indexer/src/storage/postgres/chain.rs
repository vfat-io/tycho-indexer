use async_trait::async_trait;
use chrono::NaiveDateTime;
use diesel::prelude::*;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use itertools::Itertools;
use std::collections::HashMap;
use tracing::{instrument, warn};
use tycho_types::Bytes;

use crate::storage::{
    BlockHash, BlockIdentifier, ChainGateway, ContractDelta, StorableBlock, StorableContract,
    StorableToken, StorableTransaction, StorageError, TxHash,
};

use super::{orm, schema, PostgresGateway};

#[async_trait]
impl<B, TX, A, D, T> ChainGateway for PostgresGateway<B, TX, A, D, T>
where
    B: StorableBlock<orm::Block, orm::NewBlock, i64>,
    TX: StorableTransaction<orm::Transaction, orm::NewTransaction, i64>,
    D: ContractDelta,
    A: StorableContract<orm::Contract, orm::NewContract, i64>,
    T: StorableToken<orm::Token, orm::NewToken, i64>,
{
    type DB = AsyncPgConnection;
    type Block = B;
    type Transaction = TX;

    #[instrument(skip_all)]
    async fn upsert_block(
        &self,
        new: &[Self::Block],
        conn: &mut Self::DB,
    ) -> Result<(), StorageError> {
        use super::schema::block::dsl::*;
        if new.is_empty() {
            warn!("Upsert blocks called with empty blocks!");
            return Ok(())
        }
        let block_chain_id = self.get_chain_id(new[0].chain());
        let new_blocks = new
            .iter()
            .map(|n| n.to_storage(block_chain_id))
            .collect_vec();

        // assumes that block with the same hash will not appear with different values
        diesel::insert_into(block)
            .values(&new_blocks)
            .on_conflict_do_nothing()
            .execute(conn)
            .await
            .map_err(|err| {
                StorageError::from_diesel(
                    err,
                    "Block",
                    &format!("Batch: {} and {} more", &new_blocks[0].hash, new_blocks.len() - 1),
                    None,
                )
            })?;
        Ok(())
    }

    #[instrument(skip_all)]
    async fn get_block(
        &self,
        block_id: &BlockIdentifier,
        conn: &mut Self::DB,
    ) -> Result<Self::Block, StorageError> {
        // taking a reference here is necessary, to not move block_id
        // so it can be used in the map_err closure later on. It would
        // be better if BlockIdentifier was copy though (complicates lifetimes).
        let orm_block = match &block_id {
            BlockIdentifier::Number((chain, number)) => {
                orm::Block::by_number(*chain, *number, conn).await
            }

            BlockIdentifier::Hash(block_hash) => orm::Block::by_hash(block_hash, conn).await,
            BlockIdentifier::Latest(chain) => orm::Block::most_recent(*chain, conn).await,
        }
        .map_err(|err| StorageError::from_diesel(err, "Block", &block_id.to_string(), None))?;
        let chain = self.get_chain(&orm_block.chain_id);
        B::from_storage(orm_block, chain)
    }

    #[instrument(skip_all)]
    async fn upsert_tx(
        &self,
        new: &[Self::Transaction],
        conn: &mut Self::DB,
    ) -> Result<(), StorageError> {
        use super::schema::transaction::dsl::*;
        if new.is_empty() {
            warn!("Upsert tx called with empty transactions!");
            return Ok(())
        }

        let block_hashes = new
            .iter()
            .map(|n| n.block_hash())
            .collect_vec();
        let parent_blocks = schema::block::table
            .filter(schema::block::hash.eq_any(&block_hashes))
            .select((schema::block::hash, schema::block::id))
            .get_results::<(Bytes, i64)>(conn)
            .await
            .map_err(|err| {
                StorageError::from_diesel(
                    err,
                    "Transaction",
                    &format!("Batch: {:x} and {} more", &block_hashes[0], block_hashes.len() - 1),
                    Some("Block".to_owned()),
                )
            })?
            .into_iter()
            .collect::<HashMap<_, _>>();

        let orm_txns = new
            .iter()
            .map(|n| {
                let block_h = n.block_hash();
                let bid = *parent_blocks
                    .get(&block_h)
                    .ok_or_else(|| {
                        StorageError::NoRelatedEntity(
                            "Block".to_string(),
                            "Transaction".to_string(),
                            format!("{}", block_h),
                        )
                    })?;
                Ok(n.to_storage(bid))
            })
            .collect::<Result<Vec<orm::NewTransaction>, StorageError>>()?;

        // assumes that tx with the same hash will not appear with different values
        diesel::insert_into(transaction)
            .values(&orm_txns)
            .on_conflict_do_nothing()
            .execute(conn)
            .await
            .map_err(|err| {
                StorageError::from_diesel(
                    err,
                    "Transaction",
                    &format!("Batch {:x} and {} more", &orm_txns[0].hash, orm_txns.len() - 1),
                    None,
                )
            })?;
        Ok(())
    }

    #[instrument(skip_all)]
    async fn get_tx(
        &self,
        hash: &TxHash,
        conn: &mut Self::DB,
    ) -> Result<Self::Transaction, StorageError> {
        schema::transaction::table
            .inner_join(schema::block::table)
            .filter(schema::transaction::hash.eq(&hash))
            .select((orm::Transaction::as_select(), schema::block::hash))
            .first::<(orm::Transaction, BlockHash)>(conn)
            .await
            .map(|(orm_tx, block_hash)| TX::from_storage(orm_tx, &block_hash))
            .map_err(|err| {
                StorageError::from_diesel(err, "Transaction", &hex::encode(hash), None)
            })?
    }

    async fn revert_state(
        &self,
        to: &BlockIdentifier,
        conn: &mut AsyncPgConnection,
    ) -> Result<(), StorageError> {
        // To revert all changes of a chain, we need to delete & modify entries
        // from a big number of tables. Reverting state, signifies deleting
        // history. We will not keep any branches in the db only the main branch
        // will be kept.
        let block = orm::Block::by_id(to, conn).await?;

        // All entities and version updates are connected to the block via a
        // cascade delete, this ensures that the state is reverted by simply
        // deleting the correct blocks, which then triggers cascading deletes on
        // child entries. All blocks after the `to` block are deleted - the `to`
        // block and its connected data persists.
        diesel::delete(
            schema::block::table
                .filter(schema::block::number.gt(block.number))
                .filter(schema::block::chain_id.eq(block.chain_id)),
        )
        .execute(conn)
        .await?;

        // Any versioned table's rows, which have `valid_to` set to "> block.ts"
        // need, to be updated to be valid again (thus, valid_to = NULL).
        diesel::update(
            schema::contract_storage::table.filter(schema::contract_storage::valid_to.gt(block.ts)),
        )
        .set(schema::contract_storage::valid_to.eq(Option::<NaiveDateTime>::None))
        .execute(conn)
        .await?;

        diesel::update(
            schema::account_balance::table.filter(schema::account_balance::valid_to.gt(block.ts)),
        )
        .set(schema::account_balance::valid_to.eq(Option::<NaiveDateTime>::None))
        .execute(conn)
        .await?;

        diesel::update(
            schema::contract_code::table.filter(schema::contract_code::valid_to.gt(block.ts)),
        )
        .set(schema::contract_code::valid_to.eq(Option::<NaiveDateTime>::None))
        .execute(conn)
        .await?;

        diesel::update(
            schema::protocol_state::table.filter(schema::protocol_state::valid_to.gt(block.ts)),
        )
        .set(schema::protocol_state::valid_to.eq(Option::<NaiveDateTime>::None))
        .execute(conn)
        .await?;

        diesel::update(
            schema::protocol_calls_contract::table
                .filter(schema::protocol_calls_contract::valid_to.gt(block.ts)),
        )
        .set(schema::protocol_calls_contract::valid_to.eq(Option::<NaiveDateTime>::None))
        .execute(conn)
        .await?;

        // Any versioned table's rows, which have `deleted_at` set to "> block.ts"
        // need, to be updated to be valid again (thus, deleted_at = NULL).
        diesel::update(schema::account::table.filter(schema::account::deleted_at.gt(block.ts)))
            .set(schema::account::deleted_at.eq(Option::<NaiveDateTime>::None))
            .execute(conn)
            .await?;

        diesel::update(
            schema::protocol_component::table
                .filter(schema::protocol_component::deleted_at.gt(block.ts)),
        )
        .set(schema::protocol_component::deleted_at.eq(Option::<NaiveDateTime>::None))
        .execute(conn)
        .await?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use diesel_async::AsyncConnection;
    use ethers::types::{H160, H256, U256};

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

    async fn setup_data(conn: &mut AsyncPgConnection) {
        let chain_id: i64 = db_fixtures::insert_chain(conn, "ethereum").await;
        let block_ids = db_fixtures::insert_blocks(conn, chain_id).await;
        db_fixtures::insert_txns(
            conn,
            &[(
                block_ids[0],
                1i64,
                "0xbb7e16d797a9e2fbc537e30f91ed3d27a254dd9578aa4c3af3e5f0d3e8130945",
            )],
        )
        .await;
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
            ts: "2020-01-01T01:00:00".parse().unwrap(),
        }
    }

    #[tokio::test]
    async fn test_get_block_latest() {
        let mut conn = setup_db().await;
        setup_data(&mut conn).await;
        let gw = EVMGateway::from_connection(&mut conn).await;
        let exp = block("0xb495a1d7e6663152ae92708da4843337b958146015a2802f4193a410044698c9");
        let block_id = BlockIdentifier::Latest(Chain::Ethereum);

        let block = gw
            .get_block(&block_id, &mut conn)
            .await
            .unwrap();

        assert_eq!(block, exp);
    }

    #[tokio::test]
    async fn test_get_block() {
        let mut conn = setup_db().await;
        setup_data(&mut conn).await;
        let gw = EVMGateway::from_connection(&mut conn).await;
        let exp = block("0xb495a1d7e6663152ae92708da4843337b958146015a2802f4193a410044698c9");
        let block_id = BlockIdentifier::Number((Chain::Ethereum, 2));

        let block = gw
            .get_block(&block_id, &mut conn)
            .await
            .unwrap();

        assert_eq!(block, exp);
    }

    #[tokio::test]
    async fn test_add_block() {
        let mut conn = setup_db().await;
        setup_data(&mut conn).await;
        let gw = EVMGateway::from_connection(&mut conn).await;
        let block = block("0xbadbabe000000000000000000000000000000000000000000000000000000000");

        gw.upsert_block(&[block], &mut conn)
            .await
            .unwrap();
        let retrieved_block = gw
            .get_block(&BlockIdentifier::Hash(block.hash.into()), &mut conn)
            .await
            .unwrap();

        assert_eq!(retrieved_block, block);
    }

    #[tokio::test]
    async fn test_upsert_block() {
        let mut conn = setup_db().await;
        setup_data(&mut conn).await;
        let gw = EVMGateway::from_connection(&mut conn).await;
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
            ts: "2020-01-01T00:00:00".parse().unwrap(),
        };

        gw.upsert_block(&[block], &mut conn)
            .await
            .unwrap();
        let retrieved_block = gw
            .get_block(&BlockIdentifier::Hash(block.hash.into()), &mut conn)
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
            to: Some(H160::from_str("0x6b175474e89094c44da98b954eedeac495271d0f").expect("to ok")),
            index: 1,
        }
    }

    #[tokio::test]
    async fn test_get_tx() {
        let mut conn = setup_db().await;
        setup_data(&mut conn).await;
        let gw = EVMGateway::from_connection(&mut conn).await;
        let exp = transaction("0xbb7e16d797a9e2fbc537e30f91ed3d27a254dd9578aa4c3af3e5f0d3e8130945");

        let tx = gw
            .get_tx(&exp.hash.into(), &mut conn)
            .await
            .unwrap();

        assert_eq!(tx, exp);
    }

    #[tokio::test]
    async fn test_add_tx() {
        let mut conn = setup_db().await;
        setup_data(&mut conn).await;
        let gw = EVMGateway::from_connection(&mut conn).await;
        let mut tx =
            transaction("0xbadbabe000000000000000000000000000000000000000000000000000000000");
        tx.block_hash =
            H256::from_str("0xb495a1d7e6663152ae92708da4843337b958146015a2802f4193a410044698c9")
                .unwrap();

        gw.upsert_tx(&[tx], &mut conn)
            .await
            .unwrap();
        let retrieved_tx = gw
            .get_tx(&tx.hash.into(), &mut conn)
            .await
            .unwrap();

        assert_eq!(tx, retrieved_tx);
    }

    #[tokio::test]
    async fn test_upsert_tx() {
        let mut conn = setup_db().await;
        setup_data(&mut conn).await;
        let gw = EVMGateway::from_connection(&mut conn).await;
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
            to: Some(H160::from_str("0x6B175474E89094C44Da98b954EedeAC495271d0F").expect("to ok")),
            index: 1,
        };

        gw.upsert_tx(&[tx], &mut conn)
            .await
            .unwrap();
        let retrieved_tx = gw
            .get_tx(&tx.hash.into(), &mut conn)
            .await
            .unwrap();

        assert_eq!(tx, retrieved_tx);
    }

    async fn setup_revert_data(conn: &mut AsyncPgConnection) {
        let chain_id = db_fixtures::insert_chain(conn, "ethereum").await;
        let blk = db_fixtures::insert_blocks(conn, chain_id).await;
        let txn = db_fixtures::insert_txns(
            conn,
            &[
                (
                    // deploy c0
                    blk[0],
                    1i64,
                    "0xbb7e16d797a9e2fbc537e30f91ed3d27a254dd9578aa4c3af3e5f0d3e8130945",
                ),
                (
                    // change c0 state, deploy c2
                    blk[0],
                    2i64,
                    "0x794f7df7a3fe973f1583fbb92536f9a8def3a89902439289315326c04068de54",
                ),
                // ----- Block 01 LAST
                (
                    // deploy c1, delete c2
                    blk[1],
                    1i64,
                    "0x3108322284d0a89a7accb288d1a94384d499504fe7e04441b0706c7628dee7b7",
                ),
                (
                    // change c0 and c1 state
                    blk[1],
                    2i64,
                    "0x50449de1973d86f21bfafa7c72011854a7e33a226709dc3e2e4edcca34188388",
                ),
                // ----- Block 02 LAST
            ],
        )
        .await;

        // Account C0
        let c0 = db_fixtures::insert_account(
            conn,
            "6B175474E89094C44Da98b954EedeAC495271d0F",
            "account0",
            chain_id,
            Some(txn[0]),
        )
        .await;
        db_fixtures::insert_account_balance(conn, 0, txn[0], Some("2020-01-01T00:00:00"), c0).await;
        db_fixtures::insert_contract_code(conn, c0, txn[0], Bytes::from_str("C0C0C0").unwrap())
            .await;
        db_fixtures::insert_account_balance(conn, 100, txn[1], Some("2020-01-01T01:00:00"), c0)
            .await;
        // Slot 2 is never modified again
        db_fixtures::insert_slots(conn, c0, txn[1], "2020-01-01T00:00:00", None, &[(2, 1, None)])
            .await;
        // First version for slots 0 and 1.
        db_fixtures::insert_slots(
            conn,
            c0,
            txn[1],
            "2020-01-01T00:00:00",
            Some("2020-01-01T01:00:00"),
            &[(0, 1, None), (1, 5, None)],
        )
        .await;
        db_fixtures::insert_account_balance(conn, 101, txn[3], None, c0).await;
        // Second and final version for 0 and 1, new slots 5 and 6
        db_fixtures::insert_slots(
            conn,
            c0,
            txn[3],
            "2020-01-01T01:00:00",
            None,
            &[(0, 2, Some(1)), (1, 3, Some(5)), (5, 25, None), (6, 30, None)],
        )
        .await;

        // Account C1
        let c1 = db_fixtures::insert_account(
            conn,
            "73BcE791c239c8010Cd3C857d96580037CCdd0EE",
            "c1",
            chain_id,
            Some(txn[2]),
        )
        .await;
        db_fixtures::insert_account_balance(conn, 50, txn[2], None, c1).await;
        db_fixtures::insert_contract_code(conn, c1, txn[2], Bytes::from_str("C1C1C1").unwrap())
            .await;
        db_fixtures::insert_slots(
            conn,
            c1,
            txn[3],
            "2020-01-01T01:00:00",
            None,
            &[(0, 128, None), (1, 255, None)],
        )
        .await;
    }

    #[tokio::test]
    async fn test_revert() {
        let mut conn = setup_db().await;
        setup_revert_data(&mut conn).await;

        // set up contracts data
        let block1_hash =
            H256::from_str("0x88e96d4537bea4d9c05d12549907b32561d3bf31f45aae734cdc119f13406cb6")
                .unwrap()
                .0
                .into();
        let c0_address =
            Bytes::from_str("6B175474E89094C44Da98b954EedeAC495271d0F").expect("c0 address valid");
        let exp_slots: HashMap<U256, U256> = vec![
            (U256::from(0), U256::from(1)),
            (U256::from(1), U256::from(5)),
            (U256::from(2), U256::from(1)),
        ]
        .into_iter()
        .collect();

        let gw = EVMGateway::from_connection(&mut conn).await;

        gw.revert_state(&BlockIdentifier::Hash(block1_hash), &mut conn)
            .await
            .unwrap();

        let slots: HashMap<U256, U256> = schema::contract_storage::table
            .inner_join(schema::account::table)
            .filter(schema::account::address.eq(c0_address))
            .select((schema::contract_storage::slot, schema::contract_storage::value))
            .get_results::<(Bytes, Option<Bytes>)>(&mut conn)
            .await
            .unwrap()
            .iter()
            .map(|(k, v)| {
                (
                    U256::from_big_endian(k),
                    v.as_ref()
                        .map(|rv| U256::from_big_endian(rv))
                        .unwrap_or_else(U256::zero),
                )
            })
            .collect();
        assert_eq!(slots, exp_slots);

        let c1 = schema::account::table
            .filter(schema::account::title.eq("c1"))
            .select(schema::account::id)
            .get_results::<i64>(&mut conn)
            .await
            .unwrap();
        assert_eq!(c1.len(), 0);
    }
}
