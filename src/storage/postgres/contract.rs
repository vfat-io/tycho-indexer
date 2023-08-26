use std::{collections::HashMap, sync::Arc};

use chrono::{NaiveDateTime, Utc};
use diesel::{pg::Pg, prelude::*, sql_types::BigInt};
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use ethers::types::U256;

use crate::storage::{orm, schema, BlockIdentifier, BlockOrTimestamp, ContractId, StorageError};

use super::PostgresGateway;

impl<B, TX> PostgresGateway<B, TX> {
    async fn get_slots_delta(
        &self,
        id: ContractId<'_>,
        current_version: Option<BlockOrTimestamp>,
        target_version: Option<BlockOrTimestamp>,
        conn: &mut AsyncPgConnection,
    ) -> Result<HashMap<U256, U256>, StorageError> {
        let chain_id = self.get_chain_id(id.0);
        let start_version_ts = version_to_ts(&current_version, conn).await?;
        let target_version_ts = version_to_ts(&target_version, conn).await?;

        dbg!(&start_version_ts);
        dbg!(&target_version_ts);

        let contract_id = schema::contract::table
            .inner_join(schema::chain::table)
            .filter(schema::chain::id.eq(chain_id))
            .filter(schema::contract::address.eq(id.1))
            .select(schema::contract::id)
            .first::<i64>(conn)
            .await
            .unwrap();

        dbg!(&contract_id);

        let (storage_alias, tx_alias) = diesel::alias!(
            schema::contract_storage as storage_alias,
            schema::transaction as tx_alias
        );

        // TODO: We need to use joins here there is no easy way to handle
        // insertion or deletions
        if start_version_ts <= target_version_ts {
            // We are going forward, we need to find all slots that had any
            // changes made to them, finally find the respective value of these
            // slots at target_version. Additionally we need to find any deleted
            // slots and emit them with value 0.

            let changed_slots = schema::contract_storage::table
                .inner_join(schema::transaction::table)
                .filter(schema::contract_storage::contract_id.eq(contract_id))
                .filter(
                    schema::contract_storage::valid_from
                        .gt(start_version_ts)
                        .and(schema::contract_storage::valid_from.le(target_version_ts))
                        .or(schema::contract_storage::valid_to
                            .gt(start_version_ts)
                            .and(schema::contract_storage::valid_to.le(target_version_ts))),
                )
                .select(schema::contract_storage::slot)
                .distinct_on(schema::contract_storage::slot)
                .get_results::<Vec<u8>>(conn)
                .await?;

            let changed_values: Vec<(Vec<u8>, Vec<u8>)> = schema::contract_storage::table
                .inner_join(schema::transaction::table)
                .filter(schema::contract_storage::contract_id.eq(contract_id))
                .filter(schema::contract_storage::slot.eq_any(changed_slots))
                .filter(schema::contract_storage::valid_from.le(target_version_ts))
                .filter(
                    schema::contract_storage::valid_to
                        .is_null()
                        .or(schema::contract_storage::valid_to.gt(target_version_ts)),
                )
                // select max tx_index row
                .order_by((
                    schema::contract_storage::slot,
                    schema::transaction::index.desc(),
                ))
                .select((
                    schema::contract_storage::slot,
                    schema::contract_storage::value,
                ))
                .distinct_on(schema::contract_storage::slot)
                .get_results::<(Vec<u8>, Vec<u8>)>(conn)
                .await?;

            // TODO any changed slots that do not have values here were deleted

            Ok(HashMap::from_iter(changed_values.iter().map(|(rk, rv)| {
                // TODO: don't panic, use error if bytes are too short
                (U256::from_big_endian(rk), U256::from_big_endian(rv))
            })))
        } else {
            // we are going backwards, so we'll include data to revert from the
            // current version. Assume the target version changes were applied
            // previously already
            Ok(HashMap::new())
        }
    }
}

async fn version_to_ts(
    start_version: &Option<BlockOrTimestamp>,
    conn: &mut AsyncPgConnection,
) -> Result<NaiveDateTime, StorageError> {
    match &start_version {
        Some(BlockOrTimestamp::Block(BlockIdentifier::Hash(h))) => {
            Ok(orm::Block::by_hash(&h, conn)
                .await
                .map_err(|err| StorageError::from_diesel(err, "Block", &hex::encode(h), None))?
                .ts)
        }
        Some(BlockOrTimestamp::Block(BlockIdentifier::Number((chain, no)))) => {
            Ok(orm::Block::by_number(*chain, *no, conn)
                .await
                .map_err(|err| StorageError::from_diesel(err, "Block", &format!("{}", no), None))?
                .ts)
        }
        Some(BlockOrTimestamp::Timestamp(ts)) => Ok(*ts),
        None => Ok(Utc::now().naive_utc()),
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use chrono::NaiveDateTime;
    use diesel::prelude::*;
    use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
    use ethers::types::H160;
    use ethers::types::U256;

    use crate::extractor::evm;
    use crate::models::Chain;
    use crate::storage::orm;
    use crate::storage::postgres::fixtures;
    use crate::storage::postgres::PostgresGateway;
    use crate::storage::schema;
    use crate::storage::BlockOrTimestamp;
    use crate::storage::ContractId;

    async fn setup_db() -> AsyncPgConnection {
        let db_url = std::env::var("DATABASE_URL").unwrap();
        let mut conn = AsyncPgConnection::establish(&db_url).await.unwrap();
        conn.begin_test_transaction().await.unwrap();
        conn
    }

    async fn setup_slots_delta(conn: &mut AsyncPgConnection) {
        let chain_id = fixtures::insert_chain(conn, "ethereum").await;
        let blk = fixtures::insert_blocks(conn, chain_id).await;
        let txn = fixtures::insert_txns(
            conn,
            &[
                (
                    blk[0],
                    1i64,
                    "0xbb7e16d797a9e2fbc537e30f91ed3d27a254dd9578aa4c3af3e5f0d3e8130945",
                ),
                (
                    blk[1],
                    1i64,
                    "0x3108322284d0a89a7accb288d1a94384d499504fe7e04441b0706c7628dee7b7",
                ),
            ],
        )
        .await;
        let c0 = fixtures::insert_contract(
            conn,
            "0x6B175474E89094C44Da98b954EedeAC495271d0F",
            "c0",
            chain_id,
        )
        .await;
        fixtures::insert_slots(
            conn,
            c0,
            txn[0],
            "2020-01-01T00:00:00",
            &[(0, 1), (1, 1), (2, 1)],
        )
        .await;
        fixtures::insert_slots(conn, c0, txn[1], "2020-01-01T01:00:00", &[(0, 2), (1, 3)]).await;
    }

    #[tokio::test]
    async fn get_slots_delta() {
        let mut conn = setup_db().await;
        setup_slots_delta(&mut conn).await;

        let all_slots: Vec<orm::ContractStorage> = schema::contract_storage::table
            .select(orm::ContractStorage::as_select())
            .get_results(&mut conn)
            .await
            .unwrap();

        dbg!(all_slots
            .iter()
            .map(|s| (
                U256::from_big_endian(&s.slot),
                U256::from_big_endian(&s.value),
                s.valid_from,
                s.valid_to
            ))
            .collect::<Vec<_>>());

        let gw = PostgresGateway::<evm::Block, evm::Transaction>::from_connection(&mut conn).await;

        let tmp = gw
            .get_slots_delta(
                ContractId(
                    Chain::Ethereum,
                    H160::from_str("0x6B175474E89094C44Da98b954EedeAC495271d0F")
                        .unwrap()
                        .as_bytes(),
                ),
                Some(BlockOrTimestamp::Timestamp(
                    "2020-01-01T00:00:00".parse::<NaiveDateTime>().unwrap(),
                )),
                Some(BlockOrTimestamp::Timestamp(
                    "2020-01-01T02:00:00".parse::<NaiveDateTime>().unwrap(),
                )),
                &mut conn,
            )
            .await;
        dbg!(tmp);
    }
}
