//! # Postgres based storage backend
//!
//! This postgres-based storage backend provides implementations for the
//! traits defined in the storage module.
//!
//! ## Design Decisions
//!
//! ### Representation of Enums as Tables
//!
//! Certain enums such as 'Chain' are modelled as tables in our implementation.
//! This decision stems from an understanding that while extending the Rust
//! codebase to include more enums is a straightforward task, modifying the type
//! of a SQL column can be an intricate process. By representing enums as
//! tables, we circumvent unnecessary migrations when modifying Chain or
//! ProtocolSystem enums.
//!
//! With this representation, it's important to synchronize them whenever the
//! enums members changed. This can be done automatically once at system
//! startup.
//!
//!
//! Note: A removed enum can be ignored safely even though it might instigate a
//! panic if an associated entity still exists in the database and retrieved
//! with a codebase which no longer presents the enum value.
//!
//! ### Atomic Transactions
//!
//! In our design, direct connection to the database and consequently beginning,
//! committing, or rolling back transactions isn't handled within these
//! common-purpose implementations. Rather, each operation receives a connection
//! reference which can either be a simple DB connection, or a DB connection
//! within a transactional context.
//!
//! This approach enables us to chain multiple common-purpose CRUD operations
//! into a single transaction. This guarantees preservation of valid state
//! throughout the application lifetime, even if the process panics during
//! database operations.
pub mod chain;
pub mod contract;
pub mod contract_state;
pub mod extraction_state;
pub mod orm;
pub mod schema;

use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use diesel::prelude::*;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use ethers::types::{H160, H256};

use super::StorageError;
use crate::extractor::evm;
use crate::models::Chain;

pub struct EnumTableCache<E> {
    map_id: HashMap<E, i64>,
    map_enum: HashMap<i64, E>,
}

/// Provides caching for enum and its database ID relationships.
///
/// Uses a double sided hash map to provide quick lookups in both directions.
impl<E> EnumTableCache<E>
where
    E: Eq + Hash + Copy + From<String> + std::fmt::Debug,
{
    /// Creates a new cache from a slice of tuples.
    ///
    /// # Arguments
    ///
    /// * `entries` - A slice of tuples ideally obtained from a database query.
    pub fn from_tuples(entries: &[(i64, String)]) -> Self {
        let mut cache = Self {
            map_id: HashMap::new(),
            map_enum: HashMap::new(),
        };
        for (id_, name_) in entries {
            let val = E::from(name_.to_owned());
            cache.map_id.insert(val, *id_);
            cache.map_enum.insert(*id_, val);
        }
        cache
    }
    /// Fetches the associated database ID for an enum variant. Panics on cache
    /// miss.
    ///
    /// # Arguments
    ///
    /// * `val` - The enum variant to lookup.
    fn get_id(&self, val: E) -> i64 {
        *self.map_id.get(&val).unwrap_or_else(|| {
            panic!(
                "Unexpected cache miss for enum {:?}, entries: {:?}",
                val, self.map_id
            )
        })
    }

    /// Retrieves the corresponding enum variant for a database ID. Panics on
    /// cache miss.
    ///
    /// # Arguments
    ///
    /// * `id` - The database ID to lookup.
    fn get_chain(&self, id: i64) -> E {
        *self.map_enum.get(&id).unwrap_or_else(|| {
            panic!(
                "Unexpected cache miss for id {}, entries: {:?}",
                id, self.map_enum
            )
        })
    }
}

type ChainEnumCache = EnumTableCache<Chain>;

impl From<diesel::result::Error> for StorageError {
    fn from(value: diesel::result::Error) -> Self {
        StorageError::Unexpected(format!("DieselError: {}", value))
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
    chain_id_cache: Arc<ChainEnumCache>,
    _phantom_block: PhantomData<B>,
    _phantom_tx: PhantomData<TX>,
}

impl<B, TX> PostgresGateway<B, TX> {
    pub fn new(cache: Arc<ChainEnumCache>) -> Self {
        Self {
            chain_id_cache: cache,
            _phantom_block: PhantomData,
            _phantom_tx: PhantomData,
        }
    }

    #[cfg(test)]
    async fn from_connection(conn: &mut AsyncPgConnection) -> Self {
        let results: Vec<(i64, String)> = async {
            use schema::chain::dsl::*;
            chain
                .select((id, name))
                .load(conn)
                .await
                .expect("Failed to load chain ids!")
        }
        .await;
        let cache = Arc::new(ChainEnumCache::from_tuples(&results));
        Self::new(cache)
    }

    fn get_chain_id(&self, chain: Chain) -> i64 {
        self.chain_id_cache.get_id(chain)
    }

    fn get_chain(&self, id: i64) -> Chain {
        self.chain_id_cache.get_chain(id)
    }
}

#[cfg(test)]
mod fixtures {
    use std::str::FromStr;

    use diesel::prelude::*;
    use diesel_async::{AsyncPgConnection, RunQueryDsl};
    use ethers::types::{H160, H256, U256};

    use crate::storage::schema;

    // Insert a new chain
    pub async fn insert_chain(conn: &mut AsyncPgConnection, name: &str) -> i64 {
        diesel::insert_into(schema::chain::table)
            .values(schema::chain::name.eq(name))
            .returning(schema::chain::id)
            .get_result(conn)
            .await
            .unwrap()
    }

    /// Inserts two sequential blocks
    pub async fn insert_blocks(conn: &mut AsyncPgConnection, chain_id: i64) -> Vec<i64> {
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
        diesel::insert_into(schema::block::table)
            .values(&block_records)
            .returning(schema::block::id)
            .get_results(conn)
            .await
            .unwrap()
    }

    /// Insert a bunch of transactions using (block_id, index, hash)
    pub async fn insert_txns(conn: &mut AsyncPgConnection, txns: &[(i64, i64, &str)]) -> Vec<i64> {
        let from_val = H160::from_str("0x4648451b5F87FF8F0F7D622bD40574bb97E25980").unwrap();
        let to_val = H160::from_str("0x6B175474E89094C44Da98b954EedeAC495271d0F").unwrap();
        let data: Vec<_> = txns
            .iter()
            .map(|(b, i, h)| {
                use schema::transaction::dsl::*;
                (
                    block_id.eq(b),
                    index.eq(i),
                    hash.eq(H256::from_str(h)
                        .expect("valid txhash")
                        .as_bytes()
                        .to_owned()),
                    from.eq(from_val.as_bytes()),
                    to.eq(to_val.as_bytes()),
                )
            })
            .collect();
        diesel::insert_into(schema::transaction::table)
            .values(&data)
            .returning(schema::transaction::id)
            .get_results(conn)
            .await
            .unwrap()
    }

    pub async fn insert_contract(
        conn: &mut AsyncPgConnection,
        address: &str,
        title: &str,
        chain_id: i64,
    ) -> i64 {
        diesel::insert_into(schema::contract::table)
            .values((
                schema::contract::title.eq(title),
                schema::contract::chain_id.eq(chain_id),
                schema::contract::address
                    .eq(hex::decode("6B175474E89094C44Da98b954EedeAC495271d0F").unwrap()),
            ))
            .returning(schema::contract::id)
            .get_result(conn)
            .await
            .unwrap()
    }

    pub async fn insert_slots(
        conn: &mut AsyncPgConnection,
        contract_id: i64,
        modify_tx: i64,
        valid_from: &str,
        slots: &[(u64, u64)],
    ) -> Vec<i64> {
        let ts = valid_from.parse::<chrono::NaiveDateTime>().unwrap();
        let data = slots
            .iter()
            .enumerate()
            .map(|(idx, (k, v))| {
                (
                    schema::contract_storage::slot.eq(hex::decode(format!(
                        "{:064x}",
                        U256::from(*k)
                    ))
                    .unwrap()),
                    schema::contract_storage::value.eq(hex::decode(format!(
                        "{:064x}",
                        U256::from(*v)
                    ))
                    .unwrap()),
                    schema::contract_storage::contract_id.eq(contract_id),
                    schema::contract_storage::modify_tx.eq(modify_tx),
                    schema::contract_storage::valid_from.eq(ts),
                    schema::contract_storage::ordinal.eq(idx as i64),
                )
            })
            .collect::<Vec<_>>();

        diesel::insert_into(schema::contract_storage::table)
            .values(&data)
            .returning(schema::contract_storage::id)
            .get_results(conn)
            .await
            .unwrap()
    }
}
