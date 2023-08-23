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
pub mod chain_gateway;

use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use diesel::prelude::*;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use ethers::types::{H160, H256};

use super::{BlockIdentifier, ChainGateway, StorableBlock, StorableTransaction};
use crate::extractor::evm;
use crate::models::Chain;
use crate::storage::schema;
use crate::storage::{orm, StorageError};

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
            use super::schema::chain::dsl::*;
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
