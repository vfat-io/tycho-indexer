use async_trait::async_trait;
use chrono::NaiveDateTime;
use diesel::{dsl::sql, prelude::*, sql_types::Bool};
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use diesel_derive_enum::DbEnum;
use std::collections::{HashMap, HashSet};

use crate::{
    hex_bytes::Bytes,
    models,
    storage::{
        Address, Balance, BlockHash, BlockIdentifier, Code, CodeHash, ContractId, StorageError,
        TxHash,
    },
};

use super::{
    schema::{
        account, account_balance, block, chain, contract_code, contract_storage, extraction_state,
        protocol_component, protocol_holds_token, protocol_state, protocol_system, protocol_type,
        token, transaction,
    },
    versioning::{DeltaVersionedRow, StoredVersionedRow, VersionedRow},
};

#[derive(Identifiable, Queryable, Selectable)]
#[diesel(table_name = chain)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Chain {
    pub id: i64,
    pub name: String,
    pub inserted_ts: NaiveDateTime,
    pub modified_ts: NaiveDateTime,
}

/// Represents the state of an extractor.
///
/// Note that static extraction parameters are usually defined through
/// infrastructure configuration tools (e.g., terraform). This struct only
/// maintains dynamic state that changes during runtime and has to be persisted
/// between restarts.
#[derive(Identifiable, Queryable, Associations, Selectable)]
#[diesel(belongs_to(Chain))]
#[diesel(table_name = extraction_state)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct ExtractionState {
    /// Unique identifier for the extraction state.
    pub id: i64,

    /// Name of the extractor.
    pub name: String,

    // Version of the extractor.
    pub version: String,

    /// Chain identifier that the extractor instance is scoped to.
    pub chain_id: i64,

    /// Last fully extracted cursor for the corresponding substream.
    /// Can be null, indicating no cursor has been extracted yet.
    pub cursor: Option<Vec<u8>>,

    /// Additional attributes that the extractor needs to persist.
    /// Stored as a JSON binary object.
    pub attributes: Option<serde_json::Value>,

    /// Timestamp when this entry was inserted into the table.
    pub inserted_ts: NaiveDateTime,

    /// Timestamp when this entry was last modified.
    pub modified_ts: NaiveDateTime,
}

impl ExtractionState {
    /// Retrieves an `ExtractionState` based on the provided extractor name and chain ID.
    ///
    /// This method performs a join operation with the `chain` table and filters the results
    /// based on the given extractor name and chain ID. It then selects the matching
    /// `ExtractionState` and fetches the first result.
    ///
    /// # Parameters
    /// - `extractor`: The name of the extractor to filter by.
    /// - `chain_id`: The ID of the chain to filter by.
    /// - `conn`: A mutable reference to an asynchronous PostgreSQL connection.
    ///
    /// # Returns
    /// - `Ok(Some(ExtractionState))` if a matching `ExtractionState` is found.
    /// - `Ok(None)` if no matching entry is found in the database.
    /// - `Err(DieselError)` if a Diesel error occurs during the query.
    pub async fn by_name(
        extractor: &str,
        chain_id: i64,
        conn: &mut AsyncPgConnection,
    ) -> QueryResult<Option<ExtractionState>> {
        extraction_state::table
            .inner_join(chain::table)
            .filter(extraction_state::name.eq(extractor))
            .filter(chain::id.eq(chain_id))
            .select(ExtractionState::as_select())
            .first::<ExtractionState>(conn)
            .await
            .optional()
    }
}

#[derive(Insertable)]
#[diesel(table_name = extraction_state)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct NewExtractionState<'a> {
    pub name: &'a str,
    pub version: &'a str,
    pub chain_id: i64,
    pub cursor: Option<&'a [u8]>,
    pub attributes: Option<&'a serde_json::Value>,
    pub modified_ts: NaiveDateTime,
}

#[derive(AsChangeset, Debug)]
#[diesel(table_name = extraction_state)]
pub struct ExtractionStateForm<'a> {
    pub cursor: Option<&'a [u8]>,
    pub attributes: Option<&'a serde_json::Value>,
    pub modified_ts: Option<NaiveDateTime>,
}

#[derive(Identifiable, Queryable, Associations, Selectable)]
#[diesel(belongs_to(Chain))]
#[diesel(table_name = block)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Block {
    pub id: i64,
    pub hash: BlockHash,
    pub parent_hash: BlockHash,
    pub chain_id: i64,
    pub main: bool,
    pub number: i64,
    pub ts: NaiveDateTime,
    pub inserted_ts: NaiveDateTime,
    pub modified_ts: NaiveDateTime,
}

impl Block {
    pub async fn by_number(
        chain: models::Chain,
        number: i64,
        conn: &mut AsyncPgConnection,
    ) -> QueryResult<Block> {
        block::table
            .inner_join(chain::table)
            .filter(block::number.eq(number))
            .filter(chain::name.eq(chain.to_string()))
            .select(Block::as_select())
            .first::<Block>(conn)
            .await
    }

    pub async fn by_hash(block_hash: &[u8], conn: &mut AsyncPgConnection) -> QueryResult<Block> {
        block::table
            .filter(block::hash.eq(block_hash))
            .select(Block::as_select())
            .first::<Block>(conn)
            .await
    }

    pub async fn most_recent(
        chain: models::Chain,
        conn: &mut AsyncPgConnection,
    ) -> QueryResult<Block> {
        block::table
            .inner_join(chain::table)
            .filter(chain::name.eq(chain.to_string()))
            .order(block::number.desc())
            .select(Block::as_select())
            .first::<Block>(conn)
            .await
    }

    pub async fn by_id(id: &BlockIdentifier, conn: &mut AsyncPgConnection) -> QueryResult<Block> {
        match id {
            BlockIdentifier::Hash(hash) => Self::by_hash(hash, conn).await,
            BlockIdentifier::Number((chain, number)) => {
                Self::by_number(*chain, *number, conn).await
            }
            BlockIdentifier::Latest(chain) => Self::most_recent(*chain, conn).await,
        }
    }
}

#[derive(Insertable)]
#[diesel(table_name = block)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct NewBlock {
    pub hash: BlockHash,
    pub parent_hash: BlockHash,
    pub chain_id: i64,
    pub main: bool,
    pub number: i64,
    pub ts: NaiveDateTime,
}

#[derive(Identifiable, Queryable, Associations, Selectable, Debug)]
#[diesel(belongs_to(Block))]
#[diesel(table_name = transaction)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Transaction {
    pub id: i64,
    pub hash: TxHash,
    pub block_id: i64,
    pub from: Address,
    pub to: Address,
    pub index: i64,
    pub inserted_ts: NaiveDateTime,
    pub modified_ts: NaiveDateTime,
}

impl Transaction {
    pub async fn by_hash(hash: &[u8], conn: &mut AsyncPgConnection) -> QueryResult<Self> {
        transaction::table
            .filter(transaction::hash.eq(hash))
            .select(Self::as_select())
            .first::<Self>(conn)
            .await
    }

    pub async fn ids_by_hash(
        hashes: &[TxHash],
        conn: &mut AsyncPgConnection,
    ) -> Result<HashMap<TxHash, i64>, StorageError> {
        use super::schema::transaction::dsl::*;

        let results = transaction
            .filter(hash.eq_any(hashes))
            .select((hash, id))
            .load::<(TxHash, i64)>(conn)
            .await?;

        Ok(results.into_iter().collect())
    }

    // fetches the transaction id, hash, index and block timestamp for a given set of hashes
    pub async fn ids_and_ts_by_hash(
        hashes: &[&TxHash],
        conn: &mut AsyncPgConnection,
    ) -> QueryResult<Vec<(i64, Bytes, i64, NaiveDateTime)>> {
        transaction::table
            .inner_join(block::table)
            .filter(transaction::hash.eq_any(hashes))
            .select((transaction::id, transaction::hash, transaction::index, block::ts))
            .get_results::<(i64, Bytes, i64, NaiveDateTime)>(conn)
            .await
    }
}

#[derive(Insertable)]
#[diesel(table_name = transaction)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct NewTransaction {
    pub hash: TxHash,
    pub block_id: i64,
    pub from: Address,
    pub to: Address,
    pub index: i64,
}

#[derive(Identifiable, Queryable, Selectable)]
#[diesel(table_name = protocol_system)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct ProtocolSystem {
    pub id: i64,
    pub name: String,
    pub inserted_ts: NaiveDateTime,
    pub modified_ts: NaiveDateTime,
}

#[derive(Insertable, Debug)]
#[diesel(table_name=protocol_system)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct NewProtocolSystem {
    pub name: String,
}

#[derive(Debug, DbEnum, Clone, PartialEq)]
#[ExistingTypePath = "crate::storage::postgres::schema::sql_types::FinancialType"]
pub enum FinancialType {
    Swap,
    Psm,
    Debt,
    Leverage,
}

#[derive(Debug, DbEnum, Clone, PartialEq)]
#[ExistingTypePath = "crate::storage::postgres::schema::sql_types::ImplementationType"]
pub enum ImplementationType {
    Custom,
    Vm,
}

#[derive(Identifiable, Queryable, Selectable)]
#[diesel(table_name = protocol_type)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct ProtocolType {
    pub id: i64,
    pub name: String,
    pub financial_type: FinancialType,
    pub attribute_schema: Option<serde_json::Value>,
    pub implementation: ImplementationType,
    pub inserted_ts: NaiveDateTime,
    pub modified_ts: NaiveDateTime,
}

#[derive(AsChangeset, Insertable)]
#[diesel(table_name = protocol_type)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct NewProtocolType {
    pub name: String,
    pub financial_type: FinancialType,
    pub attribute_schema: Option<serde_json::Value>,
    pub implementation: ImplementationType,
}

#[derive(Identifiable, Queryable, Associations, Selectable, Clone, Debug, PartialEq)]
#[diesel(belongs_to(Chain))]
#[diesel(belongs_to(ProtocolType))]
#[diesel(belongs_to(ProtocolSystem))]
#[diesel(belongs_to(Transaction, foreign_key = creation_tx))]
#[diesel(table_name = protocol_component)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct ProtocolComponent {
    pub id: i64,
    pub chain_id: i64,
    pub external_id: String,
    pub protocol_type_id: i64,
    pub protocol_system_id: i64,
    pub attributes: Option<serde_json::Value>,
    pub created_at: NaiveDateTime,
    pub deleted_at: Option<NaiveDateTime>,
    pub inserted_ts: NaiveDateTime,
    pub modified_ts: NaiveDateTime,
    pub creation_tx: i64,
    pub deletion_tx: Option<i64>,
}

#[derive(Insertable, AsChangeset, Debug)]
#[diesel(belongs_to(Chain))]
#[diesel(belongs_to(ProtocolType))]
#[diesel(belongs_to(ProtocolSystem))]
#[diesel(table_name = protocol_component)]
pub struct NewProtocolComponent {
    pub external_id: String,
    pub chain_id: i64,
    pub protocol_type_id: i64,
    pub protocol_system_id: i64,
    pub creation_tx: i64,
    pub created_at: NaiveDateTime,
    pub attributes: Option<serde_json::Value>,
}

impl ProtocolComponent {
    pub async fn ids_by_external_ids(
        external_ids: &[&str],
        conn: &mut AsyncPgConnection,
    ) -> QueryResult<Vec<(i64, String)>> {
        protocol_component::table
            .filter(protocol_component::external_id.eq_any(external_ids))
            .select((protocol_component::id, protocol_component::external_id))
            .get_results::<(i64, String)>(conn)
            .await
    }
}

#[derive(Identifiable, Queryable, Associations, Selectable, Clone, Debug)]
#[diesel(belongs_to(ProtocolComponent))]
#[diesel(table_name = protocol_state)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct ProtocolState {
    pub id: i64,
    pub protocol_component_id: i64,
    pub attribute_name: String,
    pub attribute_value: Bytes,
    pub previous_value: Option<Bytes>,
    pub modify_tx: i64,
    pub valid_from: NaiveDateTime,
    pub valid_to: Option<NaiveDateTime>,
    pub inserted_ts: NaiveDateTime,
    pub modified_ts: NaiveDateTime,
}

impl ProtocolState {
    /// retrieves all matching protocol states along with their linked component ids and transaction
    /// hashes. To get state deltas, provide a start and end timestamp. To get full states, provide
    /// either only an end timestamp or no timestamp (latest state).
    pub async fn by_id(
        component_ids: &[&str],
        chain_id: i64,
        start_ts: Option<NaiveDateTime>,
        end_ts: Option<NaiveDateTime>,
        conn: &mut AsyncPgConnection,
    ) -> QueryResult<Vec<(Self, String, Bytes)>> {
        let mut query = protocol_state::table
            .inner_join(protocol_component::table)
            .inner_join(transaction::table.on(transaction::id.eq(protocol_state::modify_tx)))
            .filter(protocol_component::external_id.eq_any(component_ids))
            .filter(protocol_component::chain_id.eq(chain_id))
            .filter(
                protocol_state::valid_to
                    .gt(end_ts)
                    .or(protocol_state::valid_to.is_null()),
            )
            .into_boxed();

        // if end timestamp is provided, we want to filter by valid_from <= end_ts
        if let Some(ts) = end_ts {
            query = query.filter(protocol_state::valid_from.le(ts));
        }

        // if start timestamp is provided, we want to filter by valid_from > start_ts
        if let Some(ts) = start_ts {
            query = query.filter(protocol_state::valid_from.gt(ts));
        }

        query
            .order_by((protocol_component::external_id, transaction::block_id, transaction::index))
            .select((Self::as_select(), protocol_component::external_id, transaction::hash))
            .get_results::<(Self, String, Bytes)>(conn)
            .await
    }

    /// retrieves all matching protocol states along with their linked component ids and transaction
    /// hashes. To get state deltas, provide a start and end timestamp. To get full states, provide
    /// either only an end timestamp or no timestamp (latest state).
    pub async fn by_protocol_system(
        system: String,
        chain_id: i64,
        start_ts: Option<NaiveDateTime>,
        end_ts: Option<NaiveDateTime>,
        conn: &mut AsyncPgConnection,
    ) -> QueryResult<Vec<(Self, String, Bytes)>> {
        let mut query = protocol_state::table
            .inner_join(protocol_component::table)
            .inner_join(
                protocol_system::table
                    .on(protocol_component::protocol_system_id.eq(protocol_system::id)),
            )
            .inner_join(transaction::table.on(transaction::id.eq(protocol_state::modify_tx)))
            .filter(protocol_system::name.eq(system.to_string()))
            .filter(protocol_component::chain_id.eq(chain_id))
            .filter(
                protocol_state::valid_to
                    .gt(end_ts)
                    .or(protocol_state::valid_to.is_null()),
            )
            .into_boxed();

        // if end timestamp is provided, we want to filter by valid_from <= end_ts
        if let Some(ts) = end_ts {
            query = query.filter(protocol_state::valid_from.le(ts));
        }

        // if start timestamp is provided, we want to filter by valid_from > start_ts
        if let Some(ts) = start_ts {
            query = query.filter(protocol_state::valid_from.gt(ts));
        }

        query
            .order_by((
                protocol_state::protocol_component_id,
                transaction::block_id,
                transaction::index,
            ))
            .select((Self::as_select(), protocol_component::external_id, transaction::hash))
            .get_results::<(Self, String, Bytes)>(conn)
            .await
    }

    /// retrieves all matching protocol states along with their linked component ids and transaction
    /// hashes. To get state deltas, provide a start and end timestamp. To get full states, provide
    /// either only an end timestamp or no timestamp (latest state).
    pub async fn by_chain(
        chain_id: i64,
        start_ts: Option<NaiveDateTime>,
        end_ts: Option<NaiveDateTime>,
        conn: &mut AsyncPgConnection,
    ) -> QueryResult<Vec<(Self, String, Bytes)>> {
        let mut query = protocol_state::table
            .inner_join(protocol_component::table)
            .inner_join(transaction::table.on(transaction::id.eq(protocol_state::modify_tx)))
            .filter(protocol_component::chain_id.eq(chain_id))
            .filter(
                protocol_state::valid_to
                    .gt(end_ts)
                    .or(protocol_state::valid_to.is_null()),
            )
            .into_boxed();

        // if end timestamp is provided, we want to filter by valid_from <= end_ts
        if let Some(ts) = end_ts {
            query = query.filter(protocol_state::valid_from.le(ts));
        }

        // if start timestamp is provided, we want to filter by valid_from > start_ts
        if let Some(ts) = start_ts {
            query = query.filter(protocol_state::valid_from.gt(ts));
        }

        query
            .order_by((
                protocol_state::protocol_component_id,
                transaction::block_id,
                transaction::index,
            ))
            .select((Self::as_select(), protocol_component::external_id, transaction::hash))
            .get_results::<(Self, String, Bytes)>(conn)
            .await
    }

    /// retrieves all matching protocol states where their attributes have been deleted
    pub async fn deleted_by_id(
        component_ids: &[&str],
        chain_id: i64,
        start_ts: NaiveDateTime,
        end_ts: NaiveDateTime,
        conn: &mut AsyncPgConnection,
    ) -> QueryResult<Vec<(Self, String, Bytes)>> {
        let mut query = protocol_state::table
            .inner_join(protocol_component::table)
            .inner_join(transaction::table.on(transaction::id.eq(protocol_state::modify_tx)))
            .filter(protocol_component::external_id.eq_any(component_ids))
            .filter(protocol_component::chain_id.eq(chain_id))
            .filter(
                protocol_state::valid_to
                    .le(end_ts)
                    .and(protocol_state::valid_to.ge(start_ts)),
            )
            .into_boxed();

        // Subquery to exclude entities that have a valid at version at end_ts
        let sub_query = format!("NOT EXISTS (
                                SELECT 1 FROM protocol_state ps2
                                WHERE ps2.protocol_component_id = protocol_state.protocol_component_id
                                AND ps2.attribute_name = protocol_state.attribute_name
                                AND ps2.valid_from <= '{}'
                                AND (ps2.valid_to > '{}' OR ps2.valid_to IS NULL)
                            )", end_ts, end_ts);
        query = query.filter(sql::<Bool>(&sub_query));

        query
            .order_by((
                protocol_state::protocol_component_id,
                transaction::block_id,
                transaction::index,
            ))
            .select((Self::as_select(), protocol_component::external_id, transaction::hash))
            .get_results::<(Self, String, Bytes)>(conn)
            .await
    }

    /// retrieves all matching protocol states where their attributes have been deleted
    pub async fn deleted_by_protocol_system(
        system: String,
        chain_id: i64,
        start_ts: NaiveDateTime,
        end_ts: NaiveDateTime,
        conn: &mut AsyncPgConnection,
    ) -> QueryResult<Vec<(Self, String, Bytes)>> {
        let mut query = protocol_state::table
            .inner_join(protocol_component::table)
            .inner_join(
                protocol_system::table
                    .on(protocol_component::protocol_system_id.eq(protocol_system::id)),
            )
            .inner_join(transaction::table.on(transaction::id.eq(protocol_state::modify_tx)))
            .filter(protocol_system::name.eq(system.to_string()))
            .filter(protocol_component::chain_id.eq(chain_id))
            .filter(
                protocol_state::valid_to
                    .le(end_ts)
                    .and(protocol_state::valid_to.ge(start_ts)),
            )
            .into_boxed();

        // Subquery to exclude entities that have a valid at version at end_ts
        let sub_query = format!("NOT EXISTS (
                                SELECT 1 FROM protocol_state ps2
                                WHERE ps2.protocol_component_id = protocol_state.protocol_component_id
                                AND ps2.attribute_name = protocol_state.attribute_name
                                AND ps2.valid_from <= '{}'
                                AND (ps2.valid_to > '{}' OR ps2.valid_to IS NULL)
                            )", end_ts, end_ts);
        query = query.filter(sql::<Bool>(&sub_query));

        query
            .order_by((
                protocol_state::protocol_component_id,
                transaction::block_id,
                transaction::index,
            ))
            .select((Self::as_select(), protocol_component::external_id, transaction::hash))
            .get_results::<(Self, String, Bytes)>(conn)
            .await
    }

    /// retrieves all matching protocol states where their attributes have been deleted
    pub async fn deleted_by_chain(
        chain_id: i64,
        start_ts: NaiveDateTime,
        end_ts: NaiveDateTime,
        conn: &mut AsyncPgConnection,
    ) -> QueryResult<Vec<(Self, String, Bytes)>> {
        let mut query = protocol_state::table
            .inner_join(protocol_component::table)
            .inner_join(transaction::table.on(transaction::id.eq(protocol_state::modify_tx)))
            .filter(protocol_component::chain_id.eq(chain_id))
            .filter(
                protocol_state::valid_to
                    .le(end_ts)
                    .and(protocol_state::valid_to.ge(start_ts)),
            )
            .into_boxed();

        // Subquery to exclude entities that have a valid at version at end_ts
        let sub_query = format!("NOT EXISTS (
                                SELECT 1 FROM protocol_state ps2
                                WHERE ps2.protocol_component_id = protocol_state.protocol_component_id
                                AND ps2.attribute_name = protocol_state.attribute_name
                                AND ps2.valid_from <= '{}'
                                AND (ps2.valid_to > '{}' OR ps2.valid_to IS NULL)
                            )", end_ts, end_ts);
        query = query.filter(sql::<Bool>(&sub_query));

        query
            .order_by((
                protocol_state::protocol_component_id,
                transaction::block_id,
                transaction::index,
            ))
            .select((Self::as_select(), protocol_component::external_id, transaction::hash))
            .get_results::<(Self, String, Bytes)>(conn)
            .await
    }

    // retrieves old values for matching protocol states
    pub async fn reverted_by_id(
        component_ids: &[&str],
        chain_id: i64,
        start_ts: NaiveDateTime,
        end_ts: NaiveDateTime,
        conn: &mut AsyncPgConnection,
    ) -> QueryResult<Vec<(String, String, Option<Bytes>)>> {
        // We query all states that were added between the start and end timestamps, filtered by
        // component id. We then group it by component and attribute and order it by tx,
        // then we deduplicate by taking the first row per group. This gives us the first
        // state update for each component-attribute pair. Finally, we return the component id,
        // attribute name and previous value for each component-attribute pair.
        protocol_state::table
            .inner_join(
                protocol_component::table
                    .on(protocol_component::id.eq(protocol_state::protocol_component_id)),
            )
            .inner_join(transaction::table.on(transaction::id.eq(protocol_state::modify_tx)))
            .filter(protocol_component::external_id.eq_any(component_ids))
            .filter(protocol_component::chain_id.eq(chain_id))
            .filter(protocol_state::valid_from.gt(end_ts))
            .filter(protocol_state::valid_from.le(start_ts))
            .order_by((
                protocol_state::protocol_component_id,
                protocol_state::attribute_name,
                transaction::block_id,
                transaction::index,
            ))
            .select((
                protocol_component::external_id,
                protocol_state::attribute_name,
                protocol_state::previous_value,
            ))
            .distinct_on((protocol_state::protocol_component_id, protocol_state::attribute_name))
            .get_results::<(String, String, Option<Bytes>)>(conn)
            .await
    }

    // retrieves old values for matching protocol states
    pub async fn reverted_by_system(
        system: String,
        chain_id: i64,
        start_ts: NaiveDateTime,
        end_ts: NaiveDateTime,
        conn: &mut AsyncPgConnection,
    ) -> QueryResult<Vec<(String, String, Option<Bytes>)>> {
        // We query all states that were added between the start and end timestamps, filtered by
        // system. We then group it by component and attribute and order it by tx,
        // then we deduplicate by taking the first row per group. This gives us the first
        // state update for each component-attribute pair. Finally, we return the component id,
        // attribute name and previous value for each component-attribute pair.
        protocol_state::table
            .inner_join(protocol_component::table)
            .inner_join(
                protocol_system::table
                    .on(protocol_component::protocol_system_id.eq(protocol_system::id)),
            )
            .inner_join(transaction::table.on(transaction::id.eq(protocol_state::modify_tx)))
            .filter(protocol_system::name.eq(system.to_string()))
            .filter(protocol_component::chain_id.eq(chain_id))
            .filter(protocol_state::valid_from.gt(end_ts))
            .filter(protocol_state::valid_from.le(start_ts))
            .order_by((
                protocol_state::protocol_component_id,
                protocol_state::attribute_name,
                transaction::block_id,
                transaction::index,
            ))
            .select((
                protocol_component::external_id,
                protocol_state::attribute_name,
                protocol_state::previous_value,
            ))
            .distinct_on((protocol_state::protocol_component_id, protocol_state::attribute_name))
            .get_results::<(String, String, Option<Bytes>)>(conn)
            .await
    }

    // retrieves old values for matching protocol states
    pub async fn reverted_by_chain(
        chain_id: i64,
        start_ts: NaiveDateTime,
        end_ts: NaiveDateTime,
        conn: &mut AsyncPgConnection,
    ) -> QueryResult<Vec<(String, String, Option<Bytes>)>> {
        // We query all states that were added between the start and end timestamps, filtered by
        // chain. We then group it by component and attribute and order it by tx,
        // then we deduplicate by taking the first row per group. This gives us the first
        // state update for each component-attribute pair. Finally, we return the component id,
        // attribute name and previous value for each component-attribute pair.
        protocol_state::table
            .inner_join(protocol_component::table)
            .inner_join(transaction::table.on(transaction::id.eq(protocol_state::modify_tx)))
            .filter(protocol_component::chain_id.eq(chain_id))
            .filter(protocol_state::valid_from.gt(end_ts))
            .filter(protocol_state::valid_from.le(start_ts))
            .order_by((
                protocol_state::protocol_component_id,
                protocol_state::attribute_name,
                transaction::block_id,
                transaction::index,
            ))
            .select((
                protocol_component::external_id,
                protocol_state::attribute_name,
                protocol_state::previous_value,
            ))
            .distinct_on((protocol_state::protocol_component_id, protocol_state::attribute_name))
            .get_results::<(String, String, Option<Bytes>)>(conn)
            .await
    }
}

#[derive(Insertable, Clone)]
#[diesel(table_name = protocol_state)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct NewProtocolState {
    pub protocol_component_id: i64,
    pub attribute_name: Option<String>,
    pub attribute_value: Option<Bytes>,
    pub previous_value: Option<Bytes>,
    pub modify_tx: i64,
    pub valid_from: NaiveDateTime,
    pub valid_to: Option<NaiveDateTime>,
}

#[derive(Identifiable, Queryable, Associations, Selectable, Debug, PartialEq)]
#[diesel(belongs_to(Chain))]
#[diesel(belongs_to(Transaction, foreign_key = creation_tx))]
#[diesel(table_name = account)]
#[diesel(check_for_backend(diesel::pg::Pg))]
/// Represents an account on a blockchain.
///
/// An `Account` is identified by its blockchain (`Chain`) and address (`H160`). It may have a
/// descriptive `title` and contains information about storage slots, balance, associated code
/// (bytecode), code hash, and transaction hashes related to balance modification, code
/// modification, and optional creation. Additional information about accounts.
/// - A Contract is also an Account, but an Account is not necessarily a Contract.
/// - An account is considered a contract if it has associated code.
pub struct Account {
    pub id: i64,
    pub title: String,
    pub address: Address,
    pub chain_id: i64,
    pub creation_tx: Option<i64>,
    pub created_at: Option<NaiveDateTime>,
    pub deleted_at: Option<NaiveDateTime>,
    pub deletion_tx: Option<i64>,
    pub inserted_ts: NaiveDateTime,
    pub modified_ts: NaiveDateTime,
}

impl Account {
    pub async fn by_id(
        account_id: &ContractId,
        conn: &mut AsyncPgConnection,
    ) -> QueryResult<Account> {
        account::table
            .inner_join(chain::table)
            .filter(account::address.eq(&account_id.address))
            .filter(chain::name.eq(account_id.chain.to_string()))
            .select(Account::as_select())
            .first::<Account>(conn)
            .await
    }

    /// retrieves a account by address
    pub async fn by_address(
        address: &[u8],
        conn: &mut AsyncPgConnection,
    ) -> QueryResult<Vec<Self>> {
        account::table
            .filter(account::address.eq(address))
            .select(Self::as_select())
            .get_results::<Self>(conn)
            .await
    }

    pub async fn get_addresses_by_id(
        ids: impl Iterator<Item = &i64>,
        conn: &mut AsyncPgConnection,
    ) -> QueryResult<Vec<(i64, Address)>> {
        account::table
            .filter(account::id.eq_any(ids))
            .select((account::id, account::address))
            .get_results::<(i64, Address)>(conn)
            .await
    }
}

#[derive(AsChangeset, Insertable, Debug)]
#[diesel(table_name = account)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct NewAccount<'a> {
    pub title: &'a str,
    pub address: &'a [u8],
    pub chain_id: i64,
    pub creation_tx: Option<i64>,
    pub created_at: Option<NaiveDateTime>,
    pub deleted_at: Option<NaiveDateTime>,
}

#[derive(Identifiable, Queryable, Associations, Selectable, Debug, PartialEq)]
#[diesel(belongs_to(Account))]
#[diesel(table_name = token)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Token {
    pub id: i64,
    pub account_id: i64,
    pub symbol: String,
    pub decimals: i32,
    pub tax: i64,
    pub gas: Vec<Option<i64>>,
    pub inserted_ts: NaiveDateTime,
    pub modified_ts: NaiveDateTime,
}

#[derive(AsChangeset, Insertable, Debug)]
#[diesel(table_name = token)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct NewToken {
    pub account_id: i64,
    pub symbol: String,
    pub decimals: i32,
    pub tax: i64,
    pub gas: Vec<Option<i64>>,
}

#[derive(Identifiable, Queryable, Associations, Selectable, Debug)]
#[diesel(belongs_to(Account))]
#[diesel(table_name = account_balance)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct AccountBalance {
    pub id: i64,
    pub balance: Balance,
    pub account_id: i64,
    pub modify_tx: i64,
    pub valid_from: NaiveDateTime,
    pub valid_to: Option<NaiveDateTime>,
    pub inserted_ts: NaiveDateTime,
    pub modified_ts: NaiveDateTime,
}

impl AccountBalance {
    /// retrieves all balances from a certain account
    pub async fn all_versions(
        address: &Address,
        conn: &mut AsyncPgConnection,
    ) -> QueryResult<Vec<Self>> {
        account_balance::table
            .inner_join(account::table)
            .filter(account::address.eq(address))
            .select(Self::as_select())
            .get_results::<Self>(conn)
            .await
    }
}

#[async_trait]
impl StoredVersionedRow for AccountBalance {
    type EntityId = i64;
    type PrimaryKey = i64;
    type Version = NaiveDateTime;

    fn get_pk(&self) -> Self::PrimaryKey {
        self.id
    }

    fn get_valid_to(&self) -> Self::Version {
        self.valid_to.expect("valid to set")
    }

    fn get_entity_id(&self) -> Self::EntityId {
        self.account_id
    }

    async fn latest_versions_by_ids<I: IntoIterator<Item = Self::EntityId> + Send + Sync>(
        ids: I,
        conn: &mut AsyncPgConnection,
    ) -> Result<Vec<Box<Self>>, StorageError> {
        Ok(account_balance::table
            .filter(account_balance::account_id.eq_any(ids))
            .select(Self::as_select())
            .get_results::<Self>(conn)
            .await?
            .into_iter()
            .map(Box::new)
            .collect())
    }

    fn table_name() -> &'static str {
        "account_balance"
    }
}

#[derive(Insertable, Debug)]
#[diesel(table_name = account_balance)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct NewAccountBalance {
    pub balance: Balance,
    pub account_id: i64,
    pub modify_tx: i64,
    pub valid_from: NaiveDateTime,
    pub valid_to: Option<NaiveDateTime>,
}

impl VersionedRow for NewAccountBalance {
    type SortKey = (i64, NaiveDateTime, i64);
    type EntityId = i64;
    type Version = NaiveDateTime;

    fn get_entity_id(&self) -> Self::EntityId {
        self.account_id
    }

    fn get_sort_key(&self) -> Self::SortKey {
        (self.account_id, self.valid_from, self.modify_tx)
    }

    fn set_valid_to(&mut self, end_version: Self::Version) {
        self.valid_to = Some(end_version);
    }

    fn get_valid_from(&self) -> Self::Version {
        self.valid_from
    }
}

#[derive(Identifiable, Queryable, Associations, Selectable, Debug)]
#[diesel(belongs_to(Account))]
#[diesel(table_name = contract_code)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct ContractCode {
    pub id: i64,
    pub code: Code,
    pub hash: CodeHash,
    pub account_id: i64,
    pub modify_tx: i64,
    pub valid_from: NaiveDateTime,
    pub valid_to: Option<NaiveDateTime>,
    pub inserted_ts: NaiveDateTime,
    pub modified_ts: NaiveDateTime,
}

impl ContractCode {
    /// retrieves all codes from a certain account
    pub async fn all_versions(
        address: &[u8],
        conn: &mut AsyncPgConnection,
    ) -> QueryResult<Vec<Self>> {
        contract_code::table
            .inner_join(account::table)
            .filter(account::address.eq(address))
            .select(Self::as_select())
            .get_results::<Self>(conn)
            .await
    }
}

#[async_trait]
impl StoredVersionedRow for ContractCode {
    type EntityId = i64;
    type PrimaryKey = i64;
    type Version = NaiveDateTime;

    fn get_pk(&self) -> Self::PrimaryKey {
        self.id
    }

    fn get_valid_to(&self) -> Self::Version {
        self.valid_to.expect("valid to set")
    }

    fn get_entity_id(&self) -> Self::EntityId {
        self.account_id
    }

    async fn latest_versions_by_ids<I: IntoIterator<Item = Self::EntityId> + Send + Sync>(
        ids: I,
        conn: &mut AsyncPgConnection,
    ) -> Result<Vec<Box<Self>>, StorageError> {
        Ok(contract_code::table
            .filter(contract_code::account_id.eq_any(ids))
            .select(Self::as_select())
            .get_results::<Self>(conn)
            .await?
            .into_iter()
            .map(Box::new)
            .collect())
    }

    fn table_name() -> &'static str {
        "contract_code"
    }
}

#[derive(Insertable, Debug)]
#[diesel(table_name = contract_code)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct NewContractCode<'a> {
    pub code: &'a Code,
    pub hash: CodeHash,
    pub account_id: i64,
    pub modify_tx: i64,
    pub valid_from: NaiveDateTime,
    pub valid_to: Option<NaiveDateTime>,
}

impl<'a> VersionedRow for NewContractCode<'a> {
    type SortKey = (i64, NaiveDateTime, i64);
    type EntityId = i64;
    type Version = NaiveDateTime;

    fn get_entity_id(&self) -> Self::EntityId {
        self.account_id
    }

    fn get_sort_key(&self) -> Self::SortKey {
        (self.account_id, self.valid_from, self.modify_tx)
    }

    fn set_valid_to(&mut self, end_version: Self::Version) {
        self.valid_to = Some(end_version);
    }

    fn get_valid_from(&self) -> Self::Version {
        self.valid_from
    }
}

// theoretically this struct could also simply reference the original struct.
// Unfortunately that really doesn't play nicely with async_trait on the Gateway
// and makes the types a lot more complicted. Once the system is up and running
// this could be improved though.
pub struct NewContract {
    pub title: String,
    pub address: Address,
    pub chain_id: i64,
    pub creation_tx: Option<i64>,
    pub created_at: Option<NaiveDateTime>,
    pub deleted_at: Option<NaiveDateTime>,
    pub balance: Balance,
    pub code: Code,
    pub code_hash: CodeHash,
}

impl NewContract {
    pub fn new_account(&self) -> NewAccount {
        NewAccount {
            title: &self.title,
            address: &self.address,
            chain_id: self.chain_id,
            creation_tx: self.creation_tx,
            created_at: self.created_at,
            deleted_at: None,
        }
    }
    pub fn new_balance(
        &self,
        account_id: i64,
        modify_tx: i64,
        modify_ts: NaiveDateTime,
    ) -> NewAccountBalance {
        NewAccountBalance {
            balance: self.balance.clone(),
            account_id,
            modify_tx,
            valid_from: modify_ts,
            valid_to: None,
        }
    }
    pub fn new_code(
        &self,
        account_id: i64,
        modify_tx: i64,
        modify_ts: NaiveDateTime,
    ) -> NewContractCode {
        NewContractCode {
            code: &self.code,
            hash: self.code_hash.clone(),
            account_id,
            modify_tx,
            valid_from: modify_ts,
            valid_to: None,
        }
    }
}

#[derive(Identifiable, Queryable, Associations, Selectable, Debug)]
#[diesel(belongs_to(Account))]
#[diesel(table_name = contract_storage)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct ContractStorage {
    pub id: i64,
    pub slot: Bytes,
    pub value: Option<Bytes>,
    pub previous_value: Option<Bytes>,
    pub account_id: i64,
    pub modify_tx: i64,
    pub ordinal: i64,
    pub valid_from: NaiveDateTime,
    pub valid_to: Option<NaiveDateTime>,
    pub inserted_ts: NaiveDateTime,
    pub modified_ts: NaiveDateTime,
}

#[async_trait]
impl StoredVersionedRow for ContractStorage {
    type EntityId = (i64, Bytes);
    type PrimaryKey = i64;
    type Version = NaiveDateTime;

    fn get_pk(&self) -> Self::PrimaryKey {
        self.id
    }

    fn get_valid_to(&self) -> Self::Version {
        self.valid_to.expect("valid_to is set")
    }

    fn get_entity_id(&self) -> Self::EntityId {
        (self.account_id, self.slot.clone())
    }

    // Clippy false positive
    #[allow(clippy::mutable_key_type)]
    async fn latest_versions_by_ids<I: IntoIterator<Item = Self::EntityId> + Send + Sync>(
        ids: I,
        conn: &mut AsyncPgConnection,
    ) -> Result<Vec<Box<Self>>, StorageError> {
        let (accounts, slots): (Vec<_>, Vec<_>) = ids.into_iter().unzip();
        let tuple_ids = accounts
            .iter()
            .zip(slots.iter())
            .collect::<HashSet<_>>();
        Ok(contract_storage::table
            .select(ContractStorage::as_select())
            .into_boxed()
            .filter(
                contract_storage::account_id
                    .eq_any(&accounts)
                    .and(contract_storage::slot.eq_any(&slots))
                    .and(contract_storage::valid_to.is_null()),
            )
            .get_results(conn)
            .await?
            .into_iter()
            .filter(|cs| tuple_ids.contains(&(&cs.account_id, &cs.slot)))
            .map(Box::new)
            .collect())
    }

    fn table_name() -> &'static str {
        "contract_storage"
    }
}

#[derive(Insertable, Debug)]
#[diesel(table_name = contract_storage)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct NewSlot<'a> {
    pub slot: &'a Bytes,
    pub value: Option<&'a Bytes>,
    pub previous_value: Option<&'a Bytes>,
    pub account_id: i64,
    pub modify_tx: i64,
    pub ordinal: i64,
    pub valid_from: NaiveDateTime,
    pub valid_to: Option<NaiveDateTime>,
}

impl<'a> VersionedRow for NewSlot<'a> {
    type EntityId = (i64, Bytes);
    type SortKey = ((i64, Bytes), NaiveDateTime, i64);
    type Version = NaiveDateTime;

    fn get_entity_id(&self) -> Self::EntityId {
        (self.account_id, self.slot.clone())
    }

    fn get_sort_key(&self) -> Self::SortKey {
        (self.get_entity_id(), self.valid_from, self.ordinal)
    }

    fn set_valid_to(&mut self, end_version: Self::Version) {
        self.valid_to = Some(end_version);
    }

    fn get_valid_from(&self) -> Self::Version {
        self.valid_from
    }
}

impl<'a> DeltaVersionedRow for NewSlot<'a> {
    type Value = Option<&'a Bytes>;

    fn get_value(&self) -> Self::Value {
        self.value
    }

    fn set_previous_value(&mut self, previous_value: Self::Value) {
        self.previous_value = previous_value
    }
}

pub struct Contract {
    pub account: Account,
    pub balance: AccountBalance,
    pub code: ContractCode,
}

#[derive(Identifiable, Queryable, Associations, Selectable)]
#[diesel(primary_key(protocol_component_id, token_id))]
#[diesel(belongs_to(ProtocolComponent))]
#[diesel(belongs_to(Token))]
#[diesel(table_name = protocol_holds_token)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct ProtocolHoldsToken {
    protocol_component_id: i64,
    token_id: i64,
    pub inserted_ts: NaiveDateTime,
    pub modified_ts: NaiveDateTime,
}
/*
pub fn get_tokens(protocol: &ProtocolComponent, conn: &mut PgConnection) -> Vec<Token> {
    let token_ids = ProtocolHoldsToken::belonging_to(protocol)
        .select(protocol_holds_token::token_id)
        .distinct();
    token::table
        .filter(token::id.eq_any(token_ids))
        .load::<Token>(conn)
        .expect("Could not load tokens")
}

pub struct ProtocolComponentWithToken {
    pub protocol: ProtocolComponent,
    pub tokens: Vec<Token>,
}

pub fn add_tokens(
    protocols: Vec<ProtocolComponent>,
    conn: &mut PgConnection,
) -> Result<Vec<ProtocolComponentWithToken>, Box<dyn Error + Send + Sync>> {
    let tokens: Vec<(ProtocolHoldsToken, Token)> = ProtocolHoldsToken::belonging_to(&protocols)
        .inner_join(token::table)
        .select((ProtocolHoldsToken::as_select(), Token::as_select()))
        .load(conn)?;

    let res = tokens
        .grouped_by(&protocols)
        .into_iter()
        .zip(protocols)
        .map(|(t, p)| ProtocolComponentWithToken {
            protocol: p,
            tokens: t.into_iter().map(|(_, tok)| tok).collect(),
        })
        .collect();
    Ok(res)
}
 */
