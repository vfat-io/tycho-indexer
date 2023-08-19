use crate::models;

use super::schema::{
    block, chain, contract, contract_balance, contract_code, contract_storage,
    extractor_instance_state, protocol_component, protocol_holds_token, protocol_system,
    protocol_type, token, transaction,
};
use chrono::NaiveDateTime;
use diesel::prelude::*;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_derive_enum::DbEnum;

#[derive(Identifiable, Queryable, Selectable)]
#[diesel(table_name=chain)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Chain {
    pub id: i64,
    pub name: String,
    pub inserted_ts: NaiveDateTime,
    pub modified_ts: NaiveDateTime,
}

#[derive(Identifiable, Queryable, Associations, Selectable)]
#[diesel(belongs_to(Chain))]
#[diesel(table_name=extractor_instance_state)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct ExtractorInstanceState {
    pub id: i64,
    pub name: String,
    pub chain_id: i64,
    pub cursor: Option<Vec<u8>>,
    pub inserted_ts: NaiveDateTime,
    pub modified_ts: NaiveDateTime,
}

#[derive(Identifiable, Queryable, Associations, Selectable)]
#[diesel(belongs_to(Chain))]
#[diesel(table_name=block)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Block {
    pub id: i64,
    pub hash: Vec<u8>,
    pub parent_hash: Vec<u8>,
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
}

#[derive(Insertable)]
#[diesel(table_name=block)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct NewBlock {
    pub hash: Vec<u8>,
    pub parent_hash: Vec<u8>,
    pub chain_id: i64,
    pub main: bool,
    pub number: i64,
    pub ts: NaiveDateTime,
}

#[derive(Identifiable, Queryable, Associations, Selectable, Debug)]
#[diesel(belongs_to(Block))]
#[diesel(table_name=transaction)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Transaction {
    pub id: i64,
    pub hash: Vec<u8>,
    pub block_id: i64,
    pub from: Vec<u8>,
    pub to: Vec<u8>,
    pub index: i64,
    pub inserted_ts: NaiveDateTime,
    pub modified_ts: NaiveDateTime,
}

#[derive(Insertable)]
#[diesel(table_name=transaction)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct NewTransaction {
    pub hash: Vec<u8>,
    pub block_id: i64,
    pub from: Vec<u8>,
    pub to: Vec<u8>,
    pub index: i64,
}

#[derive(Identifiable, Queryable, Selectable)]
#[diesel(table_name=protocol_system)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct ProtocolSystem {
    pub id: i64,
    pub name: String,
    pub inserted_ts: NaiveDateTime,
    pub modified_ts: NaiveDateTime,
}

#[derive(Debug, DbEnum)]
#[ExistingTypePath = "crate::storage::schema::sql_types::FinancialProtocolType"]
pub enum FinancialProtocolType {
    Swap,
    Psm,
    Debt,
    Leverage,
}

#[derive(Debug, DbEnum)]
#[ExistingTypePath = "crate::storage::schema::sql_types::ProtocolImplementationType"]
pub enum ProtocolImplementationType {
    Custom,
    Vm,
}

#[derive(Identifiable, Queryable, Selectable)]
#[diesel(table_name=protocol_type)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct ProtocolType {
    pub id: i64,
    pub name: String,
    pub financial_type: FinancialProtocolType,
    pub attribute_schema: Option<serde_json::Value>,
    pub implementation: ProtocolImplementationType,
    pub inserted_ts: NaiveDateTime,
    pub modified_ts: NaiveDateTime,
}

#[derive(Identifiable, Queryable, Associations, Selectable)]
#[diesel(belongs_to(Chain))]
#[diesel(belongs_to(ProtocolType))]
#[diesel(belongs_to(ProtocolSystem))]
#[diesel(table_name=protocol_component)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct ProtocolComponent {
    pub id: i64,
    pub external_id: String,
    pub chain_id: i64,
    pub protocol_type_id: i64,
    pub protocol_system_id: i64,
    pub attributes: Option<serde_json::Value>,
    pub created_at: NaiveDateTime,
    pub deleted_at: Option<NaiveDateTime>,
    pub inserted_ts: NaiveDateTime,
    pub modified_ts: NaiveDateTime,
}

#[derive(Identifiable, Queryable, Associations, Selectable)]
#[diesel(belongs_to(Chain))]
#[diesel(belongs_to(Transaction, foreign_key = creation_tx))]
#[diesel(table_name=contract)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Contract {
    pub id: i64,
    pub title: String,
    pub address: Vec<u8>,
    pub chain_id: i64,
    pub creation_tx: Option<i64>,
    pub created_at: Option<NaiveDateTime>,
    pub deleted_at: Option<NaiveDateTime>,
    pub inserted_ts: NaiveDateTime,
    pub modified_ts: NaiveDateTime,
}

#[derive(Identifiable, Queryable, Associations, Selectable)]
#[diesel(belongs_to(Contract))]
#[diesel(table_name=token)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Token {
    pub id: i64,
    pub contract_id: i64,
    pub symbol: String,
    pub decimals: i32,
    pub tax: i64,
    pub gas: Vec<Option<i64>>,
    pub inserted_ts: NaiveDateTime,
    pub modified_ts: NaiveDateTime,
}

#[derive(Identifiable, Queryable, Associations, Selectable)]
#[diesel(belongs_to(Contract))]
#[diesel(table_name=contract_balance)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct ContractBalance {
    pub id: i64,
    pub balance: Vec<u8>,
    pub contract_id: i64,
    pub modify_tx: i64,
    pub valid_from: NaiveDateTime,
    pub valid_to: Option<NaiveDateTime>,
    pub inserted_ts: NaiveDateTime,
    pub modified_ts: NaiveDateTime,
}

#[derive(Identifiable, Queryable, Associations, Selectable)]
#[diesel(belongs_to(Contract))]
#[diesel(table_name=contract_code)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct ContractCode {
    pub id: i64,
    pub code: Vec<u8>,
    pub hash: Vec<u8>,
    pub contract_id: i64,
    pub modify_tx: i64,
    pub valid_from: NaiveDateTime,
    pub valid_to: Option<NaiveDateTime>,
    pub inserted_ts: NaiveDateTime,
    pub modified_ts: NaiveDateTime,
}

#[derive(Identifiable, Queryable, Associations, Selectable)]
#[diesel(belongs_to(Contract))]
#[diesel(table_name=contract_storage)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct ContractStorage {
    pub id: i64,
    pub slot: Vec<u8>,
    pub value: Vec<u8>,
    pub contract_id: i64,
    pub modify_tx: i64,
    pub valid_from: NaiveDateTime,
    pub valid_to: Option<NaiveDateTime>,
    pub inserted_ts: NaiveDateTime,
    pub modified_ts: NaiveDateTime,
}

#[derive(Identifiable, Queryable, Associations, Selectable)]
#[diesel(primary_key(protocol_component_id, token_id))]
#[diesel(belongs_to(ProtocolComponent))]
#[diesel(belongs_to(Token))]
#[diesel(table_name=protocol_holds_token)]
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
