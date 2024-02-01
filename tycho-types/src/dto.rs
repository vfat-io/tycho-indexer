//! Data Transfer Objects (or structs)
//!
//! These structs serve to serialise and deserialize messages between server and client, they should
//! be very simple and ideally not contain any business logic.
//!
//! Structs in here implement utoipa traits so they can be used to derive an OpenAPI schema.
use std::collections::HashMap;

use chrono::{NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use strum_macros::{Display, EnumString};
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

use crate::{
    serde_primitives::{hex_bytes, hex_bytes_option, hex_hashmap_key, hex_hashmap_key_value},
    Bytes,
};

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, EnumString, Display, Default,
)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
pub enum Chain {
    #[default]
    Ethereum,
    Starknet,
    ZkSync,
}

#[derive(Debug, PartialEq, Default, Copy, Clone, Deserialize, Serialize)]
pub enum ChangeType {
    #[default]
    Update,
    Deletion,
    Creation,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct ExtractorIdentity {
    pub chain: Chain,
    pub name: String,
}

impl ExtractorIdentity {
    pub fn new(chain: Chain, name: &str) -> Self {
        Self { chain, name: name.to_owned() }
    }
}

impl std::fmt::Display for ExtractorIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.chain, self.name)
    }
}

/// A command sent from the client to the server
#[derive(Deserialize, Serialize, Debug, PartialEq, Eq)]
#[serde(tag = "method", rename_all = "lowercase")]
pub enum Command {
    Subscribe { extractor_id: ExtractorIdentity },
    Unsubscribe { subscription_id: Uuid },
}

/// A response sent from the server to the client
#[derive(Deserialize, Serialize, Debug, PartialEq, Eq)]
#[serde(tag = "method", rename_all = "lowercase")]
pub enum Response {
    NewSubscription { extractor_id: ExtractorIdentity, subscription_id: Uuid },
    SubscriptionEnded { subscription_id: Uuid },
}

/// A message sent from the server to the client
#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum WebSocketMessage {
    BlockAccountChanges { subscription_id: Uuid, data: BlockAccountChanges },
    Response(Response),
}

#[derive(Debug, PartialEq, Clone, Deserialize, Serialize, Default, ToSchema)]
pub struct Block {
    pub number: u64,
    #[serde(with = "hex_bytes")]
    pub hash: Bytes,
    #[serde(with = "hex_bytes")]
    pub parent_hash: Bytes,
    pub chain: Chain,
    pub ts: NaiveDateTime,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct BlockParam {
    #[schema(value_type=Option<String>)]
    #[serde(with = "hex_bytes_option", default)]
    pub hash: Option<Bytes>,
    #[serde(default)]
    pub chain: Option<Chain>,
    #[serde(default)]
    pub number: Option<i64>,
}

#[derive(Debug, PartialEq, Clone, Default, Deserialize, Serialize)]
pub struct Transaction {
    #[serde(with = "hex_bytes")]
    pub hash: Bytes,
    #[serde(with = "hex_bytes")]
    pub block_hash: Bytes,
    #[serde(with = "hex_bytes")]
    pub from: Bytes,
    #[serde(with = "hex_bytes_option")]
    pub to: Option<Bytes>,
    pub index: u64,
}

impl Transaction {
    #[allow(clippy::too_many_arguments)]
    pub fn new(hash: Bytes, block_hash: Bytes, from: Bytes, to: Option<Bytes>, index: u64) -> Self {
        Self { hash, block_hash, from, to, index }
    }
}

/// A container for account updates grouped by account.
///
/// Hold a single update per account. This is a condensed form of
/// [BlockStateChanges].
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, Default)]
pub struct BlockAccountChanges {
    extractor: String,
    chain: Chain,
    pub block: Block,
    #[serde(with = "hex_hashmap_key")]
    pub account_updates: HashMap<Bytes, AccountUpdate>,
}

impl BlockAccountChanges {
    pub fn new(
        extractor: String,
        chain: Chain,
        block: Block,
        account_updates: HashMap<Bytes, AccountUpdate>,
    ) -> Self {
        Self { extractor, chain, block, account_updates }
    }
}

#[derive(PartialEq, Serialize, Deserialize, Clone, Debug)]
pub struct AccountUpdate {
    #[serde(with = "hex_bytes")]
    pub address: Bytes,
    pub chain: Chain,
    #[serde(with = "hex_hashmap_key_value")]
    pub slots: HashMap<Bytes, Bytes>,
    #[serde(with = "hex_bytes_option")]
    pub balance: Option<Bytes>,
    #[serde(with = "hex_bytes_option")]
    pub code: Option<Bytes>,
    pub change: ChangeType,
}

impl AccountUpdate {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        address: Bytes,
        chain: Chain,
        slots: HashMap<Bytes, Bytes>,
        balance: Option<Bytes>,
        code: Option<Bytes>,
        change: ChangeType,
    ) -> Self {
        Self { address, chain, slots, balance, code, change }
    }
}

#[derive(Serialize, Deserialize, Debug, Default, PartialEq, ToSchema)]
pub struct StateRequestBody {
    #[serde(rename = "contractIds")]
    pub contract_ids: Option<Vec<ContractId>>,
    #[serde(default = "VersionParam::default")]
    pub version: VersionParam,
}

impl StateRequestBody {
    pub fn new(contract_ids: Option<Vec<Bytes>>, version: VersionParam) -> Self {
        Self {
            contract_ids: contract_ids.map(|ids| {
                ids.into_iter()
                    .map(|id| ContractId::new(Chain::Ethereum, id))
                    .collect()
            }),
            version,
        }
    }

    pub fn from_block(block: BlockParam) -> Self {
        Self { contract_ids: None, version: VersionParam { timestamp: None, block: Some(block) } }
    }

    pub fn from_timestamp(timestamp: NaiveDateTime) -> Self {
        Self {
            contract_ids: None,
            version: VersionParam { timestamp: Some(timestamp), block: None },
        }
    }
}

/// Response from Tycho server for a contract state request.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct StateRequestResponse {
    pub accounts: Vec<ResponseAccount>,
}

impl StateRequestResponse {
    pub fn new(accounts: Vec<ResponseAccount>) -> Self {
        Self { accounts }
    }
}

#[derive(PartialEq, Clone, Serialize, Deserialize, Default, ToSchema)]
#[serde(rename = "Account")]
/// Account struct for the response from Tycho server for a contract state request.
///
/// Code is serialized as a hex string instead of a list of bytes.
pub struct ResponseAccount {
    pub chain: Chain,
    #[schema(value_type=String, example="0xc9f2e6ea1637E499406986ac50ddC92401ce1f58")]
    #[serde(with = "hex_bytes")]
    pub address: Bytes,
    #[schema(value_type=String, example="Protocol Vault")]
    pub title: String,
    #[schema(value_type=HashMap<String, String>, example=json!({"0x....": "0x...."}))]
    #[serde(with = "hex_hashmap_key_value")]
    pub slots: HashMap<Bytes, Bytes>,
    #[schema(value_type=HashMap<String, String>, example="0x00")]
    #[serde(with = "hex_bytes")]
    pub balance: Bytes,
    #[schema(value_type=HashMap<String, String>, example="0xBADBABE")]
    #[serde(with = "hex_bytes")]
    pub code: Bytes,
    #[schema(value_type=HashMap<String, String>, example="0x123456789")]
    #[serde(with = "hex_bytes")]
    pub code_hash: Bytes,
    #[schema(value_type=HashMap<String, String>, example="0x8f1133bfb054a23aedfe5d25b1d81b96195396d8b88bd5d4bcf865fc1ae2c3f4")]
    #[serde(with = "hex_bytes")]
    pub balance_modify_tx: Bytes,
    #[schema(value_type=HashMap<String, String>, example="0x8f1133bfb054a23aedfe5d25b1d81b96195396d8b88bd5d4bcf865fc1ae2c3f4")]
    #[serde(with = "hex_bytes")]
    pub code_modify_tx: Bytes,
    #[schema(value_type=HashMap<String, String>, example="0x8f1133bfb054a23aedfe5d25b1d81b96195396d8b88bd5d4bcf865fc1ae2c3f4")]
    #[serde(with = "hex_bytes_option")]
    pub creation_tx: Option<Bytes>,
}

impl ResponseAccount {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        chain: Chain,
        address: Bytes,
        title: String,
        slots: HashMap<Bytes, Bytes>,
        balance: Bytes,
        code: Bytes,
        code_hash: Bytes,
        balance_modify_tx: Bytes,
        code_modify_tx: Bytes,
        creation_tx: Option<Bytes>,
    ) -> Self {
        Self {
            chain,
            address,
            title,
            slots,
            balance,
            code,
            code_hash,
            balance_modify_tx,
            code_modify_tx,
            creation_tx,
        }
    }
}

/// Implement Debug for ResponseAccount manually to avoid printing the code field.
impl std::fmt::Debug for ResponseAccount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResponseAccount")
            .field("chain", &self.chain)
            .field("address", &self.address)
            .field("title", &self.title)
            .field("slots", &self.slots)
            .field("balance", &self.balance)
            .field("code", &format!("[{} bytes]", self.code.len()))
            .field("code_hash", &self.code_hash)
            .field("balance_modify_tx", &self.balance_modify_tx)
            .field("code_modify_tx", &self.code_modify_tx)
            .field("creation_tx", &self.creation_tx)
            .finish()
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct ContractId {
    #[serde(with = "hex_bytes")]
    pub address: Bytes,
    pub chain: Chain,
}

/// Uniquely identifies a contract on a specific chain.
impl ContractId {
    pub fn new(chain: Chain, address: Bytes) -> Self {
        Self { address, chain }
    }

    pub fn address(&self) -> &Bytes {
        &self.address
    }
}

impl Display for ContractId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: 0x{}", self.chain, hex::encode(&self.address))
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct VersionParam {
    pub timestamp: Option<NaiveDateTime>,
    pub block: Option<BlockParam>,
}

impl VersionParam {
    pub fn new(timestamp: Option<NaiveDateTime>, block: Option<BlockParam>) -> Self {
        Self { timestamp, block }
    }
}

impl Default for VersionParam {
    fn default() -> Self {
        VersionParam { timestamp: Some(Utc::now().naive_utc()), block: None }
    }
}

#[derive(Serialize, Deserialize, Default, Debug, IntoParams)]
pub struct StateRequestParameters {
    #[param(default = 0)]
    pub tvl_gt: Option<u64>,
    #[param(default = 0)]
    pub inertia_min_gt: Option<u64>,
}

impl StateRequestParameters {
    pub fn to_query_string(&self) -> String {
        let mut parts = vec![];

        if let Some(tvl_gt) = self.tvl_gt {
            parts.push(format!("tvl_gt={}", tvl_gt));
        }

        if let Some(inertia) = self.inertia_min_gt {
            parts.push(format!("inertia_min_gt={}", inertia));
        }

        let mut res = parts.join("&");
        if !res.is_empty() {
            res = format!("?{res}");
        }
        res
    }
}

#[derive(Serialize, Deserialize, Debug, Default, PartialEq, ToSchema)]
pub struct TokensRequestBody {
    #[serde(rename = "tokenAddresses")]
    #[schema(value_type=Option<Vec<String>>)]
    pub token_addresses: Option<Vec<Bytes>>,
}

impl TokensRequestBody {
    pub fn new(token_addresses: Option<Vec<Bytes>>) -> Self {
        Self { token_addresses }
    }
}

/// Response from Tycho server for a tokens request.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct TokensRequestResponse {
    pub tokens: Vec<ResponseToken>,
}

impl TokensRequestResponse {
    pub fn new(tokens: Vec<ResponseToken>) -> Self {
        Self { tokens }
    }
}

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize, Default, ToSchema)]
#[serde(rename = "Token")]
/// Token struct for the response from Tycho server for a tokens request.
pub struct ResponseToken {
    pub chain: Chain,
    #[schema(value_type=String, example="0xc9f2e6ea1637E499406986ac50ddC92401ce1f58")]
    #[serde(with = "hex_bytes")]
    pub address: Bytes,
    #[schema(value_type=String, example="WETH")]
    pub symbol: String,
    pub decimals: u32,
    pub tax: u64,
    pub gas: Vec<Option<u64>>,
}

impl ResponseToken {
    pub fn new(
        chain: Chain,
        address: Bytes,
        symbol: String,
        decimals: u32,
        tax: u64,
        gas: Vec<Option<u64>>,
    ) -> Self {
        Self { chain, address, symbol, decimals, tax, gas }
    }
}

#[derive(Serialize, Deserialize, Debug, Default, PartialEq, ToSchema)]
pub struct TokensRequestBody {
    #[serde(rename = "tokenAddresses")]
    pub token_addresses: Option<Vec<Bytes>>,
}

impl TokensRequestBody {
    pub fn new(token_addresses: Option<Vec<Bytes>>) -> Self {
        Self { token_addresses }
    }
}

/// Response from Tycho server for a tokens request.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct TokensRequestResponse {
    pub tokens: Vec<ResponseToken>,
}

impl TokensRequestResponse {
    pub fn new(tokens: Vec<ResponseToken>) -> Self {
        Self { tokens }
    }
}

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize, Default, ToSchema)]
#[serde(rename = "Token")]
/// Token struct for the response from Tycho server for a tokens request.
pub struct ResponseToken {
    pub chain: Chain,
    #[schema(value_type=String, example="0xc9f2e6ea1637E499406986ac50ddC92401ce1f58")]
    #[serde(with = "hex_bytes")]
    pub address: Bytes,
    #[schema(value_type=String, example="WETH")]
    pub symbol: String,
    pub decimals: u32,
    pub tax: u64,
    pub gas: Vec<Option<u64>>,
}

impl ResponseToken {
    pub fn new(
        chain: Chain,
        address: Bytes,
        symbol: String,
        decimals: u32,
        tax: u64,
        gas: Vec<Option<u64>>,
    ) -> Self {
        Self { chain, address, symbol, decimals, tax, gas }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse_state_request() {
        let json_str = r#"
        {
            "contractIds": [
                {
                    "address": "0xb4eccE46b8D4e4abFd03C9B806276A6735C9c092",
                    "chain": "ethereum"
                }
            ],
            "version": {
                "timestamp": "2069-01-01T04:20:00",
                "block": {
                    "hash": "0x24101f9cb26cd09425b52da10e8c2f56ede94089a8bbe0f31f1cda5f4daa52c4",
                    "parentHash": "0x8d75152454e60413efe758cc424bfd339897062d7e658f302765eb7b50971815",
                    "number": 213,
                    "chain": "ethereum"
                }
            }
        }
        "#;

        let result: StateRequestBody = serde_json::from_str(json_str).unwrap();

        let contract0 = "b4eccE46b8D4e4abFd03C9B806276A6735C9c092"
            .parse()
            .unwrap();
        let block_hash = "24101f9cb26cd09425b52da10e8c2f56ede94089a8bbe0f31f1cda5f4daa52c4"
            .parse()
            .unwrap();
        let block_number = 213;

        let expected_timestamp =
            NaiveDateTime::parse_from_str("2069-01-01T04:20:00", "%Y-%m-%dT%H:%M:%S").unwrap();

        let expected = StateRequestBody {
            contract_ids: Some(vec![ContractId::new(Chain::Ethereum, contract0)]),
            version: VersionParam {
                timestamp: Some(expected_timestamp),
                block: Some(BlockParam {
                    hash: Some(block_hash),
                    chain: Some(Chain::Ethereum),
                    number: Some(block_number),
                }),
            },
        };

        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_state_request_no_contract_specified() {
        let json_str = r#"
    {
        "version": {
            "timestamp": "2069-01-01T04:20:00",
            "block": {
                "hash": "0x24101f9cb26cd09425b52da10e8c2f56ede94089a8bbe0f31f1cda5f4daa52c4",
                "parentHash": "0x8d75152454e60413efe758cc424bfd339897062d7e658f302765eb7b50971815",
                "number": 213,
                "chain": "ethereum"
            }
        }
    }
    "#;

        let result: StateRequestBody = serde_json::from_str(json_str).unwrap();

        let block_hash = "24101f9cb26cd09425b52da10e8c2f56ede94089a8bbe0f31f1cda5f4daa52c4".into();
        let block_number = 213;
        let expected_timestamp =
            NaiveDateTime::parse_from_str("2069-01-01T04:20:00", "%Y-%m-%dT%H:%M:%S").unwrap();

        let expected = StateRequestBody {
            contract_ids: None,
            version: VersionParam {
                timestamp: Some(expected_timestamp),
                block: Some(BlockParam {
                    hash: Some(block_hash),
                    chain: Some(Chain::Ethereum),
                    number: Some(block_number),
                }),
            },
        };

        assert_eq!(result, expected);
    }
}
