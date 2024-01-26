use std::collections::HashMap;

use chrono::{NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use strum_macros::{Display, EnumString};
use uuid::Uuid;

use crate::serde_helpers::{hex_bytes, hex_bytes_option, hex_hashmap_key, hex_hashmap_key_value};

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
    BlockAccountChanges(BlockAccountChanges),
    Response(Response),
}

#[derive(Debug, PartialEq, Clone, Deserialize, Serialize, Default)]
pub struct Block {
    pub number: u64,
    #[serde(with = "hex_bytes")]
    pub hash: Vec<u8>,
    #[serde(with = "hex_bytes")]
    pub parent_hash: Vec<u8>,
    pub chain: Chain,
    pub ts: NaiveDateTime,
}

#[derive(Debug, PartialEq, Clone, Default, Deserialize, Serialize)]
pub struct Transaction {
    #[serde(with = "hex_bytes")]
    pub hash: Vec<u8>,
    #[serde(with = "hex_bytes")]
    pub block_hash: Vec<u8>,
    #[serde(with = "hex_bytes")]
    pub from: Vec<u8>,
    #[serde(with = "hex_bytes_option")]
    pub to: Option<Vec<u8>>,
    pub index: u64,
}

impl Transaction {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        hash: Vec<u8>,
        block_hash: Vec<u8>,
        from: Vec<u8>,
        to: Option<Vec<u8>>,
        index: u64,
    ) -> Self {
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
    pub account_updates: HashMap<Vec<u8>, AccountUpdate>,
}

impl BlockAccountChanges {
    pub fn new(
        extractor: String,
        chain: Chain,
        block: Block,
        account_updates: HashMap<Vec<u8>, AccountUpdate>,
    ) -> Self {
        Self { extractor, chain, block, account_updates }
    }
}

#[derive(PartialEq, Serialize, Deserialize, Clone, Debug)]
pub struct AccountUpdate {
    #[serde(with = "hex_bytes")]
    pub address: Vec<u8>,
    pub chain: Chain,
    #[serde(with = "hex_hashmap_key_value")]
    pub slots: HashMap<Vec<u8>, Vec<u8>>,
    #[serde(with = "hex_bytes_option")]
    pub balance: Option<Vec<u8>>,
    #[serde(with = "hex_bytes_option")]
    pub code: Option<Vec<u8>>,
    pub change: ChangeType,
}

impl AccountUpdate {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        address: Vec<u8>,
        chain: Chain,
        slots: HashMap<Vec<u8>, Vec<u8>>,
        balance: Option<Vec<u8>>,
        code: Option<Vec<u8>>,
        change: ChangeType,
    ) -> Self {
        Self { address, chain, slots, balance, code, change }
    }
}

#[derive(Serialize, Debug, Default)]
pub struct StateRequestBody {
    #[serde(rename = "contractIds")]
    pub contract_ids: Option<Vec<ContractId>>,
    #[serde(default = "Version::default")]
    pub version: Version,
}

// TODO: move this to generic
impl StateRequestBody {
    pub fn new(contract_ids: Option<Vec<Vec<u8>>>, version: Version) -> Self {
        Self {
            contract_ids: contract_ids.map(|ids| {
                ids.into_iter()
                    .map(|id| ContractId::new(Chain::Ethereum, id))
                    .collect()
            }),
            version,
        }
    }

    pub fn from_block(block: Block) -> Self {
        Self { contract_ids: None, version: Version { timestamp: block.ts, block: Some(block) } }
    }

    pub fn from_timestamp(timestamp: NaiveDateTime) -> Self {
        Self { contract_ids: None, version: Version { timestamp, block: None } }
    }
}

/// Response from Tycho server for a contract state request.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct StateRequestResponse {
    pub accounts: Vec<ResponseAccount>,
}

impl StateRequestResponse {
    pub fn new(accounts: Vec<ResponseAccount>) -> Self {
        Self { accounts }
    }
}

#[derive(PartialEq, Clone, Serialize, Deserialize, Default)]
#[serde(rename = "Account")]
/// Account struct for the response from Tycho server for a contract state request.
///
/// Code is serialized as a hex string instead of a list of bytes.
pub struct ResponseAccount {
    pub chain: Chain,
    #[serde(with = "hex_bytes")]
    pub address: Vec<u8>,
    pub title: String,
    #[serde(with = "hex_hashmap_key_value")]
    pub slots: HashMap<Vec<u8>, Vec<u8>>,
    #[serde(with = "hex_bytes")]
    pub balance: Vec<u8>,
    #[serde(with = "hex_bytes")]
    pub code: Vec<u8>,
    #[serde(with = "hex_bytes")]
    pub code_hash: Vec<u8>,
    #[serde(with = "hex_bytes")]
    pub balance_modify_tx: Vec<u8>,
    #[serde(with = "hex_bytes")]
    pub code_modify_tx: Vec<u8>,
    #[serde(with = "hex_bytes_option")]
    pub creation_tx: Option<Vec<u8>>,
}

impl ResponseAccount {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        chain: Chain,
        address: Vec<u8>,
        title: String,
        slots: HashMap<Vec<u8>, Vec<u8>>,
        balance: Vec<u8>,
        code: Vec<u8>,
        code_hash: Vec<u8>,
        balance_modify_tx: Vec<u8>,
        code_modify_tx: Vec<u8>,
        creation_tx: Option<Vec<u8>>,
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
    pub address: Vec<u8>,
    pub chain: Chain,
}

/// Uniquely identifies a contract on a specific chain.
impl ContractId {
    pub fn new(chain: Chain, address: Vec<u8>) -> Self {
        Self { address, chain }
    }

    pub fn address(&self) -> &Vec<u8> {
        &self.address
    }
}

impl Display for ContractId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: 0x{}", self.chain, hex::encode(&self.address))
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Version {
    timestamp: NaiveDateTime,
    block: Option<Block>,
}

impl Version {
    pub fn new(timestamp: NaiveDateTime, block: Option<Block>) -> Self {
        Self { timestamp, block }
    }
}

impl Default for Version {
    fn default() -> Self {
        Version { timestamp: Utc::now().naive_utc(), block: None }
    }
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct StateRequestParameters {
    #[serde(default = "Chain::default")]
    chain: Chain,
    tvl_gt: Option<u64>,
    inertia_min_gt: Option<u64>,
}

impl StateRequestParameters {
    pub fn to_query_string(&self) -> String {
        let mut parts = vec![];

        parts.push(format!("chain={}", self.chain));

        if let Some(tvl_gt) = self.tvl_gt {
            parts.push(format!("tvl_gt={}", tvl_gt));
        }

        if let Some(inertia) = self.inertia_min_gt {
            parts.push(format!("inertia_min_gt={}", inertia));
        }

        parts.join("&")
    }
}
