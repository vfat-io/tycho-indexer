use crate::{extractor::evm, storage::ComponentId};
use crate::{models::Chain, storage::ChangeType}; // TODO: Move change type
use chrono::NaiveDateTime;
use ethers::types::{H160, H256};
use std::collections::{HashMap, HashSet};
use tycho_types::Bytes;

#[derive(Clone, Default, PartialEq, Debug)]
pub struct Block {
    pub hash: Bytes,
    pub parent_hash: Bytes,
    pub number: u64,
    pub chain: Chain,
    pub ts: NaiveDateTime,
}

// TODO: this is temporary to remove no of errs
impl From<&evm::Block> for Block {
    fn from(value: &evm::Block) -> Self {
        Self {
            hash: Bytes::from(value.hash),
            parent_hash: Bytes::from(value.parent_hash),
            number: value.number,
            chain: value.chain,
            ts: value.ts,
        }
    }
}

impl From<Block> for evm::Block {
    fn from(value: Block) -> Self {
        Self {
            number: value.number,
            hash: H256::from_slice(&value.hash),
            parent_hash: H256::from_slice(&value.parent_hash),
            chain: value.chain,
            ts: value.ts,
        }
    }
}

#[derive(Clone, Default, PartialEq, Debug)]
pub struct Transaction {
    pub hash: Bytes,
    pub block_hash: Bytes,
    pub from: Bytes,
    pub to: Option<Bytes>,
    pub index: u64,
}

impl From<Transaction> for evm::Transaction {
    fn from(value: Transaction) -> Self {
        Self {
            hash: H256::from_slice(&value.hash),
            block_hash: H256::from_slice(&value.block_hash),
            from: H160::from_slice(&value.from),
            to: value
                .to
                .as_ref()
                .map(|e| H160::from_slice(e)),
            index: value.index,
        }
    }
}

impl From<&evm::Transaction> for Transaction {
    fn from(value: &evm::Transaction) -> Self {
        Self {
            hash: Bytes::from(value.hash),
            block_hash: Bytes::from(value.block_hash),
            from: Bytes::from(value.from),
            to: value.to.map(Bytes::from),
            index: value.index,
        }
    }
}

pub struct ComponentBalance {
    pub token: Bytes,
    pub new_balance: Bytes,
    pub balance_float: f64,
    pub modify_tx: Bytes,
    pub component_id: String,
}

pub struct Contract {
    chain: Chain,
    address: Bytes,
    title: String,
    slots: HashMap<Bytes, Bytes>,
    balance: Bytes,
    code_hash: Bytes,
    balance_modify_tx: Bytes,
    code_modify_tx: Bytes,
    creation_tx: Option<Bytes>,
}

pub struct ContractDelta {
    chain: Chain,
    address: Bytes,
    slots: HashMap<Bytes, Bytes>,
    balance: Option<Bytes>,
    code: Option<Bytes>,
    change: ChangeType,
}

pub struct CurrencyToken {
    pub address: Bytes,
    pub symbol: String,
    pub decimals: u32,
    pub tax: u64,
    pub gas: Vec<Option<u64>>,
    pub chain: Chain,
    /// Quality is between 0-100, where:
    ///  - 100: Normal token
    ///  - 75: Rebase token
    ///  - 50: Fee token
    ///  - 0: Scam token that we shouldn't use
    pub quality: u32,
}

pub struct ProtocolComponent {
    pub id: String,
    pub protocol_system: String,
    pub protocol_type_name: String,
    pub chain: Chain,
    pub tokens: Vec<Bytes>,
    pub contract_ids: Vec<Bytes>,
    pub static_attributes: HashMap<String, Bytes>,
    pub change: ChangeType,
    pub creation_tx: Bytes,
    pub created_at: NaiveDateTime,
}

pub struct ProtocolComponentState {
    pub component_id: String,
    pub attributes: HashMap<String, Bytes>,
    pub modify_tx: Bytes,
}

pub struct ProtocolComponentStateDelta {
    pub component_id: String,
    pub updated_attributes: HashMap<String, Bytes>,
    pub removed_attributes: HashSet<String>,
}

pub struct BlockTransactionDeltas<T> {
    pub extractor: String,
    pub chain: Chain,
    pub block: Block,
    pub revert: bool,
    pub deltas: Vec<TransactionDeltaGroup<T>>,
}

pub struct TransactionDeltaGroup<T> {
    changes: T,
    protocol_component: HashMap<String, ProtocolComponent>,
    component_balances: HashMap<String, ComponentBalance>,
    component_tvl: HashMap<String, f64>,
    tx: Transaction,
}

pub struct BlockAggregatedDeltas<T> {
    pub extractor: String,
    pub chain: Chain,
    pub block: Block,
    pub revert: bool,
    pub deltas: T,
    pub new_components: HashMap<String, ProtocolComponent>,
    pub deleted_components: HashMap<String, ProtocolComponent>,
    pub component_balances: HashMap<ComponentId, HashMap<Bytes, ComponentBalance>>,
    component_tvl: HashMap<String, f64>,
}
