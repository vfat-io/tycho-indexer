use crate::models::Chain; // TODO: Move change type
use crate::{
    extractor::evm,
    models::protocol::{ComponentBalance, ProtocolComponent},
    storage::ComponentId,
};
use chrono::NaiveDateTime;
use ethers::types::{H160, H256};
use std::collections::HashMap;
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
