use crate::{
    models::{
        contract::ContractDelta,
        protocol::{ComponentBalance, ProtocolComponent, ProtocolComponentStateDelta},
        Chain, ComponentId,
    },
    Bytes,
};
use chrono::NaiveDateTime;
use std::collections::HashMap;

#[derive(Clone, Default, PartialEq, Debug)]
pub struct Block {
    pub hash: Bytes,
    pub parent_hash: Bytes,
    pub number: u64,
    pub chain: Chain,
    pub ts: NaiveDateTime,
}

#[derive(Clone, Default, PartialEq, Debug)]
pub struct Transaction {
    pub hash: Bytes,
    pub block_hash: Bytes,
    pub from: Bytes,
    pub to: Option<Bytes>,
    pub index: u64,
}

pub struct BlockTransactionDeltas<T> {
    pub extractor: String,
    pub chain: Chain,
    pub block: Block,
    pub revert: bool,
    pub deltas: Vec<TransactionDeltaGroup<T>>,
}

#[allow(dead_code)]
pub struct TransactionDeltaGroup<T> {
    changes: T,
    protocol_component: HashMap<String, ProtocolComponent>,
    component_balances: HashMap<String, ComponentBalance>,
    component_tvl: HashMap<String, f64>,
    tx: Transaction,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BlockAggregatedDeltas<T: Clone + std::fmt::Debug + PartialEq> {
    pub extractor: String,
    pub chain: Chain,
    pub block: Block,
    pub revert: bool,
    pub deltas: T,
    pub new_components: HashMap<String, ProtocolComponent>,
    pub deleted_components: HashMap<String, ProtocolComponent>,
    pub component_balances: HashMap<ComponentId, HashMap<Bytes, ComponentBalance>>,
    #[allow(dead_code)]
    component_tvl: HashMap<String, f64>,
}

pub type NativeBlockDeltas = BlockAggregatedDeltas<HashMap<String, ProtocolComponentStateDelta>>;
pub type VmBlockDeltas = BlockAggregatedDeltas<HashMap<Bytes, ContractDelta>>;

impl<T> BlockAggregatedDeltas<T>
where
    T: Clone + std::fmt::Debug + PartialEq,
{
    pub fn new(
        extractor: &str,
        chain: Chain,
        block: Block,
        revert: bool,
        deltas: &T,
        new_components: &HashMap<String, ProtocolComponent>,
        deleted_components: &HashMap<String, ProtocolComponent>,
        component_balances: &HashMap<ComponentId, HashMap<Bytes, ComponentBalance>>,
        component_tvl: &HashMap<String, f64>,
    ) -> Self {
        Self {
            extractor: extractor.to_string(),
            chain,
            block,
            revert,
            deltas: deltas.clone(),
            new_components: new_components.clone(),
            deleted_components: deleted_components.clone(),
            component_balances: component_balances.clone(),
            component_tvl: component_tvl.clone(),
        }
    }
}

pub trait BlockScoped {
    fn block(&self) -> Block;
}

impl<T> BlockScoped for BlockAggregatedDeltas<T>
where
    T: Clone + std::fmt::Debug + PartialEq,
{
    fn block(&self) -> Block {
        self.block.clone()
    }
}
