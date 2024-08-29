use crate::{
    models::{
        contract::AccountUpdate,
        protocol::{ComponentBalance, ProtocolComponent, ProtocolComponentStateDelta},
        Chain, ComponentId,
    },
    Bytes,
};
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use std::{any::Any, collections::HashMap, sync::Arc};

use super::{token::CurrencyToken, Address, ExtractorIdentity, NormalisedMessage};

#[derive(Clone, Default, PartialEq, Serialize, Deserialize, Debug)]
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

impl Transaction {
    pub fn new(hash: Bytes, block_hash: Bytes, from: Bytes, to: Option<Bytes>, index: u64) -> Self {
        Transaction { hash, block_hash, from, to, index }
    }
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

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
pub struct AggregatedBlockChanges {
    pub extractor: String,
    pub chain: Chain,
    pub block: Block,
    pub finalized_block_height: u64,
    pub revert: bool,
    pub state_updates: HashMap<String, ProtocolComponentStateDelta>,
    pub account_updates: HashMap<Bytes, AccountUpdate>,
    pub new_tokens: HashMap<Address, CurrencyToken>,
    pub new_protocol_components: HashMap<String, ProtocolComponent>,
    pub deleted_protocol_components: HashMap<String, ProtocolComponent>,
    pub component_balances: HashMap<ComponentId, HashMap<Bytes, ComponentBalance>>,
    pub component_tvl: HashMap<String, f64>,
}

impl AggregatedBlockChanges {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        extractor: &str,
        chain: Chain,
        block: Block,
        finalised_block_height: u64,
        revert: bool,
        state_deltas: HashMap<String, ProtocolComponentStateDelta>,
        account_deltas: HashMap<Bytes, AccountUpdate>,
        new_tokens: HashMap<Address, CurrencyToken>,
        new_components: HashMap<String, ProtocolComponent>,
        deleted_components: HashMap<String, ProtocolComponent>,
        component_balances: HashMap<ComponentId, HashMap<Bytes, ComponentBalance>>,
        component_tvl: HashMap<String, f64>,
    ) -> Self {
        Self {
            extractor: extractor.to_string(),
            chain,
            block,
            finalized_block_height: finalised_block_height,
            revert,
            state_updates: state_deltas,
            account_updates: account_deltas,
            new_protocol_components: new_components,
            deleted_protocol_components: deleted_components,
            component_balances,
            component_tvl,
            new_tokens,
        }
    }
}

impl std::fmt::Display for AggregatedBlockChanges {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "block_number: {}, extractor: {}", self.block.number, self.extractor)
    }
}

#[typetag::serde]
impl NormalisedMessage for AggregatedBlockChanges {
    fn source(&self) -> ExtractorIdentity {
        ExtractorIdentity::new(self.chain, &self.extractor)
    }

    fn drop_state(&self) -> Arc<dyn NormalisedMessage> {
        Arc::new(Self {
            extractor: self.extractor.clone(),
            chain: self.chain,
            block: self.block.clone(),
            finalized_block_height: self.finalized_block_height,
            revert: self.revert,
            account_updates: HashMap::new(),
            state_updates: HashMap::new(),
            new_tokens: self.new_tokens.clone(),
            new_protocol_components: self.new_protocol_components.clone(),
            deleted_protocol_components: self.deleted_protocol_components.clone(),
            component_balances: self.component_balances.clone(),
            component_tvl: self.component_tvl.clone(),
        })
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub trait BlockScoped {
    fn block(&self) -> Block;
}

impl BlockScoped for AggregatedBlockChanges {
    fn block(&self) -> Block {
        self.block.clone()
    }
}
