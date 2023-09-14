pub mod ambient;

use crate::{
    models::{Chain, ExtractorIdentity, NormalisedMessage},
    storage::StateGatewayType,
};
use std::collections::HashMap;

use chrono::NaiveDateTime;
use ethers::types::{H160, H256, U256};
use serde::{Deserialize, Serialize};

pub struct SwapPool {}

pub struct ERC20Token {}

#[derive(Debug, PartialEq, Copy, Clone)]
pub struct Block {
    pub number: u64,
    pub hash: H256,
    pub parent_hash: H256,
    pub chain: Chain,
    pub ts: NaiveDateTime,
}

#[derive(Debug, PartialEq, Copy, Clone, Default)]
pub struct Transaction {
    pub hash: H256,
    pub block_hash: H256,
    pub from: H160,
    pub to: H160,
    pub index: u64,
}

#[derive(PartialEq, Debug, Clone)]
pub struct Account {
    pub chain: Chain,
    pub address: H160,
    pub title: String,
    pub slots: HashMap<U256, U256>,
    pub balance: U256,
    pub code: Vec<u8>,
    pub code_hash: H256,
    pub balance_modify_tx: H256,
    pub code_modify_tx: H256,
    pub creation_tx: Option<H256>,
}

impl Account {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        chain: Chain,
        address: H160,
        title: String,
        slots: HashMap<U256, U256>,
        balance: U256,
        code: Vec<u8>,
        code_hash: H256,
        balance_modify_tx: H256,
        code_modify_tx: H256,
        creation_tx: Option<H256>,
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

    pub fn set_balance(&mut self, new_balance: U256, modified_at: H256) {
        self.balance = new_balance;
        self.balance_modify_tx = modified_at;
    }
}

#[derive(PartialEq, Serialize, Deserialize, Clone, Debug)]
pub struct AccountUpdate {
    extractor: String,
    chain: Chain,
    pub address: H160,
    pub slots: HashMap<U256, U256>,
    pub balance: Option<U256>,
    pub code: Option<Vec<u8>>,
    pub code_hash: Option<H256>,
    pub tx_hash: Option<H256>,
}

impl AccountUpdate {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        extractor: String,
        chain: Chain,
        address: H160,
        slots: HashMap<U256, U256>,
        balance: Option<U256>,
        code: Option<Vec<u8>>,
        code_hash: Option<H256>,
        tx_hash: Option<H256>,
    ) -> Self {
        Self { extractor, chain, address, slots, balance, code, code_hash, tx_hash }
    }
}

struct UpdateWithTransaction(Transaction, AccountUpdate);

struct BlockStateChanges {
    block: Block,
    account_updates: HashMap<H256, UpdateWithTransaction>,
    new_pools: HashMap<H160, SwapPool>,
}

impl NormalisedMessage for AccountUpdate {
    fn source(&self) -> ExtractorIdentity {
        ExtractorIdentity { chain: self.chain, name: self.extractor.clone() }
    }
}

pub type EVMStateGateway<DB> = StateGatewayType<DB, Block, Transaction, ERC20Token, Account>;
