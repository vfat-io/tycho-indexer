pub mod ambient;

use crate::{
    models::{Chain, ExtractorIdentity, NormalisedMessage},
    storage::StateGatewayType,
};
use std::collections::HashMap;

use crate::pb::tycho::evm::v1 as substreams;
use chrono::NaiveDateTime;
use ethers::{
    types::{H160, H256, U256},
    utils::keccak256,
};
use serde::{Deserialize, Serialize};

use super::ExtractionError;
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
    pub to: Option<H160>,
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
    pub tx: Transaction,
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
        tx: Transaction,
    ) -> Self {
        Self { extractor, chain, address, slots, balance, code, tx }
    }
}

pub struct BlockStateChanges {
    pub block: Block,
    pub account_updates: HashMap<H256, AccountUpdate>,
    pub new_pools: HashMap<H160, SwapPool>,
}

impl NormalisedMessage for AccountUpdate {
    fn source(&self) -> ExtractorIdentity {
        ExtractorIdentity { chain: self.chain, name: self.extractor.clone() }
    }
}

pub type EVMStateGateway<DB> = StateGatewayType<DB, Block, Transaction, ERC20Token, Account>;

impl Block {
    pub fn try_from_message(msg: substreams::Block, chain: Chain) -> Result<Self, ExtractionError> {
        Ok(Self {
            chain,
            number: msg.number,
            hash: parse_32bytes(&msg.hash)?,
            parent_hash: parse_32bytes(&msg.parent_hash)?,
            ts: NaiveDateTime::from_timestamp_opt(msg.ts as i64, 0).ok_or_else(|| {
                ExtractionError::DecodeError(format!(
                    "Failed to convert timestamp {} to datetime!",
                    msg.ts
                ))
            })?,
        })
    }
}

fn parse_32bytes<T>(v: &[u8]) -> Result<T, ExtractionError>
where
    T: From<[u8; 32]>,
{
    let data: [u8; 32] = v.try_into().map_err(|_| {
        ExtractionError::DecodeError(format!("Failed to decode hash: {}", hex::encode(v)))
    })?;
    Ok(T::from(data))
}

fn parse_h160(v: &[u8]) -> Result<H160, ExtractionError> {
    let parent_hash: [u8; 20] = v.try_into().map_err(|_| {
        ExtractionError::DecodeError(format!("Failed to decode hash: {}", hex::encode(v)))
    })?;
    Ok(H160::from(parent_hash))
}

impl Transaction {
    pub fn try_from_message(
        msg: substreams::Transaction,
        block_hash: &H256,
    ) -> Result<Self, ExtractionError> {
        let to = if !msg.to.is_empty() { Some(parse_h160(&msg.to)?) } else { None };
        Ok(Self {
            hash: parse_32bytes(&msg.hash)?,
            block_hash: *block_hash,
            from: parse_h160(&msg.from)?,
            to,
            index: msg.index,
        })
    }
}

impl AccountUpdate {
    pub fn try_from_message(
        msg: substreams::ContractChange,
        tx: &Transaction,
        extractor: &str,
        chain: Chain,
    ) -> Result<Self, ExtractionError> {
        Ok(Self {
            address: parse_h160(&msg.address)?,
            extractor: extractor.to_owned(),
            chain,
            slots: HashMap::new(),
            balance: if !msg.balance.is_empty() {
                Some(parse_32bytes(&msg.balance)?)
            } else {
                None
            },
            code: if !msg.code.is_empty() { Some(msg.code) } else { None },
            tx: *tx,
        })
    }
}

impl BlockStateChanges {
    pub fn try_from_message(
        msg: substreams::BlockContractChanges,
        extractor: &str,
        chain: Chain,
    ) -> Result<Self, ExtractionError> {
        if let Some(block) = msg.block {
            let block = Block::try_from_message(block, chain)?;
            let mut account_updates = HashMap::new();

            for change in msg.changes.into_iter() {
                if let Some(tx) = change.tx {
                    let tx = Transaction::try_from_message(tx, &block.hash)?;
                    for el in change.contract_changes.into_iter() {
                        let update = AccountUpdate::try_from_message(el, &tx, extractor, chain)?;
                        if account_updates
                            .insert(tx.hash, update)
                            .is_some()
                        {
                            // Currently this can't happen as the changes are
                            // usually aggregated on the substreams handlers.
                            // But in case it should be necessary we can later
                            // add another level of aggregation here.
                            return Err(ExtractionError::DecodeError(format!(
                                "Duplicate transaction {}",
                                tx.hash,
                            )))
                        }
                    }
                }
            }
            return Ok(Self { block, account_updates, new_pools: HashMap::new() })
        }
        Err(ExtractionError::Empty)
    }
}

impl Account {
    pub fn from(val: AccountUpdate) -> Self {
        let code = val.code.unwrap_or_default();
        let code_hash = H256::from(keccak256(&code));
        Self {
            address: val.address,
            chain: val.chain,
            title: format!("Contract {:x}", val.address),
            slots: val.slots,
            balance: val.balance.unwrap_or_default(),
            code,
            code_hash,
            balance_modify_tx: val.tx.hash,
            code_modify_tx: val.tx.hash,
            creation_tx: Some(val.tx.hash),
        }
    }
}
