pub mod ambient;

use crate::{
    models::{Chain, ExtractorIdentity, NormalisedMessage},
    storage::StateGatewayType,
};
use std::{
    collections::{hash_map::Entry, HashMap},
    ops::Deref,
};

use crate::pb::tycho::evm::v1 as substreams;
use chrono::NaiveDateTime;
use ethers::{
    types::{H160, H256, U256},
    utils::keccak256,
};
use serde::{Deserialize, Serialize};

use super::ExtractionError;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SwapPool {}

pub struct ERC20Token {}

#[derive(Debug, PartialEq, Copy, Clone, Deserialize, Serialize)]
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

impl From<&AccountUpdateWithTx> for Account {
    fn from(value: &AccountUpdateWithTx) -> Self {
        let empty_hash = H256::from(keccak256(Vec::new()));
        Account::new(
            value.chain,
            value.address,
            format!("{:x}", value.address),
            value.slots.clone(),
            value.balance.unwrap_or_default(),
            value.code.clone().unwrap_or_default(),
            value
                .code
                .as_ref()
                .map(|v| H256::from(keccak256(v)))
                .unwrap_or(empty_hash),
            value.tx.hash,
            value.tx.hash,
            Some(value.tx.hash),
        )
    }
}

#[derive(PartialEq, Serialize, Deserialize, Clone, Debug)]
pub struct AccountUpdate {
    pub address: H160,
    pub chain: Chain,
    pub slots: HashMap<U256, U256>,
    pub balance: Option<U256>,
    pub code: Option<Vec<u8>>,
}

impl AccountUpdate {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        address: H160,
        chain: Chain,
        slots: HashMap<U256, U256>,
        balance: Option<U256>,
        code: Option<Vec<u8>>,
    ) -> Self {
        Self { address, chain, slots, balance, code }
    }

    // TODO: test
    fn merge(&mut self, other: AccountUpdate) -> Result<(), ExtractionError> {
        if self.address != other.address {
            return Err(ExtractionError::Unknown(
                "Can't merge AccountUpdates from differing identities".into(),
            ))
        }

        self.slots.extend(other.slots);

        self.balance = other.balance.or(self.balance);
        self.code = other.code.or(self.code.take());

        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BlockAccountChanges {
    extractor: String,
    chain: Chain,
    pub block: Block,
    pub account_updates: HashMap<H160, AccountUpdate>,
    pub new_pools: HashMap<H160, SwapPool>,
}

impl NormalisedMessage for BlockAccountChanges {
    fn source(&self) -> ExtractorIdentity {
        ExtractorIdentity { chain: self.chain, name: self.extractor.clone() }
    }
}

#[derive(Debug)]
pub struct AccountUpdateWithTx {
    pub update: AccountUpdate,
    pub tx: Transaction,
}

impl AccountUpdateWithTx {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        address: H160,
        chain: Chain,
        slots: HashMap<U256, U256>,
        balance: Option<U256>,
        code: Option<Vec<u8>>,
        tx: Transaction,
    ) -> Self {
        Self { update: AccountUpdate { address, chain, slots, balance, code }, tx }
    }

    pub fn merge(&mut self, other: AccountUpdateWithTx) -> Result<(), ExtractionError> {
        if self.tx.block_hash != other.tx.block_hash {
            return Err(ExtractionError::Unknown(format!(
                "Can't merge AccountUpdates from different blocks: {:x} != {:x}",
                self.tx.block_hash, other.tx.block_hash,
            )))
        }
        if self.tx.index == other.tx.index {
            return Err(ExtractionError::Unknown(format!(
                "Can't merge AccountUpdates from the same transaction: {:x}",
                self.tx.hash
            )))
        }
        if self.tx.index > other.tx.index {
            return Err(ExtractionError::Unknown(format!(
                "Can't merge AccountUpdates with lower transaction index: {} > {}",
                self.tx.index, other.tx.index
            )))
        }
        self.tx = other.tx;
        self.update.merge(other.update)
    }
}

impl Deref for AccountUpdateWithTx {
    type Target = AccountUpdate;

    fn deref(&self) -> &Self::Target {
        &self.update
    }
}

#[derive(Debug)]
pub struct BlockStateChanges {
    extractor: String,
    chain: Chain,
    pub block: Block,
    pub tx_updates: Vec<AccountUpdateWithTx>,
    pub new_pools: HashMap<H160, SwapPool>,
}

pub type EVMStateGateway<DB> = StateGatewayType<DB, Block, Transaction, Account, AccountUpdate>;

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
    if v.len() > 32 {
        return Err(ExtractionError::DecodeError(format!(
            "Byte slice too long: Expected 32, got {}",
            v.len()
        )))
    }
    let mut data: [u8; 32] = [0; 32];
    let start_index = 32 - v.len();
    data[start_index..].copy_from_slice(v);

    Ok(T::from(data))
}

fn parse_h160(v: &[u8]) -> Result<H160, ExtractionError> {
    if v.len() > 20 {
        return Err(ExtractionError::DecodeError(format!(
            "H160: Byte slice too long: Expected 20, got {}",
            v.len()
        )))
    }

    let mut data: [u8; 20] = [0; 20];
    let start_index = 20 - v.len();
    data[start_index..].copy_from_slice(v);

    Ok(H160::from(data))
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

impl AccountUpdateWithTx {
    pub fn try_from_message(
        msg: substreams::ContractChange,
        tx: &Transaction,
        chain: Chain,
    ) -> Result<Self, ExtractionError> {
        let update = AccountUpdate {
            address: parse_h160(&msg.address)?,
            chain,
            slots: msg
                .slots
                .into_iter()
                .map(|cs| Ok((parse_32bytes::<U256>(&cs.slot)?, parse_32bytes::<U256>(&cs.value)?)))
                .collect::<Result<HashMap<_, _>, ExtractionError>>()?,
            balance: if !msg.balance.is_empty() {
                Some(parse_32bytes(&msg.balance)?)
            } else {
                None
            },
            code: if !msg.code.is_empty() { Some(msg.code) } else { None },
        };
        Ok(Self { update, tx: *tx })
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
            let mut tx_updates = Vec::new();

            for change in msg.changes.into_iter() {
                if let Some(tx) = change.tx {
                    let tx = Transaction::try_from_message(tx, &block.hash)?;
                    for el in change.contract_changes.into_iter() {
                        let update = AccountUpdateWithTx::try_from_message(el, &tx, chain)?;
                        tx_updates.push(update);
                    }
                }
            }
            tx_updates.sort_unstable_by_key(|update| update.tx.index);
            return Ok(Self {
                extractor: extractor.to_owned(),
                chain,
                block,
                tx_updates,
                new_pools: HashMap::new(),
            })
        }
        Err(ExtractionError::Empty)
    }

    pub fn merge_updates(self) -> Result<BlockAccountChanges, ExtractionError> {
        let mut account_updates: HashMap<H160, AccountUpdate> = HashMap::new();

        for update in self
            .tx_updates
            .into_iter()
            .map(|v| v.update)
        {
            match account_updates.entry(update.address) {
                Entry::Occupied(mut e) => {
                    e.get_mut().merge(update)?;
                }
                Entry::Vacant(e) => {
                    e.insert(update);
                }
            }
        }
        Ok(BlockAccountChanges {
            extractor: self.extractor,
            chain: self.chain,
            block: self.block,
            account_updates,
            new_pools: self.new_pools,
        })
    }
}
