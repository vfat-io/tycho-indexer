use crate::{
    extractor::{
        evm,
        evm::{Account, AccountUpdate},
    },
    models::Chain,
    storage,
    storage::ChangeType,
};
use ethers::prelude::{H160, U256};
use std::collections::HashMap;
use tycho_types::Bytes;

#[derive(Clone, Debug, PartialEq)]
pub struct Contract {
    pub chain: Chain,
    pub address: Bytes,
    pub title: String,
    pub slots: HashMap<Bytes, Bytes>,
    pub balance: Bytes,
    pub code: Bytes,
    pub code_hash: Bytes,
    pub balance_modify_tx: Bytes,
    pub code_modify_tx: Bytes,
    pub creation_tx: Option<Bytes>,
}

impl Contract {
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

    #[cfg(test)]
    pub fn set_balance(&mut self, new_balance: &Bytes, modified_at: &Bytes) {
        self.balance = new_balance.clone();
        self.balance_modify_tx = modified_at.clone();
    }
}

impl From<evm::Account> for Contract {
    fn from(value: Account) -> Self {
        Self {
            chain: value.chain,
            address: Bytes::from(value.address.as_bytes()),
            title: value.title.clone(),
            slots: value
                .slots
                .into_iter()
                .map(|(u, v)| (Bytes::from(u), Bytes::from(v)))
                .collect(),
            balance: Bytes::from(value.balance),
            code: value.code,
            code_hash: Bytes::from(value.code_hash.as_bytes()),
            balance_modify_tx: Bytes::from(value.balance_modify_tx.as_bytes()),
            code_modify_tx: Bytes::from(value.code_modify_tx.as_bytes()),
            creation_tx: value
                .creation_tx
                .map(|s| Bytes::from(s.as_bytes())),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ContractDelta {
    pub chain: Chain,
    pub address: Bytes,
    pub slots: HashMap<Bytes, Bytes>,
    pub balance: Option<Bytes>,
    pub code: Option<Bytes>,
    pub change: ChangeType,
}

impl ContractDelta {
    pub fn new() -> Self {
        todo!();
    }
    pub fn contract_id(&self) -> storage::ContractId {
        storage::ContractId::new(self.chain, self.address.clone())
    }
}

// Temporary until evm models are phased out
impl From<evm::AccountUpdate> for ContractDelta {
    fn from(value: AccountUpdate) -> Self {
        Self {
            chain: value.chain,
            address: Bytes::from(value.address.as_bytes()),
            slots: value
                .slots
                .into_iter()
                .map(|(u, v)| (Bytes::from(u), Bytes::from(v)))
                .collect(),
            balance: value.balance.map(|s| Bytes::from(s)),
            code: value.code,
            change: value.change,
        }
    }
}

impl From<ContractDelta> for evm::AccountUpdate {
    fn from(value: ContractDelta) -> Self {
        Self {
            address: H160::from_slice(&value.address),
            chain: value.chain,
            slots: value
                .slots
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
            balance: value.balance.map(|b| U256::from(b)),
            code: value.code,
            change: value.change,
        }
    }
}

// Keep this one, it is useful
impl From<Contract> for ContractDelta {
    fn from(value: Contract) -> Self {
        Self {
            chain: value.chain,
            address: value.address,
            slots: value.slots,
            balance: Some(value.balance),
            code: Some(value.code),
            change: ChangeType::Creation,
        }
    }
}
