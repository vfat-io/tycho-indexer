use crate::{
    models::{Chain, ChangeType, ContractId},
    Bytes,
};
use std::collections::HashMap;

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

    pub fn set_balance(&mut self, new_balance: &Bytes, modified_at: &Bytes) {
        self.balance = new_balance.clone();
        self.balance_modify_tx = modified_at.clone();
    }
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct ContractDelta {
    pub chain: Chain,
    pub address: Bytes,
    pub slots: HashMap<Bytes, Bytes>,
    pub balance: Option<Bytes>,
    pub code: Option<Bytes>,
    pub change: ChangeType,
}

impl ContractDelta {
    pub fn deleted(chain: &Chain, address: &Bytes) -> Self {
        Self {
            chain: *chain,
            address: address.clone(),
            change: ChangeType::Deletion,
            ..Default::default()
        }
    }

    pub fn new(
        chain: &Chain,
        address: &Bytes,
        slots: Option<&HashMap<Bytes, Option<Bytes>>>,
        balance: Option<&Bytes>,
        code: Option<&Bytes>,
        change: ChangeType,
    ) -> Self {
        Self {
            chain: *chain,
            address: address.clone(),
            change,
            slots: slots
                .map(|storage| {
                    storage
                        .iter()
                        // TODO: unwrap or default converts to 0x0 instead of a 32 byte 0 string
                        .map(|(k, v)| (k.clone(), v.clone().unwrap_or_default()))
                        .collect()
                })
                .unwrap_or_default(),
            balance: balance.cloned(),
            code: code.cloned(),
        }
    }

    pub fn contract_id(&self) -> ContractId {
        ContractId::new(self.chain, self.address.clone())
    }
}

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
