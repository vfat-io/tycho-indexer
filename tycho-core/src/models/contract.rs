use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::{
    keccak256,
    models::{Chain, ChangeType, ContractId, DeltaError},
};
use std::collections::HashMap;

use super::{
    blockchain::Transaction, Address, Balance, Code, CodeHash, StoreKey, StoreVal, TxHash,
};

#[derive(Clone, Debug, PartialEq)]
pub struct Account {
    pub chain: Chain,
    pub address: Address,
    pub title: String,
    pub slots: HashMap<StoreKey, StoreVal>,
    pub native_balance: Balance,
    pub code: Code,
    pub code_hash: CodeHash,
    pub balance_modify_tx: TxHash,
    pub code_modify_tx: TxHash,
    pub creation_tx: Option<TxHash>,
}

impl Account {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        chain: Chain,
        address: Address,
        title: String,
        slots: HashMap<StoreKey, StoreVal>,
        native_balance: Balance,
        code: Code,
        code_hash: CodeHash,
        balance_modify_tx: TxHash,
        code_modify_tx: TxHash,
        creation_tx: Option<TxHash>,
    ) -> Self {
        Self {
            chain,
            address,
            title,
            slots,
            native_balance,
            code,
            code_hash,
            balance_modify_tx,
            code_modify_tx,
            creation_tx,
        }
    }

    pub fn set_balance(&mut self, new_balance: &Balance, modified_at: &Balance) {
        self.native_balance = new_balance.clone();
        self.balance_modify_tx = modified_at.clone();
    }

    pub fn apply_contract_delta(&mut self, delta: &AccountUpdate) -> Result<(), DeltaError> {
        let self_id = (self.chain, &self.address);
        let other_id = (delta.chain, &delta.address);
        if self_id != other_id {
            return Err(DeltaError::IdMismatch(format!("{:?}", self_id), format!("{:?}", other_id)));
        }
        if let Some(balance) = delta.balance.as_ref() {
            self.native_balance.clone_from(balance);
        }
        if let Some(code) = delta.code.as_ref() {
            self.code.clone_from(code);
        }
        self.slots.extend(
            delta
                .slots
                .clone()
                .into_iter()
                .map(|(k, v)| (k, v.unwrap_or_default())),
        );
        // TODO: Update modify_tx, code_modify_tx and code_hash.
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
pub struct AccountUpdate {
    pub chain: Chain,
    pub address: Address,
    pub slots: HashMap<StoreKey, Option<StoreVal>>,
    pub balance: Option<Balance>,
    pub code: Option<Code>,
    pub change: ChangeType,
}

impl AccountUpdate {
    pub fn deleted(chain: &Chain, address: &Address) -> Self {
        Self {
            chain: *chain,
            address: address.clone(),
            change: ChangeType::Deletion,
            ..Default::default()
        }
    }

    pub fn new(
        chain: Chain,
        address: Address,
        slots: HashMap<StoreKey, Option<StoreVal>>,
        balance: Option<Balance>,
        code: Option<Code>,
        change: ChangeType,
    ) -> Self {
        Self { chain, address, change, slots, balance, code }
    }

    pub fn contract_id(&self) -> ContractId {
        ContractId::new(self.chain, self.address.clone())
    }

    pub fn into_account(self, tx: &Transaction) -> Account {
        let empty_hash = keccak256(Vec::new());
        Account::new(
            self.chain,
            self.address.clone(),
            format!("{:#020x}", self.address),
            self.slots
                .into_iter()
                .map(|(k, v)| (k, v.map(Into::into).unwrap_or_default()))
                .collect(),
            self.balance.unwrap_or_default(),
            self.code.clone().unwrap_or_default(),
            self.code
                .as_ref()
                .map(keccak256)
                .unwrap_or(empty_hash)
                .into(),
            tx.hash.clone(),
            tx.hash.clone(),
            Some(tx.hash.clone()),
        )
    }

    // Convert AccountUpdate into Account using references.
    pub fn ref_into_account(&self, tx: &Transaction) -> Account {
        let empty_hash = keccak256(Vec::new());
        if self.change != ChangeType::Creation {
            warn!("Creating an account from a partial change!")
        }

        Account::new(
            self.chain,
            self.address.clone(),
            format!("{:#020x}", self.address),
            self.slots
                .clone()
                .into_iter()
                .map(|(k, v)| (k, v.unwrap_or_default()))
                .collect(),
            self.balance.clone().unwrap_or_default(),
            self.code.clone().unwrap_or_default(),
            self.code
                .as_ref()
                .map(keccak256)
                .unwrap_or(empty_hash)
                .into(),
            tx.hash.clone(),
            tx.hash.clone(),
            Some(tx.hash.clone()),
        )
    }

    /// Merge this update (`self`) with another one (`other`)
    ///
    /// This function is utilized for aggregating multiple updates into a single
    /// update. The attribute values of `other` are set on `self`.
    /// Meanwhile, contract storage maps are merged, in which keys from `other`
    /// take precedence.
    ///
    /// Be noted that, this function will mutate the state of the calling
    /// struct. An error will occur if merging updates from different accounts.
    ///
    /// There are no further validation checks within this method, hence it
    /// could be used as needed. However, you should give preference to
    /// utilizing [TransactionVMUpdates] for merging, when possible.
    ///
    /// # Errors
    ///
    /// It returns an `CoreError::MergeError` error if `self.address` and
    /// `other.address` are not identical.
    ///
    /// # Arguments
    ///
    /// * `other`: An instance of `AccountUpdate`. The attribute values and keys
    /// of `other` will overwrite those of `self`.
    pub fn merge(&mut self, other: AccountUpdate) -> Result<(), String> {
        if self.address != other.address {
            return Err(format!(
                "Can't merge AccountUpdates from differing identities; Expected {:#020x}, got {:#020x}",
                self.address, other.address
            ));
        }

        self.slots.extend(other.slots);

        if let Some(balance) = other.balance {
            self.balance = Some(balance)
        }
        self.code = other.code.or(self.code.take());

        Ok(())
    }

    pub fn is_update(&self) -> bool {
        self.change == ChangeType::Update
    }

    pub fn is_creation(&self) -> bool {
        self.change == ChangeType::Creation
    }
}

impl From<Account> for AccountUpdate {
    fn from(value: Account) -> Self {
        Self {
            chain: value.chain,
            address: value.address,
            slots: value
                .slots
                .into_iter()
                .map(|(k, v)| (k, Some(v)))
                .collect(),
            balance: Some(value.native_balance),
            code: Some(value.code),
            change: ChangeType::Creation,
        }
    }
}
