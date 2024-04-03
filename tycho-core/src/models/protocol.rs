use crate::{
    models::{Chain, ChangeType},
    Bytes,
};
use chrono::NaiveDateTime;
use std::collections::{HashMap, HashSet};

use super::{Address, AttrStoreKey, Balance, ComponentId, DeltaError, StoreVal, TxHash};

#[derive(Debug, Clone, PartialEq)]
pub struct ProtocolComponent {
    pub id: ComponentId,
    pub protocol_system: String,
    pub protocol_type_name: String,
    pub chain: Chain,
    pub tokens: Vec<Address>,
    pub contract_addresses: Vec<Address>,
    pub static_attributes: HashMap<AttrStoreKey, StoreVal>,
    pub change: ChangeType,
    pub creation_tx: TxHash,
    pub created_at: NaiveDateTime,
}

impl ProtocolComponent {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: &str,
        protocol_system: &str,
        protocol_type_name: &str,
        chain: Chain,
        tokens: Vec<Address>,
        contract_addresses: Vec<Address>,
        static_attributes: HashMap<AttrStoreKey, StoreVal>,
        change: ChangeType,
        creation_tx: TxHash,
        created_at: NaiveDateTime,
    ) -> Self {
        Self {
            id: id.to_string(),
            protocol_system: protocol_system.to_string(),
            protocol_type_name: protocol_type_name.to_string(),
            chain,
            tokens,
            contract_addresses,
            static_attributes,
            change,
            creation_tx,
            created_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProtocolComponentState {
    pub component_id: ComponentId,
    pub attributes: HashMap<AttrStoreKey, StoreVal>,
    // used during snapshots retrieval by the gateway
    pub balances: HashMap<Address, Balance>,
}

impl ProtocolComponentState {
    pub fn new(
        component_id: &str,
        attributes: HashMap<AttrStoreKey, StoreVal>,
        balances: HashMap<Address, Balance>,
    ) -> Self {
        Self { component_id: component_id.to_string(), attributes, balances }
    }

    pub fn apply_state_delta(
        &mut self,
        delta: &ProtocolComponentStateDelta,
    ) -> Result<(), DeltaError> {
        if self.component_id != delta.component_id {
            return Err(DeltaError::IdMismatch(
                self.component_id.clone(),
                delta.component_id.clone(),
            ));
        }
        self.attributes.extend(
            delta
                .updated_attributes
                .clone()
                .into_iter(),
        );

        self.attributes
            .retain(|attr, _| !delta.deleted_attributes.contains(attr));

        Ok(())
    }

    pub fn apply_balance_delta(
        &mut self,
        delta: &HashMap<Bytes, ComponentBalance>,
    ) -> Result<(), DeltaError> {
        self.balances.extend(
            delta
                .iter()
                .map(|(k, v)| (k.clone(), v.new_balance.clone())),
        );

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProtocolComponentStateDelta {
    pub component_id: ComponentId,
    pub updated_attributes: HashMap<AttrStoreKey, StoreVal>,
    pub deleted_attributes: HashSet<AttrStoreKey>,
}

impl ProtocolComponentStateDelta {
    pub fn new(
        component_id: &str,
        updated_attributes: HashMap<AttrStoreKey, StoreVal>,
        deleted_attributes: HashSet<AttrStoreKey>,
    ) -> Self {
        Self { component_id: component_id.to_string(), updated_attributes, deleted_attributes }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ComponentBalance {
    pub token: Address,
    pub new_balance: Balance,
    pub balance_float: f64,
    pub modify_tx: TxHash,
    pub component_id: ComponentId,
}

impl ComponentBalance {
    pub fn new(
        token: Address,
        new_balance: Balance,
        balance_float: f64,
        modify_tx: TxHash,
        component_id: &str,
    ) -> Self {
        Self {
            token,
            new_balance,
            balance_float,
            modify_tx,
            component_id: component_id.to_string(),
        }
    }
}
