use crate::{
    extractor::{
        evm,
        evm::{ProtocolState, ProtocolStateDelta},
    },
    models::Chain,
    storage::ChangeType,
};
use chrono::NaiveDateTime;
use std::collections::{HashMap, HashSet};
use tycho_types::Bytes;

#[derive(Debug, Clone, PartialEq)]
pub struct ProtocolComponent {
    pub id: String,
    pub protocol_system: String,
    pub protocol_type_name: String,
    pub chain: Chain,
    pub tokens: Vec<Bytes>,
    pub contract_addresses: Vec<Bytes>,
    pub static_attributes: HashMap<String, Bytes>,
    // TODO: decide for a module. dto or storage. -> probably promote change type eventually
    pub change: ChangeType,
    pub creation_tx: Bytes,
    pub created_at: NaiveDateTime,
}

impl ProtocolComponent {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: &str,
        protocol_system: &str,
        protocol_type_name: &str,
        chain: Chain,
        tokens: Vec<Bytes>,
        contract_addresses: Vec<Bytes>,
        static_attributes: HashMap<String, Bytes>,
        change: ChangeType,
        creation_tx: Bytes,
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

impl From<&evm::ProtocolComponent> for ProtocolComponent {
    fn from(value: &evm::ProtocolComponent) -> Self {
        Self {
            id: value.id.clone(),
            protocol_system: value.protocol_system.clone(),
            protocol_type_name: value.protocol_type_name.clone(),
            chain: value.chain,
            tokens: value
                .tokens
                .iter()
                .map(|t| t.as_bytes().into())
                .collect(),
            contract_addresses: value
                .contract_ids
                .iter()
                .map(|a| a.as_bytes().into())
                .collect(),
            static_attributes: value.static_attributes.clone(),
            change: value.change,
            creation_tx: value.creation_tx.into(),
            created_at: value.created_at,
        }
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct ProtocolComponentState {
    pub component_id: String,
    pub attributes: HashMap<String, Bytes>,
    pub modify_tx: Bytes,
}

impl ProtocolComponentState {
    pub fn new(component_id: &str, attributes: HashMap<String, Bytes>, modify_tx: Bytes) -> Self {
        Self { component_id: component_id.to_string(), attributes, modify_tx }
    }
}

impl From<&evm::ProtocolState> for ProtocolComponentState {
    fn from(value: &ProtocolState) -> Self {
        Self {
            component_id: value.component_id.clone(),
            attributes: value.attributes.clone(),
            modify_tx: Bytes::from(value.modify_tx.as_bytes()),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProtocolComponentStateDelta {
    pub component_id: String,
    pub updated_attributes: HashMap<String, Bytes>,
    // TODO: rename back to deleted_attributes
    pub removed_attributes: HashSet<String>,
}

impl ProtocolComponentStateDelta {
    pub fn new(
        component_id: &str,
        updated_attributes: HashMap<String, Bytes>,
        removed_attributes: HashSet<String>,
    ) -> Self {
        Self { component_id: component_id.to_string(), updated_attributes, removed_attributes }
    }
}

impl From<&evm::ProtocolStateDelta> for ProtocolComponentStateDelta {
    fn from(value: &ProtocolStateDelta) -> Self {
        Self {
            component_id: value.component_id.clone(),
            updated_attributes: value.updated_attributes.clone(),
            removed_attributes: value.deleted_attributes.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ComponentBalance {
    pub token: Bytes,
    pub new_balance: Bytes,
    pub balance_float: f64,
    pub modify_tx: Bytes,
    pub component_id: String,
}

impl ComponentBalance {
    pub fn new(
        token: Bytes,
        new_balance: Bytes,
        balance_float: f64,
        modify_tx: Bytes,
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

impl From<&evm::ComponentBalance> for ComponentBalance {
    fn from(value: &evm::ComponentBalance) -> Self {
        Self {
            token: value.token.as_bytes().into(),
            new_balance: value.balance.clone(),
            balance_float: value.balance_float,
            modify_tx: value.modify_tx.as_bytes().into(),
            component_id: value.component_id.clone(),
        }
    }
}

// TODO: remove this, only here so the old revert method compiles.
impl From<ComponentBalance> for evm::ComponentBalance {
    fn from(_value: ComponentBalance) -> Self {
        todo!()
    }
}
