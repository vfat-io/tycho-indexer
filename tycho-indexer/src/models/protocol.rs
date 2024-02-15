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

pub struct ProtocolComponent {
    pub id: String,
    pub protocol_system: String,
    pub protocol_type_name: String,
    pub chain: Chain,
    pub tokens: Vec<Bytes>,
    pub contract_ids: Vec<Bytes>,
    pub static_attributes: HashMap<String, Bytes>,
    pub change: ChangeType,
    pub creation_tx: Bytes,
    pub created_at: NaiveDateTime,
}

impl From<evm::ProtocolComponent> for ProtocolComponent {
    fn from(_value: evm::ProtocolComponent) -> Self {
        todo!()
    }
}

pub struct ProtocolComponentState {
    pub component_id: String,
    pub attributes: HashMap<String, Bytes>,
    pub modify_tx: Bytes,
}

impl From<evm::ProtocolState> for ProtocolComponentState {
    fn from(_value: ProtocolState) -> Self {
        todo!()
    }
}

pub struct ProtocolComponentStateDelta {
    pub component_id: String,
    pub updated_attributes: HashMap<String, Bytes>,
    pub removed_attributes: HashSet<String>,
}

impl From<evm::ProtocolStateDelta> for ProtocolComponentStateDelta {
    fn from(_value: ProtocolStateDelta) -> Self {
        todo!()
    }
}

pub struct ComponentBalance {
    pub token: Bytes,
    pub new_balance: Bytes,
    pub balance_float: f64,
    pub modify_tx: Bytes,
    pub component_id: String,
}

impl From<evm::ComponentBalance> for ComponentBalance {
    fn from(_value: evm::ComponentBalance) -> Self {
        todo!()
    }
}
