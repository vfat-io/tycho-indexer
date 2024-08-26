use std::collections::HashMap;

use crate::extractor::{evm, evm::ProtocolStateDelta};
use ethers::prelude::{H160, U256};
use tycho_core::{
    models::{
        blockchain::BlockAggregatedDeltas,
        contract::{Contract, ContractDelta},
        protocol::{ComponentBalance, ProtocolComponent, ProtocolComponentStateDelta},
    },
    Bytes,
};

impl From<&evm::Account> for Contract {
    fn from(value: &evm::Account) -> Self {
        Self {
            chain: value.chain,
            address: Bytes::from(value.address.as_bytes()),
            title: value.title.clone(),
            slots: value
                .slots
                .clone()
                .into_iter()
                .map(|(u, v)| (Bytes::from(u), Bytes::from(v)))
                .collect(),
            native_balance: Bytes::from(value.balance),
            code: value.code.clone(),
            code_hash: Bytes::from(value.code_hash.as_bytes()),
            balance_modify_tx: Bytes::from(value.balance_modify_tx.as_bytes()),
            code_modify_tx: Bytes::from(value.code_modify_tx.as_bytes()),
            creation_tx: value
                .creation_tx
                .map(|s| Bytes::from(s.as_bytes())),
        }
    }
}

impl From<&evm::AccountUpdate> for ContractDelta {
    fn from(value: &evm::AccountUpdate) -> Self {
        Self {
            chain: value.chain,
            address: Bytes::from(value.address.as_bytes()),
            slots: value
                .slots
                .clone()
                .into_iter()
                .map(|(u, v)| (Bytes::from(u), Some(Bytes::from(v))))
                .collect(),
            balance: value.balance.map(Bytes::from),
            code: value.code.clone(),
            change: value.change,
        }
    }
}

// Temporary until evm models are phased out
impl From<ContractDelta> for evm::AccountUpdate {
    fn from(value: ContractDelta) -> Self {
        Self {
            address: H160::from_slice(&value.address),
            chain: value.chain,
            slots: value
                .slots
                .into_iter()
                .map(|(k, v)| (k.into(), v.map(Into::into).unwrap_or_default()))
                .collect(),
            balance: value.balance.map(U256::from),
            code: value.code,
            change: value.change,
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

impl From<&evm::ProtocolStateDelta> for ProtocolComponentStateDelta {
    fn from(value: &ProtocolStateDelta) -> Self {
        Self {
            component_id: value.component_id.clone(),
            updated_attributes: value.updated_attributes.clone(),
            deleted_attributes: value.deleted_attributes.clone(),
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

impl From<&ComponentBalance> for evm::ComponentBalance {
    fn from(value: &ComponentBalance) -> Self {
        Self {
            token: value.token.clone().into(),
            balance: value.new_balance.clone(),
            balance_float: value.balance_float,
            modify_tx: value.modify_tx.clone().into(),
            component_id: value.component_id.clone(),
        }
    }
}

impl From<&evm::AggregatedBlockChanges> for BlockAggregatedDeltas {
    fn from(value: &evm::AggregatedBlockChanges) -> Self {
        let state_deltas = value
            .state_updates
            .iter()
            .map(|(cid, delta)| (cid.clone(), delta.into()))
            .collect::<HashMap<_, ProtocolComponentStateDelta>>();
        let account_deltas = value
            .account_updates
            .iter()
            .map(|(address, delta)| (address.as_bytes().into(), delta.into()))
            .collect::<HashMap<Bytes, ContractDelta>>();
        let new_components = value
            .new_protocol_components
            .iter()
            .map(|(cid, comp)| (cid.clone(), comp.into()))
            .collect::<HashMap<_, ProtocolComponent>>();
        let deleted_components = value
            .deleted_protocol_components
            .iter()
            .map(|(cid, comp)| (cid.clone(), comp.into()))
            .collect::<HashMap<_, ProtocolComponent>>();
        let balances = value
            .component_balances
            .iter()
            .map(|(cid, balance_map)| {
                (
                    cid.clone(),
                    balance_map
                        .iter()
                        .map(|(addr, balance)| (addr.as_bytes().into(), balance.into()))
                        .collect::<HashMap<Bytes, ComponentBalance>>(),
                )
            })
            .collect::<HashMap<_, _>>();
        Self::new(
            &value.extractor,
            value.chain,
            value.block.clone(),
            value.finalized_block_height,
            value.revert,
            &state_deltas,
            &account_deltas,
            &new_components,
            &deleted_components,
            &balances,
            &value.component_tvl,
        )
    }
}
