use std::collections::HashMap;

use crate::extractor::{evm, evm::ProtocolStateDelta};
use ethers::prelude::{H160, U256};
use tycho_core::{
    models::{
        blockchain::BlockAggregatedDeltas,
        contract::AccountUpdate,
        protocol::{ComponentBalance, ProtocolComponent, ProtocolComponentStateDelta},
    },
    Bytes,
};

impl From<&evm::AccountUpdate> for AccountUpdate {
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
impl From<AccountUpdate> for evm::AccountUpdate {
    fn from(value: AccountUpdate) -> Self {
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
            .collect::<HashMap<Bytes, AccountUpdate>>();
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
                        .map(|(addr, balance)| (addr.as_bytes().into(), balance.clone()))
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
