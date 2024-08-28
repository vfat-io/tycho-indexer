use std::collections::HashMap;

use crate::extractor::{evm, evm::ProtocolStateDelta};
use tycho_core::{
    models::{
        blockchain::BlockAggregatedDeltas,
        contract::AccountUpdate,
        protocol::{ComponentBalance, ProtocolComponent, ProtocolComponentStateDelta},
    },
    Bytes,
};

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
            .map(|(address, delta)| (address.as_bytes().into(), delta.clone()))
            .collect::<HashMap<Bytes, AccountUpdate>>();
        let new_components = value
            .new_protocol_components
            .iter()
            .map(|(cid, comp)| (cid.clone(), comp.clone()))
            .collect::<HashMap<_, ProtocolComponent>>();
        let deleted_components = value
            .deleted_protocol_components
            .iter()
            .map(|(cid, comp)| (cid.clone(), comp.clone()))
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
