use std::collections::{HashMap, HashSet};

use ethers::types::U256;
use tycho_core::Bytes;

use crate::extractor::evm::{BlockEntityChanges, ProtocolStateDelta};

const USV3_MANDATORY_ATTRIBUTES: [&str; 3] = ["liquidity", "tick", "sqrt_price_x96"];

/// Post processor function that adds missing attributes to all new created uniswapV3 pools.
pub fn add_default_attributes_uniswapv3(mut changes: BlockEntityChanges) -> BlockEntityChanges {
    // TODO: Remove it while this is handled directly in the substreams modules.
    for tx in &mut changes.txs_with_update {
        for c_id in tx.new_protocol_components.keys() {
            if let Some(state) = tx.protocol_states.get_mut(c_id) {
                for mandatory_attr in USV3_MANDATORY_ATTRIBUTES {
                    if !state
                        .updated_attributes
                        .contains_key(mandatory_attr)
                    {
                        state
                            .updated_attributes
                            .insert(mandatory_attr.to_string(), Bytes::from(U256::zero()));
                    }
                }
            } else {
                let mut default_attr = HashMap::new();
                for mandatory_attr in USV3_MANDATORY_ATTRIBUTES {
                    default_attr.insert(mandatory_attr.to_string(), Bytes::from(U256::zero()));
                }
                tx.protocol_states.insert(
                    c_id.clone(),
                    ProtocolStateDelta {
                        component_id: c_id.clone(),
                        updated_attributes: default_attr,
                        deleted_attributes: HashSet::new(),
                    },
                );
            }
        }
    }
    changes
}

#[cfg(test)]
mod test {
    use crate::extractor::evm::{ProtocolChangesWithTx, Transaction};
    use ethers::types::{H160, H256};
    use std::{
        collections::{HashMap, HashSet},
        str::FromStr,
    };
    use tycho_core::{models::Chain, Bytes};

    use crate::extractor::evm;

    use super::add_default_attributes_uniswapv3;
    const BLOCK_HASH_0: &str = "0x98b4a4fef932b1862be52de218cc32b714a295fae48b775202361a6fa09b66eb";
    const CREATED_CONTRACT: &str = "0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc";

    #[test]
    fn test_uniswap_v3_default_attributes() {
        // Test that uniswap V3 post processor insert mandatory attributes with a default value on
        // new pools detected.
        let changes = evm::BlockEntityChanges::new(
            "native:test".to_owned(),
            Chain::Ethereum,
            evm::Block {
                number: 0,
                chain: Chain::Ethereum,
                hash: BLOCK_HASH_0.parse().unwrap(),
                parent_hash: BLOCK_HASH_0.parse().unwrap(),
                ts: "2020-01-01T01:00:00".parse().unwrap(),
            },
            false,
            vec![ProtocolChangesWithTx {
                tx: Transaction::new(
                    H256::zero(),
                    BLOCK_HASH_0.parse().unwrap(),
                    H160::zero(),
                    Some(H160::zero()),
                    10,
                ),
                protocol_states: HashMap::from([(
                    CREATED_CONTRACT.to_string(),
                    evm::ProtocolStateDelta {
                        component_id: CREATED_CONTRACT.to_string(),
                        updated_attributes: HashMap::from([(
                            "tick".to_string(),
                            Bytes::from(1_u64.to_be_bytes()),
                        )]),
                        deleted_attributes: HashSet::new(),
                    },
                )]),
                balance_changes: HashMap::new(),
                new_protocol_components: HashMap::from([(
                    CREATED_CONTRACT.to_string(),
                    evm::ProtocolComponent {
                        id: CREATED_CONTRACT.to_string(),
                        protocol_system: "test".to_string(),
                        protocol_type_name: "Pool".to_string(),
                        chain: Chain::Ethereum,
                        tokens: vec![
                            H160::from_str("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48").unwrap(),
                            H160::from_str("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2").unwrap(),
                        ],
                        contract_ids: vec![],
                        creation_tx: Default::default(),
                        static_attributes: Default::default(),
                        created_at: Default::default(),
                        change: Default::default(),
                    },
                )]),
            }],
        );

        let expected = evm::BlockEntityChanges::new(
            "native:test".to_owned(),
            Chain::Ethereum,
            changes.block.clone(),
            changes.revert.clone(),
            vec![ProtocolChangesWithTx {
                tx: changes
                    .txs_with_update
                    .first()
                    .unwrap()
                    .tx
                    .clone(),
                protocol_states: HashMap::from([(
                    CREATED_CONTRACT.to_string(),
                    evm::ProtocolStateDelta {
                        component_id: CREATED_CONTRACT.to_string(),
                        updated_attributes: HashMap::from([
                            ("tick".to_string(), Bytes::from(1_u64.to_be_bytes())),
                            ("sqrt_price_x96".to_string(), Bytes::from(H256::zero())),
                            ("liquidity".to_string(), Bytes::from(H256::zero())),
                        ]),
                        deleted_attributes: HashSet::new(),
                    },
                )]),
                balance_changes: HashMap::new(),
                new_protocol_components: changes
                    .txs_with_update
                    .first()
                    .unwrap()
                    .new_protocol_components
                    .clone(),
            }],
        );

        let updated_changes = add_default_attributes_uniswapv3(changes);

        assert_eq!(updated_changes, expected);
    }

    #[test]
    fn test_uniswap_v3_default_attributes_no_new_pools() {
        // Test that uniswap V3 post processor does nothing when no new pools are detected.
        let changes = evm::BlockEntityChanges::new(
            "native:test".to_owned(),
            Chain::Ethereum,
            evm::Block {
                number: 0,
                chain: Chain::Ethereum,
                hash: BLOCK_HASH_0.parse().unwrap(),
                parent_hash: BLOCK_HASH_0.parse().unwrap(),
                ts: "2020-01-01T01:00:00".parse().unwrap(),
            },
            false,
            vec![ProtocolChangesWithTx {
                tx: Transaction::new(
                    H256::zero(),
                    BLOCK_HASH_0.parse().unwrap(),
                    H160::zero(),
                    Some(H160::zero()),
                    10,
                ),
                protocol_states: HashMap::from([(
                    CREATED_CONTRACT.to_string(),
                    evm::ProtocolStateDelta {
                        component_id: CREATED_CONTRACT.to_string(),
                        updated_attributes: HashMap::from([(
                            "tick".to_string(),
                            Bytes::from(1_u64.to_be_bytes()),
                        )]),
                        deleted_attributes: HashSet::new(),
                    },
                )]),
                balance_changes: HashMap::new(),
                new_protocol_components: HashMap::new(),
            }],
        );

        let updated_changes = add_default_attributes_uniswapv3(changes.clone());

        assert_eq!(updated_changes, changes);
    }
}
