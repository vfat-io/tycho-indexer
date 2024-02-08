use std::str::FromStr;

use ethabi::ethereum_types::Address;
use substreams_ethereum::pb::eth::v2::{self as eth};

use substreams_helper::{event_handler::EventHandler, hex::Hexable};

use crate::{
    abi::factory::events::PoolCreated,
    pb::tycho::evm::v1::{
        Attribute, BlockEntityChanges, ChangeType, FinancialType, ImplementationType,
        ProtocolComponent, ProtocolType, Transaction, TransactionEntityChanges,
    },
};

#[substreams::handlers::map]
pub fn map_pools_created(
    params: String,
    block: eth::Block,
) -> Result<BlockEntityChanges, substreams::errors::Error> {
    let mut new_pools: Vec<TransactionEntityChanges> = vec![];
    let factory_address = params.as_str();

    get_new_pools(&block, &mut new_pools, factory_address);

    Ok(BlockEntityChanges { block: Some(block.into()), changes: new_pools })
}

// Extract new pools from PoolCreated events
fn get_new_pools(
    block: &eth::Block,
    new_pools: &mut Vec<TransactionEntityChanges>,
    factory_address: &str,
) {
    // Extract new pools from PoolCreated events
    let mut on_pair_created = |event: PoolCreated, _tx: &eth::TransactionTrace, _log: &eth::Log| {
        let tycho_tx: Transaction = _tx.into();

        new_pools.push(TransactionEntityChanges {
            tx: Option::from(tycho_tx),
            entity_changes: vec![],
            component_changes: vec![ProtocolComponent {
                id: event.pool.to_hex(),
                tokens: vec![event.token0, event.token1],
                contracts: vec![event.pool],
                static_att: vec![
                    Attribute {
                        name: "fee".to_string(),
                        value: event.fee.to_signed_bytes_le(),
                        change: ChangeType::Creation.into(),
                    },
                    Attribute {
                        name: "tick_spacing".to_string(),
                        value: event.tick_spacing.to_signed_bytes_le(),
                        change: ChangeType::Creation.into(),
                    },
                ],
                change: i32::from(ChangeType::Creation),
                protocol_type: Option::from(ProtocolType {
                    name: "UniswapV3".to_string(),
                    financial_type: FinancialType::Swap.into(),
                    attribute_schema: vec![],
                    implementation_type: ImplementationType::Custom.into(),
                }),
            }],
            balance_changes: vec![],
        })
    };

    let mut eh = EventHandler::new(block);

    eh.filter_by_address(vec![Address::from_str(factory_address).unwrap()]);

    eh.on::<PoolCreated, _>(&mut on_pair_created);
    eh.handle_events();
}
