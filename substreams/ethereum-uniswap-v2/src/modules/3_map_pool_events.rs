use std::collections::HashMap;

use substreams::store::{StoreGet, StoreGetProto};
use substreams_ethereum::pb::eth::v2::{self as eth};

use substreams_helper::{event_handler::EventHandler, hex::Hexable};

use crate::{
    abi::pool::events::Sync,
    pb::tycho::evm::{
        v1,
        v1::{
            Attribute, BalanceChange, BlockEntityChanges, ChangeType, EntityChanges,
            ProtocolComponent, TransactionEntityChanges,
        },
    },
    store_key::StoreKey,
    traits::PoolAddresser,
};

#[derive(Clone, Hash, Eq, PartialEq)]
struct ComponentKey<T> {
    component_id: String,
    name: T,
}

impl<T> ComponentKey<T> {
    fn new(component_id: String, name: T) -> Self {
        ComponentKey { component_id, name }
    }
}

#[derive(Clone)]
struct PartialChanges {
    transaction: v1::Transaction,
    entity_changes: HashMap<ComponentKey<String>, Attribute>,
    balance_changes: HashMap<ComponentKey<Vec<u8>>, BalanceChange>,
}

impl PartialChanges {
    fn consolidate_entity_changes(self) -> Vec<EntityChanges> {
        let mut entity_changes_map: HashMap<String, Vec<Attribute>> = HashMap::new();

        for (key, attribute) in self.entity_changes {
            entity_changes_map
                .entry(key.component_id)
                .or_default()
                .push(attribute);
        }

        entity_changes_map
            .into_iter()
            .map(|(component_id, attributes)| EntityChanges { component_id, attributes })
            .collect()
    }
}

#[substreams::handlers::map]
pub fn map_pool_events(
    block: eth::Block,
    block_entity_changes: BlockEntityChanges,
    pools_store: StoreGetProto<ProtocolComponent>,
) -> Result<BlockEntityChanges, substreams::errors::Error> {
    // Sync event is sufficient for our use-case. Since it's emitted on every reserve-altering
    // function call, we can use it as the only event to update the reserves of a pool.
    let mut block_entity_changes = block_entity_changes;
    let mut tx_changes: HashMap<Vec<u8>, PartialChanges> = HashMap::new();

    handle_sync(&block, &mut tx_changes, &pools_store);
    merge_block(&mut tx_changes, &mut block_entity_changes);

    Ok(block_entity_changes)
}

fn handle_sync(
    block: &eth::Block,
    tx_changes: &mut HashMap<Vec<u8>, PartialChanges>,
    store: &StoreGetProto<ProtocolComponent>,
) {
    let mut on_sync = |event: Sync, _tx: &eth::TransactionTrace, _log: &eth::Log| {
        let pool_address_hex = _log.address.to_hex();

        let pool =
            store.must_get_last(StoreKey::Pool.get_unique_pool_key(pool_address_hex.as_str()));
        // Convert reserves to bytes
        let reserves_bytes =
            [event.reserve0.to_signed_bytes_le(), event.reserve1.to_signed_bytes_le()];

        let tx_change = tx_changes
            .entry(_tx.hash.clone())
            .or_insert_with(|| PartialChanges {
                transaction: _tx.into(),
                entity_changes: HashMap::new(),
                balance_changes: HashMap::new(),
            });

        for (i, reserve_bytes) in reserves_bytes.iter().enumerate() {
            let attribute_name = format!("reserve{}", i);
            // By using a HashMap, we can overwrite the previous value of the reserve attribute if
            // it is for the same pool and the same attribute name (reserves).
            tx_change.entity_changes.insert(
                ComponentKey::new(pool_address_hex.clone(), attribute_name.clone()),
                Attribute {
                    name: attribute_name,
                    value: reserve_bytes.clone(),
                    change: ChangeType::Update.into(),
                },
            );
        }

        // Update balance changes for each token
        for (index, token) in pool.tokens.iter().enumerate() {
            let balance = &reserves_bytes[index];
            // HashMap also prevents having duplicate balance changes for the same pool and token.
            tx_change.balance_changes.insert(
                ComponentKey::new(pool_address_hex.clone(), token.clone()),
                BalanceChange {
                    token: token.clone(),
                    balance: balance.clone(),
                    component_id: pool_address_hex.as_bytes().to_vec(),
                },
            );
        }
    };

    let mut eh = EventHandler::new(block);
    eh.filter_by_address(PoolAddresser { store });
    eh.on::<Sync, _>(&mut on_sync);
    eh.handle_events();
}

fn merge_block(
    tx_changes: &mut HashMap<Vec<u8>, PartialChanges>,
    block_entity_changes: &mut BlockEntityChanges,
) {
    // First, iterate through the previously created transactions, extracted from the
    // map_pool_created step. If there are sync events for this transaction, add them to the
    // block_entity_changes and the corresponding balance changes.
    for change in block_entity_changes.changes.iter_mut() {
        let tx = change.tx.as_mut().unwrap();
        // If there are sync events for this transaction, add them to the block_entity_changes
        if let Some(partial_changes) = tx_changes.remove(&tx.hash) {
            change.entity_changes = partial_changes
                .clone()
                .consolidate_entity_changes();
            change.balance_changes = partial_changes
                .balance_changes
                .into_values()
                .collect();
        }
    }

    for partial_changes in tx_changes.values() {
        block_entity_changes
            .changes
            .push(TransactionEntityChanges {
                tx: Some(partial_changes.transaction.clone()),
                entity_changes: partial_changes
                    .clone()
                    .consolidate_entity_changes(),
                balance_changes: partial_changes
                    .balance_changes
                    .clone()
                    .into_values()
                    .collect(),
                component_changes: vec![],
            });
    }
}
