mod pb;

use std::collections::{hash_map::Entry, HashMap};

use hex_literal::hex;
use pb::protosim::evm::state::v1::{BlockStorageChanges, Changes, ContractStorageChange};
use substreams::{log, Hex};
use substreams_ethereum::pb::eth::{self};

const AMBIENT_CONTRACT: [u8; 20] = hex!("aaaaaaaaa24eeeb8d57d431224f73832bc34f688");

#[substreams::handlers::map]
fn map_changes(block: eth::v2::Block) -> Result<Changes, substreams::errors::Error> {
    let mut state = BlockStorageChanges {
        number: block.number,
        hash: block.hash.clone(),
        changes: Vec::new(),
    };

    // aggregate block changes
    let mut block_changes = HashMap::new();

    let mut relevant_changes: Vec<_> = block
        .calls()
        .flat_map(|v| {
            v.call
                .storage_changes
                .iter()
                .filter(|change| change.address == AMBIENT_CONTRACT)
        })
        .collect();

    relevant_changes.sort_unstable_by_key(|change| change.ordinal);

    for change in relevant_changes.into_iter() {
        let key = Hex(&change.key).to_string();
        let new_value = Hex(&change.new_value).to_string();
        let ordinal = change.ordinal;
        log::println(format!("ordinal={ordinal} Key={key} NewValue={new_value} "));
        match block_changes.entry(&change.address) {
            Entry::Vacant(e) => {
                let mut slots = HashMap::new();
                slots.insert(change.key.clone(), change.new_value.clone());
                e.insert(slots);
            }
            Entry::Occupied(mut e) => {
                let slots = e.get_mut();
                slots.insert(change.key.clone(), change.new_value.clone());
            }
        }
    }

    state.changes = block_changes
        .into_iter()
        .flat_map(|(address, slots)| {
            slots
                .into_iter()
                .map(move |(slot, value)| ContractStorageChange {
                    address: address.clone(),
                    slot,
                    value,
                    log_ordinal: 0,
                })
        })
        .collect();

    if state.changes.is_empty() {
        Ok(Changes { changes: vec![] })
    } else {
        Ok(Changes {
            changes: vec![state],
        })
    }
}
