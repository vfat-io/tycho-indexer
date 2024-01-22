use std::collections::{hash_map::Entry, HashMap};

use substreams_ethereum::pb::eth::{self};

mod contracts;
use contracts::{
    hotproxy::{AMBIENT_HOTPROXY_CONTRACT, USER_CMD_HOTPROXY_FN_SIG},
    knockout::{decode_knockout_call, AMBIENT_KNOCKOUT_CONTRACT, USER_CMD_KNOCKOUT_FN_SIG},
    main::{decode_direct_swap_call, AMBIENT_CONTRACT, SWAP_FN_SIG, USER_CMD_FN_SIG},
    micropaths::{
        decode_burn_ambient_call, decode_burn_range_call, decode_mint_ambient_call,
        decode_mint_range_call, decode_sweep_swap_call, AMBIENT_MICROPATHS_CONTRACT,
        BURN_AMBIENT_FN_SIG, BURN_RANGE_FN_SIG, MINT_AMBIENT_FN_SIG, MINT_RANGE_FN_SIG,
        SWEEP_SWAP_FN_SIG,
    },
    warmpath::{
        decode_warm_path_user_cmd_call, AMBIENT_WARMPATH_CONTRACT, USER_CMD_WARMPATH_FN_SIG,
    },
};

use crate::contracts::{hotproxy::decode_direct_swap_hotproxy_call, main::decode_pool_init};
use pb::tycho::evm::v1 as tycho;

mod pb;
mod utils;

struct SlotValue {
    new_value: Vec<u8>,
    start_value: Vec<u8>,
}

impl SlotValue {
    fn has_changed(&self) -> bool {
        self.start_value != self.new_value
    }
}

// uses a map for slots, protobuf does not
// allow bytes in hashmap keys
struct InterimContractChange {
    address: Vec<u8>,
    balance: Vec<u8>,
    code: Vec<u8>,
    slots: HashMap<Vec<u8>, SlotValue>,
    change: tycho::ChangeType,
}

impl From<InterimContractChange> for tycho::ContractChange {
    fn from(value: InterimContractChange) -> Self {
        tycho::ContractChange {
            address: value.address,
            balance: value.balance,
            code: value.code,
            slots: value
                .slots
                .into_iter()
                .filter(|(_, value)| value.has_changed())
                .map(|(slot, value)| tycho::ContractSlot { slot, value: value.new_value })
                .collect(),
            change: value.change.into(),
        }
    }
}

/// Extracts all contract changes relevant to vm simulations
///
/// This implementation has currently two major limitations:
/// 1. It is hardwired to only care about changes to the ambient main contract, this is ok for this
///    particular use case but for a more general purpose implementation this is not ideal
/// 2. Changes are processed separately, this means that if there are any side effects between each
///    other (e.g. if account is deleted and then created again in ethereum all the storage is set
///    to 0. So there is a side effect between account creation and contract storage.) these might
///    not be properly accounted for. Most of the time this should not be a major issue but may lead
///    to wrong results so consume this implementation with care. See example below for a concrete
///    case where this is problematic.
///
/// ## A very contrived example:
/// 1. Some existing contract receives a transaction that changes it state, the state is updated
/// 2. Next, this contract has self destruct called on itself
/// 3. The contract is created again using CREATE2 at the same address
/// 4. The contract receives a transaction that changes it state
/// 5. We would emit this as as contract creation with slots set from 1 and from 4, although we
///    should only emit the slots changed from 4.
#[substreams::handlers::map]
fn map_changes(
    block: eth::v2::Block,
) -> Result<tycho::BlockContractChanges, substreams::errors::Error> {
    let mut block_changes = tycho::BlockContractChanges::default();

    let mut tx_change = tycho::TransactionContractChanges::default();

    let mut changed_contracts: HashMap<Vec<u8>, InterimContractChange> = HashMap::new();

    let created_accounts: HashMap<_, _> = block
        .transactions()
        .flat_map(|tx| {
            tx.calls.iter().flat_map(|call| {
                call.account_creations
                    .iter()
                    .map(|ac| (&ac.account, ac.ordinal))
            })
        })
        .collect();

    for block_tx in block.transactions() {
        // extract storage changes
        let mut storage_changes = block_tx
            .calls
            .iter()
            .filter(|call| !call.state_reverted)
            .flat_map(|call| {
                call.storage_changes
                    .iter()
                    .filter(|c| c.address == AMBIENT_CONTRACT)
            })
            .collect::<Vec<_>>();
        storage_changes.sort_unstable_by_key(|change| change.ordinal);

        let block_calls = block_tx
            .calls
            .iter()
            .filter(|call| !call.state_reverted)
            .collect::<Vec<_>>();

        for call in block_calls {
            if call.input.len() < 4 {
                continue;
            }
            if call.address == AMBIENT_CONTRACT && call.input[0..4] == USER_CMD_FN_SIG {
                // Extract pool creations
                if let Some(protocol_component) = decode_pool_init(call)? {
                    // Handle the case when Some is returned
                    tx_change
                        .component_changes
                        .push(protocol_component);
                }
            } else if
            // Handle TVL changes caused by calling the swap function
            call.address == AMBIENT_CONTRACT && call.input[0..4] == SWAP_FN_SIG {
                // TODO: aggregate these with the previous balances to get new balances:
                let (_pool_hash, _base_flow, _quote_flow) = decode_direct_swap_call(call)?;
            } else if
            // Handle TVL changes caused by calling the userCmd method on the HotProxy contract
            call.address == AMBIENT_HOTPROXY_CONTRACT &&
                call.input[0..4] == USER_CMD_HOTPROXY_FN_SIG
            {
                // TODO: aggregate these with the previous balances to get new balances:
                let (_pool_hash, _base_flow, _quote_flow) = decode_direct_swap_hotproxy_call(call)?;
            } else if call.address == AMBIENT_MICROPATHS_CONTRACT &&
                call.input[0..4] == SWEEP_SWAP_FN_SIG
            {
                // Handle TVL changes caused by calling the sweepSwap method on the MicroPaths
                // contract
                // TODO: aggregate these flows with the previous balances to get new balances:
                let (_pool_hash, _base_flow, _quote_flow) = decode_sweep_swap_call(call)?;
            } else if call.address == AMBIENT_WARMPATH_CONTRACT &&
                call.input[0..4] == USER_CMD_WARMPATH_FN_SIG
            {
                // Handle TVL changes caused by mints, burns, or harvest when calling the userCmd
                // method on the WarmPath contract.
                let code = call.input[35];
                let is_mint =
                    code == 1 || code == 11 || code == 12 || code == 3 || code == 31 || code == 32;
                let is_burn =
                    code == 2 || code == 21 || code == 22 || code == 4 || code == 41 || code == 42;
                let is_harvest = code == 5;
                let extract_tvl = is_mint || is_burn || is_harvest;
                if extract_tvl {
                    // TODO: aggregate these flows with the previous balances to get new balances:
                    let (_pool_hash, _base_flow, _quote_flow) =
                        decode_warm_path_user_cmd_call(call)?;
                }
            } else if call.address == AMBIENT_MICROPATHS_CONTRACT &&
                call.input[0..4] == MINT_RANGE_FN_SIG
            {
                // Handle TVL changes on mintRange() calls to the MicroPaths contract
                // TODO: aggregate these flows with the previous balances to get new balances:
                let (_pool_hash, _base_flow, _quote_flow) = decode_mint_range_call(call)?;
            } else if call.address == AMBIENT_MICROPATHS_CONTRACT &&
                call.input[0..4] == MINT_AMBIENT_FN_SIG
            {
                // Handle TVL changes on mintAmbient() calls to the MicroPaths contract
                // TODO: aggregate these flows with the previous balances to get new balances:
                let (_pool_hash, _base_flow, _quote_flow) = decode_mint_ambient_call(call)?;
            } else if call.address == AMBIENT_MICROPATHS_CONTRACT &&
                call.input[0..4] == BURN_RANGE_FN_SIG
            {
                // Handle TVL changes on burnRange() calls to the MicroPaths contract
                // TODO: aggregate these flows with the previous balances to get new balances:
                let (_pool_hash, _base_flow, _quote_flow) = decode_burn_range_call(call)?;
            } else if call.address == AMBIENT_MICROPATHS_CONTRACT &&
                call.input[0..4] == BURN_AMBIENT_FN_SIG
            {
                // Handle TVL changes on burnAmbient() calls to the MicroPaths contract
                // TODO: aggregate these flows with the previous balances to get new balances:
                let (_pool_hash, _base_flow, _quote_flow) = decode_burn_ambient_call(call)?;
            } else if call.address == AMBIENT_KNOCKOUT_CONTRACT &&
                call.input[0..4] == USER_CMD_KNOCKOUT_FN_SIG
            {
                // Handle TVL changes on userCmd() calls to the KnockoutLiqPath contract
                // TODO: aggregate these flows with the previous balances to get new balances:
                let (_pool_hash, _base_flow, _quote_flow) = decode_knockout_call(call)?;
            }
        }

        // Note: some contracts change slot values and change them back to their
        //  original value before the transactions ends we remember the initial
        //  value before the first change and in the end filter found deltas
        //  that ended up not actually changing anything.
        for storage_change in storage_changes.iter() {
            match changed_contracts.entry(storage_change.address.clone()) {
                // We have already an entry recording a change about this contract
                //  only append the change about this storage slot
                Entry::Occupied(mut e) => {
                    let contract_change = e.get_mut();
                    match contract_change
                        .slots
                        .entry(storage_change.key.clone())
                    {
                        // The storage slot was already changed before, simply
                        //  update new_value
                        Entry::Occupied(mut v) => {
                            let slot_value = v.get_mut();
                            slot_value
                                .new_value
                                .copy_from_slice(&storage_change.new_value);
                        }
                        // The storage slots is being initialised for the first time
                        Entry::Vacant(v) => {
                            v.insert(SlotValue {
                                new_value: storage_change.new_value.clone(),
                                start_value: storage_change.old_value.clone(),
                            });
                        }
                    }
                }
                // Intialise a new contract change after observing a storage change
                Entry::Vacant(e) => {
                    let mut slots = HashMap::new();
                    slots.insert(
                        storage_change.key.clone(),
                        SlotValue {
                            new_value: storage_change.new_value.clone(),
                            start_value: storage_change.old_value.clone(),
                        },
                    );
                    e.insert(InterimContractChange {
                        address: storage_change.address.clone(),
                        balance: Vec::new(),
                        code: Vec::new(),
                        slots,
                        change: if created_accounts.contains_key(&storage_change.address) {
                            tycho::ChangeType::Creation
                        } else {
                            tycho::ChangeType::Update
                        },
                    });
                }
            }
        }

        // extract balance changes
        let mut balance_changes = block_tx
            .calls
            .iter()
            .filter(|call| !call.state_reverted)
            .flat_map(|call| {
                call.balance_changes
                    .iter()
                    .filter(|c| c.address == AMBIENT_CONTRACT)
            })
            .collect::<Vec<_>>();
        balance_changes.sort_unstable_by_key(|change| change.ordinal);

        for balance_change in balance_changes.iter() {
            match changed_contracts.entry(balance_change.address.clone()) {
                Entry::Occupied(mut e) => {
                    let contract_change = e.get_mut();
                    if let Some(new_balance) = &balance_change.new_value {
                        contract_change.balance.clear();
                        contract_change
                            .balance
                            .extend_from_slice(&new_balance.bytes);
                    }
                }
                Entry::Vacant(e) => {
                    if let Some(new_balance) = &balance_change.new_value {
                        e.insert(InterimContractChange {
                            address: balance_change.address.clone(),
                            balance: new_balance.bytes.clone(),
                            code: Vec::new(),
                            slots: HashMap::new(),
                            change: if created_accounts.contains_key(&balance_change.address) {
                                tycho::ChangeType::Creation
                            } else {
                                tycho::ChangeType::Update
                            },
                        });
                    }
                }
            }
        }

        // extract code changes
        let mut code_changes = block_tx
            .calls
            .iter()
            .filter(|call| !call.state_reverted)
            .flat_map(|call| {
                call.code_changes
                    .iter()
                    .filter(|c| c.address == AMBIENT_CONTRACT)
            })
            .collect::<Vec<_>>();
        code_changes.sort_unstable_by_key(|change| change.ordinal);

        for code_change in code_changes.iter() {
            match changed_contracts.entry(code_change.address.clone()) {
                Entry::Occupied(mut e) => {
                    let contract_change = e.get_mut();
                    contract_change.code.clear();
                    contract_change
                        .code
                        .extend_from_slice(&code_change.new_code);
                }
                Entry::Vacant(e) => {
                    e.insert(InterimContractChange {
                        address: code_change.address.clone(),
                        balance: Vec::new(),
                        code: code_change.new_code.clone(),
                        slots: HashMap::new(),
                        change: if created_accounts.contains_key(&code_change.address) {
                            tycho::ChangeType::Creation
                        } else {
                            tycho::ChangeType::Update
                        },
                    });
                }
            }
        }

        // if there were any changes, add transaction and push the changes
        if !storage_changes.is_empty() || !balance_changes.is_empty() || !code_changes.is_empty() {
            tx_change.tx = Some(tycho::Transaction {
                hash: block_tx.hash.clone(),
                from: block_tx.from.clone(),
                to: block_tx.to.clone(),
                index: block_tx.index as u64,
            });

            // reuse changed_contracts hash map by draining it, next iteration
            // will start empty. This avoids a costly reallocation
            for (_, change) in changed_contracts.drain() {
                tx_change
                    .contract_changes
                    .push(change.into())
            }

            block_changes
                .changes
                .push(tx_change.clone());

            // clear out the interim contract changes after we pushed those.
            tx_change.tx = None;
            tx_change.contract_changes.clear();
        }
    }

    block_changes.block = Some(tycho::Block {
        number: block.number,
        hash: block.hash.clone(),
        parent_hash: block
            .header
            .as_ref()
            .expect("Block header not present")
            .parent_hash
            .clone(),
        ts: block.timestamp_seconds(),
    });

    Ok(block_changes)
}
