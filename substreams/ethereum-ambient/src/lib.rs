use std::collections::{hash_map::Entry, HashMap};

use anyhow::{anyhow, bail};

use ethabi::{decode, ParamType};
use hex_literal::hex;
use substreams_ethereum::pb::eth::{self, v2::Call};
use tiny_keccak::{Hasher, Keccak};

use pb::tycho::evm::v1::{self as tycho};

mod pb;
const AMBIENT_CONTRACT: [u8; 20] = hex!("aaaaaaaaa24eeeb8d57d431224f73832bc34f688");
const AMBIENT_HOTPROXY_CONTRACT: [u8; 20] = hex!("37e00522Ce66507239d59b541940F99eA19fF81F");
const AMBIENT_MICROPATHS_CONTRACT: [u8; 20] = hex!("f241bEf0Ea64020655C70963ef81Fea333752367");
const AMBIENT_WARMPATH_CONTRACT: [u8; 20] = hex!("d268767BE4597151Ce2BB4a70A9E368ff26cB195");
const AMBIENT_KNOCKOUT_CONTRACT: [u8; 20] = hex!("7F5D75AdE75646919c923C98D53E9Cc7Be7ea794");

const INIT_POOL_CODE: u8 = 71;
const USER_CMD_FN_SIG: [u8; 4] = hex!("a15112f9");
const USER_CMD_WARMPATH_FN_SIG: [u8; 4] = hex!("f96dc788");
const USER_CMD_HOTPROXY_FN_SIG: [u8; 4] = hex!("f96dc788");
const USER_CMD_KNOCKOUT_FN_SIG: [u8; 4] = hex!("f96dc788");
const SWEEP_SWAP_FN_SIG: [u8; 4] = hex!("7b370fc2");

// MicroPaths fn sigs
const SWAP_FN_SIG: [u8; 4] = hex!("3d719cd9");
const MINT_RANGE_FN_SIG: [u8; 4] = hex!("2370632b");
const MINT_AMBIENT_FN_SIG: [u8; 4] = hex!("2ee11587");
const BURN_AMBIENT_FN_SIG: [u8; 4] = hex!("2a6f0864");
const BURN_RANGE_FN_SIG: [u8; 4] = hex!("7c6dfe3d");

const SWAP_ABI_INPUT: &[ParamType] = &[
    ParamType::Address,   // base
    ParamType::Address,   // quote
    ParamType::Uint(256), // pool index
    // isBuy - if true the direction of the swap is for the user to send base
    // tokens and receive back quote tokens.
    ParamType::Bool,
    ParamType::Bool,      // inBaseQty
    ParamType::Uint(128), //qty
    ParamType::Uint(16),  // poolTip
    ParamType::Uint(128), // limitPrice
    ParamType::Uint(128), // minOut
    ParamType::Uint(8),   // reserveFlags
];

const BASE_QUOTE_FLOW_OUTPUT: &[ParamType] = &[
    // The token base and quote token flows associated with this swap action.
    // Negative indicates a credit paid to the user (token balance of pool
    // decreases), positive a debit collected from the user (token balance of pool
    // increases).
    ParamType::Int(128), // baseFlow
    ParamType::Int(128), // quoteFlow
];

const LIQUIDITY_CHANGE_ABI: &[ParamType] = &[
    ParamType::Uint(8),
    ParamType::Address,   // base
    ParamType::Address,   // quote
    ParamType::Uint(256), // pool index
    ParamType::Int(256),
    ParamType::Uint(128),
    ParamType::Uint(128),
    ParamType::Uint(128),
    ParamType::Uint(8),
    ParamType::Address,
];

// ABI for the mintRange function parameters
const MINT_RANGE_ABI: &[ParamType] = &[
    ParamType::Uint(128),      //  price
    ParamType::Int(24),        //  priceTick
    ParamType::Uint(128),      //  seed
    ParamType::Uint(128),      //  conc
    ParamType::Uint(64),       //  seedGrowth
    ParamType::Uint(64),       //  concGrowth
    ParamType::Int(24),        //  lowTick
    ParamType::Int(24),        //  highTick
    ParamType::Uint(128),      //  liq
    ParamType::FixedBytes(32), //  poolHash
];

// ABI for the mintRange function with return values
const MINT_RANGE_RETURN_ABI: &[ParamType] = &[
    ParamType::Int(128),  //  baseFlow
    ParamType::Int(128),  //  quoteFlow
    ParamType::Uint(128), //  seedOut
    ParamType::Uint(128), //  concOut
];

// ABI for the burnAmbient function
const BURN_AMBIENT_ABI: &[ParamType] = &[
    ParamType::Uint(128),      // uint128 price
    ParamType::Uint(128),      // uint128 seed
    ParamType::Uint(128),      // uint128 conc
    ParamType::Uint(64),       // uint64 seedGrowth
    ParamType::Uint(64),       // uint64 concGrowth
    ParamType::Uint(128),      // uint128 liq
    ParamType::FixedBytes(32), // bytes32 poolHash
];

// ABI for the burnAmbient function with return values
const BURN_AMBIENT_RETURN_ABI: &[ParamType] = &[
    ParamType::Int(128),  // int128 baseFlow
    ParamType::Int(128),  // int128 quoteFlow
    ParamType::Uint(128), // uint128 seedOut
];

// ABI for the mintAmbient function parameters
const MINT_AMBIENT_ABI: &[ParamType] = &[
    ParamType::Uint(128),      // uint128 price
    ParamType::Uint(128),      // uint128 seed
    ParamType::Uint(128),      // uint128 conc
    ParamType::Uint(64),       // uint64 seedGrowth
    ParamType::Uint(64),       // uint64 concGrowth
    ParamType::Uint(128),      // uint128 liq
    ParamType::FixedBytes(32), // bytes32 poolHash
];

// ABI for the mintAmbient function with return values
const MINT_AMBIENT_RETURN_ABI: &[ParamType] = &[
    ParamType::Int(128),  // int128 baseFlow
    ParamType::Int(128),  // int128 quoteFlow
    ParamType::Uint(128), // uint128 seedOut
];

// ABI for the burnRange function
const BURN_RANGE_ABI: &[ParamType] = &[
    ParamType::Uint(128),      // price
    ParamType::Int(24),        // priceTick
    ParamType::Uint(128),      // seed
    ParamType::Uint(128),      // conc
    ParamType::Uint(64),       // seedGrowth
    ParamType::Uint(64),       // concGrowth
    ParamType::Int(24),        // lowTick
    ParamType::Int(24),        // highTick
    ParamType::Uint(128),      // liq
    ParamType::FixedBytes(32), // poolHash
];

const BURN_RANGE_RETURN_ABI: &[ParamType] = &[
    ParamType::Int(128),  // baseFlow
    ParamType::Int(128),  // quoteFlow
    ParamType::Uint(128), // seedOut
    ParamType::Uint(128), // concOut
];

const KNOCKOUT_EXTERNAL_ABI: &[ParamType] = &[
    ParamType::Bytes, // userCmd
];
const KNOCKOUT_INTERNAL_MINT_BURN_ABI: &[ParamType] = &[
    ParamType::Uint(8),
    ParamType::Address,   // base
    ParamType::Address,   // quote
    ParamType::Uint(256), // poolIdx
    ParamType::Int(24),
    ParamType::Int(24),
    ParamType::Bool,
    ParamType::Uint(8),
    ParamType::Uint(256),
    ParamType::Uint(256),
    ParamType::Uint(128),
    ParamType::Bool,
];

// Represents the ABI of any cmd which is not mint or burn
const KNOCKOUT_INTERNAL_OTHER_CMD_ABI: &[ParamType] = &[
    ParamType::Uint(8),
    ParamType::Address,   // base
    ParamType::Address,   // quote
    ParamType::Uint(256), // poolIdx
    ParamType::Int(24),
    ParamType::Int(24),
    ParamType::Bool,
    ParamType::Uint(8),
    ParamType::Uint(256),
    ParamType::Uint(256),
    ParamType::Uint(32),
];

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
                let user_cmd_external_abi_types = &[
                    // index of the proxy sidecar the command is being called on
                    ParamType::Uint(16),
                    // call data for internal UserCmd method
                    ParamType::Bytes,
                ];
                let user_cmd_internal_abi_types = &[
                    ParamType::Uint(8),   // command
                    ParamType::Address,   // base
                    ParamType::Address,   // quote
                    ParamType::Uint(256), // pool index
                    ParamType::Uint(128), // price
                ];

                // Decode external call to UserCmd
                if let Ok(external_params) = decode(user_cmd_external_abi_types, &call.input[4..]) {
                    let cmd_bytes = external_params[1]
                        .to_owned()
                        .into_bytes()
                        .ok_or_else(|| {
                            anyhow!("Failed to convert to bytes: {:?}", &external_params[1])
                        })?;

                    // Call data is structured differently depending on the cmd code, so only
                    // decode if this is an init pool code.
                    if cmd_bytes[31] == INIT_POOL_CODE {
                        // Decode internal call to UserCmd
                        if let Ok(internal_params) = decode(user_cmd_internal_abi_types, &cmd_bytes)
                        {
                            let base = internal_params[1]
                                .to_owned()
                                .into_address()
                                .ok_or_else(|| {
                                    anyhow!(
                                        "Failed to convert to address: {:?}",
                                        &internal_params[1]
                                    )
                                })?
                                .to_fixed_bytes()
                                .to_vec();

                            let quote = internal_params[2]
                                .to_owned()
                                .into_address()
                                .ok_or_else(|| {
                                    anyhow!(
                                        "Failed to convert to address: {:?}",
                                        &internal_params[2]
                                    )
                                })?
                                .to_fixed_bytes()
                                .to_vec();

                            let pool_index = internal_params[3]
                                .to_owned()
                                .into_uint()
                                .ok_or_else(|| anyhow!("Failed to convert to u32".to_string()))?
                                .as_u32();

                            let static_attribute = tycho::Attribute {
                                name: String::from("pool_index"),
                                value: pool_index.to_be_bytes().to_vec(),
                                change: tycho::ChangeType::Creation.into(),
                            };

                            let mut tokens: Vec<Vec<u8>> = vec![base.clone(), quote.clone()];
                            tokens.sort();

                            let new_component = tycho::ProtocolComponent {
                                id: format!(
                                    "{}{}{}",
                                    hex::encode(base.clone()),
                                    hex::encode(quote.clone()),
                                    pool_index
                                ),
                                tokens,
                                contracts: vec![AMBIENT_CONTRACT.to_vec()],
                                static_att: vec![static_attribute],
                                change: tycho::ChangeType::Creation.into(),
                                protocol_type: Some(tycho::ProtocolType {
                                    name: "Ambient".to_string(),
                                    attribute_schema: vec![],
                                    financial_type: 0,
                                    implementation_type: 0,
                                }),
                            };
                            tx_change
                                .component_changes
                                .push(new_component);
                        } else {
                            bail!("Failed to decode ABI internal call.".to_string());
                        }
                    }
                } else {
                    bail!("Failed to decode ABI external call.".to_string());
                }
            } else if
            // Handle TVL changes caused by calling the swap function
            (call.address == AMBIENT_CONTRACT && call.input[0..4] == SWAP_FN_SIG) ||
                // Handle TVL changes caused by calling the userCmd method on the HotProxy contract
                (call.address == AMBIENT_HOTPROXY_CONTRACT &&
                    call.input[0..4] == USER_CMD_HOTPROXY_FN_SIG)
            {
                // TODO: aggregate these with the previous balances to get new balances:
                let (_pool_hash, _base_flow, _quote_flow) = decode_direct_swap_call(call)?;
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
                if is_mint || is_burn || is_harvest {
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

fn encode_pool_hash(token_x: Vec<u8>, token_y: Vec<u8>, pool_idx: u32) -> [u8; 32] {
    let mut keccak = Keccak::v256();

    let mut data = Vec::new();
    data.extend_from_slice(&token_x);
    data.extend_from_slice(&token_y);
    data.extend_from_slice(&pool_idx.to_be_bytes());

    let mut output = [0u8; 32];
    keccak.update(&data);
    keccak.finalize(&mut output);
    output
}

fn decode_direct_swap_call(
    call: &Call,
) -> Result<([u8; 32], ethabi::Int, ethabi::Int), anyhow::Error> {
    if let Ok(external_input_params) = decode(SWAP_ABI_INPUT, &call.input[4..]) {
        let base_token = external_input_params[0]
            .to_owned()
            .into_address()
            .ok_or_else(|| {
                anyhow!(
                    "Failed to convert base token to address for direct swap call: {:?}",
                    &external_input_params[0]
                )
            })?
            .to_fixed_bytes()
            .to_vec();

        let quote_token = external_input_params[1]
            .to_owned()
            .into_address()
            .ok_or_else(|| {
                anyhow!(
                    "Failed to convert quote token to address for direct swap call: {:?}",
                    &external_input_params[1]
                )
            })?
            .to_fixed_bytes()
            .to_vec();

        let pool_index = external_input_params[2]
            .to_owned()
            .into_uint()
            .ok_or_else(|| {
                anyhow!("Failed to convert pool index to u32 for direct swap call".to_string())
            })?
            .as_u32();

        let (base_flow, quote_flow) = decode_flows_from_output(call)?;
        let pool_hash = encode_pool_hash(base_token, quote_token, pool_index);
        Ok((pool_hash, base_flow, quote_flow))
    } else {
        bail!("Failed to decode swap call inputs.".to_string());
    }
}

fn decode_sweep_swap_call(
    call: &Call,
) -> Result<(Vec<u8>, ethabi::Int, ethabi::Int), anyhow::Error> {
    let sweep_swap_abi: &[ParamType] = &[
        ParamType::Tuple(vec![
            ParamType::Uint(128),
            ParamType::Uint(128),
            ParamType::Uint(128),
            ParamType::Uint(64),
            ParamType::Uint(64),
        ]), // CurveState
        ParamType::Int(24), // midTick
        ParamType::Tuple(vec![
            ParamType::Bool,
            ParamType::Bool,
            ParamType::Uint(8),
            ParamType::Uint(128),
            ParamType::Uint(128),
        ]), // SwapDirective
        ParamType::Tuple(vec![
            ParamType::Tuple(vec![
                ParamType::Uint(8),  // schema
                ParamType::Uint(16), // feeRate
                ParamType::Uint(8),  // protocolTake
                ParamType::Uint(16), // tickSize
                ParamType::Uint(8),  // jitThresh
                ParamType::Uint(8),  // knockoutBits
                ParamType::Uint(8),  // oracleFlags
            ]),
            ParamType::FixedBytes(32), // poolHash
            ParamType::Address,
        ]), // PoolCursor
    ];
    let sweep_swap_abi_output: &[ParamType] = &[
        ParamType::Tuple(vec![
            ParamType::Int(128), // baseFlow
            ParamType::Int(128), // quoteFlow
            ParamType::Uint(128),
            ParamType::Uint(128),
        ]), // Chaining.PairFlow memory accum
        ParamType::Uint(128), // priceOut
        ParamType::Uint(128), // seedOut
        ParamType::Uint(128), // concOut
        ParamType::Uint(64),  // ambientOut
        ParamType::Uint(64),  // concGrowthOut
    ];
    if let Ok(sweep_swap_input) = decode(sweep_swap_abi, &call.input[4..]) {
        let pool_cursor = sweep_swap_input[3]
            .to_owned()
            .into_tuple()
            .ok_or_else(|| {
                anyhow!("Failed to convert pool cursor to tuple for sweepSwap call".to_string())
            })?;
        let pool_hash = pool_cursor[1]
            .to_owned()
            .into_fixed_bytes()
            .ok_or_else(|| {
                anyhow!("Failed to convert pool hash to fixed bytes for sweepSwap call".to_string())
            })?;
        if let Ok(sweep_swap_output) = decode(sweep_swap_abi_output, &call.return_data) {
            let pair_flow = sweep_swap_output[0]
                .to_owned()
                .into_tuple()
                .ok_or_else(|| {
                    anyhow!("Failed to convert pair flow to tuple for sweepSwap call".to_string())
                })?;

            let base_flow = pair_flow[0]
                .to_owned()
                .into_int() // Needs conversion into bytes for next step
                .ok_or_else(|| {
                    anyhow!("Failed to convert base flow to i128 for sweepSwap call".to_string())
                })?;

            let quote_flow = pair_flow[1]
                .to_owned()
                .into_int() // Needs conversion into bytes for next step
                .ok_or_else(|| {
                    anyhow!("Failed to convert quote flow to i128 for sweepSwap call".to_string())
                })?;

            Ok((pool_hash, base_flow, quote_flow))
        } else {
            bail!("Failed to decode sweepSwap outputs.".to_string());
        }
    } else {
        bail!("Failed to decode sweepSwap inputs.".to_string());
    }
}

fn decode_warm_path_user_cmd_call(
    call: &Call,
) -> Result<([u8; 32], ethabi::Int, ethabi::Int), anyhow::Error> {
    if let Ok(liquidity_change_calldata) = decode(LIQUIDITY_CHANGE_ABI, &call.input[4..]) {
        let base_token = liquidity_change_calldata[1]
            .to_owned()
            .into_address()
            .ok_or_else(|| {
                anyhow!(
                    "Failed to convert base token to address for WarmPath userCmd call: {:?}",
                    &liquidity_change_calldata[1]
                )
            })?
            .to_fixed_bytes()
            .to_vec();
        let quote_token = liquidity_change_calldata[2]
            .to_owned()
            .into_address()
            .ok_or_else(|| {
                anyhow!(
                    "Failed to convert quote token to address for WarmPath userCmd call: {:?}",
                    &liquidity_change_calldata[2]
                )
            })?
            .to_fixed_bytes()
            .to_vec();
        let pool_index = liquidity_change_calldata[3]
            .to_owned()
            .into_uint()
            .ok_or_else(|| {
                anyhow!("Failed to convert pool index to u32 for WarmPath userCmd call".to_string())
            })?
            .as_u32();

        let (base_flow, quote_flow) = decode_flows_from_output(call)?;
        let pool_hash = encode_pool_hash(base_token, quote_token, pool_index);
        Ok((pool_hash, base_flow, quote_flow))
    } else {
        bail!("Failed to decode inputs for WarmPath userCmd call.".to_string());
    }
}

fn decode_flows_from_output(call: &Call) -> Result<(ethabi::Int, ethabi::Int), anyhow::Error> {
    if let Ok(external_outputs) = decode(BASE_QUOTE_FLOW_OUTPUT, &call.return_data) {
        let base_flow = external_outputs[0]
            .to_owned()
            .into_int() // Needs conversion into bytes for next step
            .ok_or_else(|| anyhow!("Failed to convert base flow to i128".to_string()))?;

        let quote_flow = external_outputs[1]
            .to_owned()
            .into_int() // Needs conversion into bytes for next step
            .ok_or_else(|| anyhow!("Failed to convert quote flow to i128".to_string()))?;
        Ok((base_flow, quote_flow))
    } else {
        bail!("Failed to decode swap call outputs.".to_string());
    }
}

fn decode_mint_range_call(
    call: &Call,
) -> Result<(Vec<u8>, ethabi::Int, ethabi::Int), anyhow::Error> {
    if let Ok(mint_range) = decode(MINT_RANGE_ABI, &call.input[4..]) {
        let pool_hash = mint_range[9]
            .to_owned()
            .into_fixed_bytes()
            .ok_or_else(|| anyhow!("Failed to convert pool hash to fixed bytes".to_string()))?;

        if let Ok(external_outputs) = decode(MINT_RANGE_RETURN_ABI, &call.return_data) {
            let base_flow = external_outputs[0]
                .to_owned()
                .into_int() // Needs conversion into bytes for next step
                .ok_or_else(|| anyhow!("Failed to convert base flow to i128".to_string()))?;

            let quote_flow = external_outputs[1]
                .to_owned()
                .into_int() // Needs conversion into bytes for next step
                .ok_or_else(|| anyhow!("Failed to convert quote floww to i128".to_string()))?;
            Ok((pool_hash, base_flow, quote_flow))
        } else {
            bail!("Failed to decode swap call outputs.".to_string());
        }
    } else {
        bail!("Failed to decode inputs for WarmPath userCmd call.".to_string());
    }
}

fn decode_burn_ambient_call(
    call: &Call,
) -> Result<(Vec<u8>, ethabi::Int, ethabi::Int), anyhow::Error> {
    if let Ok(burn_ambient) = decode(BURN_AMBIENT_ABI, &call.input[4..]) {
        let pool_hash = burn_ambient[6]
            .to_owned()
            .into_fixed_bytes()
            .ok_or_else(|| anyhow!("Failed to convert pool hash to bytes".to_string()))?;

        if let Ok(external_outputs) = decode(BURN_AMBIENT_RETURN_ABI, &call.return_data) {
            let base_flow = external_outputs[0]
                .to_owned()
                .into_int()
                .ok_or_else(|| anyhow!("Failed to convert base flow to i128".to_string()))?;

            let quote_flow = external_outputs[1]
                .to_owned()
                .into_int()
                .ok_or_else(|| anyhow!("Failed to convert quote flow to i128".to_string()))?;

            Ok((pool_hash, base_flow, quote_flow))
        } else {
            bail!("Failed to decode burnAmbient call outputs.".to_string());
        }
    } else {
        bail!("Failed to decode inputs for burnAmbient call.".to_string());
    }
}
fn decode_mint_ambient_call(
    call: &Call,
) -> Result<(Vec<u8>, ethabi::Int, ethabi::Int), anyhow::Error> {
    if let Ok(mint_ambient) = decode(MINT_AMBIENT_ABI, &call.input[4..]) {
        let pool_hash = mint_ambient[6]
            .to_owned()
            .into_fixed_bytes()
            .ok_or_else(|| anyhow!("Failed to convert pool hash to bytes".to_string()))?;

        if let Ok(external_outputs) = decode(MINT_AMBIENT_RETURN_ABI, &call.return_data) {
            let base_flow = external_outputs[0]
                .to_owned()
                .into_int()
                .ok_or_else(|| anyhow!("Failed to convert base flow to i128".to_string()))?;

            let quote_flow = external_outputs[1]
                .to_owned()
                .into_int()
                .ok_or_else(|| anyhow!("Failed to convert quote flow to i128".to_string()))?;

            Ok((pool_hash, base_flow, quote_flow))
        } else {
            bail!("Failed to decode mintAmbient call outputs.".to_string());
        }
    } else {
        bail!("Failed to decode inputs for mintAmbient call.".to_string());
    }
}

fn decode_burn_range_call(
    call: &Call,
) -> Result<(Vec<u8>, ethabi::Int, ethabi::Int), anyhow::Error> {
    if let Ok(burn_range) = decode(BURN_RANGE_ABI, &call.input[4..]) {
        let pool_hash = burn_range[9]
            .to_owned()
            .into_fixed_bytes() // Convert Bytes32 to Vec<u8>
            .ok_or_else(|| anyhow!("Failed to convert pool hash to bytes".to_string()))?;

        if let Ok(external_outputs) = decode(BURN_RANGE_RETURN_ABI, &call.return_data) {
            let base_flow = external_outputs[0]
                .to_owned()
                .into_int()
                .ok_or_else(|| anyhow!("Failed to convert base flow to i128".to_string()))?;

            let quote_flow = external_outputs[1]
                .to_owned()
                .into_int()
                .ok_or_else(|| anyhow!("Failed to convert quote flow to i128".to_string()))?;

            Ok((pool_hash, base_flow, quote_flow))
        } else {
            bail!("Failed to decode burnRange call outputs.".to_string());
        }
    } else {
        bail!("Failed to decode inputs for burnRange call.".to_string());
    }
}
fn decode_knockout_call(
    call: &Call,
) -> Result<([u8; 32], ethabi::Int, ethabi::Int), anyhow::Error> {
    if let Ok(external_cmd) = decode(KNOCKOUT_EXTERNAL_ABI, &call.input[4..]) {
        let input_data = external_cmd[0]
            .to_owned()
            .into_bytes() // Convert Bytes32 to Vec<u8>
            .ok_or_else(|| anyhow!("Failed to Knockout userCmd input data.".to_string()))?;

        let code = input_data[31];
        let is_mint = code == 91;
        let is_burn = code == 92;

        let abi = if is_mint || is_burn {
            KNOCKOUT_INTERNAL_MINT_BURN_ABI
        } else {
            KNOCKOUT_INTERNAL_OTHER_CMD_ABI
        };

        if let Ok(mint_burn_inputs) = decode(abi, &input_data) {
            let base_token = mint_burn_inputs[1]
                .to_owned()
                .into_address()
                .ok_or_else(|| {
                    anyhow!("Failed to convert base token to address: {:?}", &mint_burn_inputs[1])
                })?
                .to_fixed_bytes()
                .to_vec();
            let quote_token = mint_burn_inputs[2]
                .to_owned()
                .into_address()
                .ok_or_else(|| {
                    anyhow!("Failed to convert quote token to address: {:?}", &mint_burn_inputs[2])
                })?
                .to_fixed_bytes()
                .to_vec();
            let pool_index = mint_burn_inputs[3]
                .to_owned()
                .into_uint()
                .ok_or_else(|| anyhow!("Failed to convert pool index to u32".to_string()))?
                .as_u32();

            let (base_flow, quote_flow) = decode_flows_from_output(call)?;
            let pool_hash = encode_pool_hash(base_token, quote_token, pool_index);
            Ok((pool_hash, base_flow, quote_flow))
        } else {
            bail!("Failed to decode burnRange call outputs.".to_string());
        }
    } else {
        bail!("Failed to decode inputs for burnRange call.".to_string());
    }
}
