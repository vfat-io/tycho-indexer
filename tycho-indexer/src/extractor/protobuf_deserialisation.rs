#![allow(deprecated)]
use std::collections::{hash_map::Entry, HashMap, HashSet};

use chrono::NaiveDateTime;
use tracing::warn;
use tycho_core::{
    models::{
        blockchain::{Block, Transaction, TxWithChanges},
        contract::{AccountBalance, AccountChangesWithTx, AccountDelta},
        protocol::{
            ComponentBalance, ProtocolChangesWithTx, ProtocolComponent, ProtocolComponentStateDelta,
        },
        Address, Chain, ChangeType, ComponentId, ProtocolType, TxHash,
    },
    Bytes,
};
use tycho_substreams::pb::tycho::evm::v1 as substreams;

use crate::extractor::{
    models::{BlockChanges, BlockContractChanges, BlockEntityChanges},
    u256_num::bytes_to_f64,
    ExtractionError,
};

pub trait TryFromMessage {
    type Args<'a>;

    fn try_from_message(args: Self::Args<'_>) -> Result<Self, ExtractionError>
    where
        Self: Sized;
}

impl TryFromMessage for AccountDelta {
    type Args<'a> = (substreams::ContractChange, Chain);

    fn try_from_message(args: Self::Args<'_>) -> Result<Self, ExtractionError> {
        let (msg, chain) = args;
        let change = ChangeType::try_from_message(msg.change())?;
        let update = AccountDelta::new(
            chain,
            msg.address.into(),
            msg.slots
                .into_iter()
                .map(|cs| (cs.slot.into(), Some(cs.value.into())))
                .collect(),
            if !msg.balance.is_empty() { Some(msg.balance.into()) } else { None },
            if !msg.code.is_empty() { Some(msg.code.into()) } else { None },
            change,
        );
        Ok(update)
    }
}

impl TryFromMessage for AccountBalance {
    type Args<'a> = (substreams::AccountBalanceChange, &'a Address, &'a Transaction);

    fn try_from_message(args: Self::Args<'_>) -> Result<Self, ExtractionError> {
        let (msg, addr, tx) = args;
        Ok(Self {
            token: msg.token.into(),
            balance: Bytes::from(msg.balance),
            modify_tx: tx.hash.clone(),
            account: addr.clone(),
        })
    }
}

impl TryFromMessage for Block {
    type Args<'a> = (substreams::Block, Chain);

    /// Parses block from tychos protobuf block message
    fn try_from_message(args: Self::Args<'_>) -> Result<Self, ExtractionError> {
        let (msg, chain) = args;

        Ok(Self {
            chain,
            number: msg.number,
            hash: msg.hash.into(),
            parent_hash: msg.parent_hash.into(),
            ts: NaiveDateTime::from_timestamp_opt(msg.ts as i64, 0).ok_or_else(|| {
                ExtractionError::DecodeError(format!(
                    "Failed to convert timestamp {} to datetime!",
                    msg.ts
                ))
            })?,
        })
    }
}

impl TryFromMessage for Transaction {
    type Args<'a> = (substreams::Transaction, &'a TxHash);

    fn try_from_message(args: Self::Args<'_>) -> Result<Self, ExtractionError> {
        let (msg, block_hash) = args;

        let to = if !msg.to.is_empty() { Some(msg.to.into()) } else { None };

        Ok(Self {
            hash: msg.hash.into(),
            block_hash: block_hash.clone(),
            from: msg.from.into(),
            to,
            index: msg.index,
        })
    }
}

impl TryFromMessage for ComponentBalance {
    type Args<'a> = (substreams::BalanceChange, &'a Transaction);

    fn try_from_message(args: Self::Args<'_>) -> Result<Self, ExtractionError> {
        let (msg, tx) = args;
        let balance_float = bytes_to_f64(&msg.balance).unwrap_or(f64::NAN);
        Ok(Self {
            token: msg.token.into(),
            balance: Bytes::from(msg.balance),
            balance_float,
            modify_tx: tx.hash.clone(),
            component_id: String::from_utf8(msg.component_id)
                .map_err(|error| ExtractionError::DecodeError(error.to_string()))?,
        })
    }
}

impl TryFromMessage for ProtocolComponent {
    type Args<'a> = (
        substreams::ProtocolComponent,
        Chain,
        &'a str,
        &'a HashMap<String, ProtocolType>,
        TxHash,
        NaiveDateTime,
    );

    fn try_from_message(args: Self::Args<'_>) -> Result<Self, ExtractionError> {
        let (msg, chain, protocol_system, protocol_types, tx_hash, creation_ts) = args;
        let tokens: Vec<Bytes> = msg
            .tokens
            .clone()
            .into_iter()
            .map(Into::into)
            .collect();

        let contract_ids = msg
            .contracts
            .clone()
            .into_iter()
            .map(Into::into)
            .collect();

        let static_attributes = msg
            .static_att
            .clone()
            .into_iter()
            .map(|attribute| (attribute.name, Bytes::from(attribute.value)))
            .collect();

        let protocol_type = msg
            .protocol_type
            .clone()
            .ok_or(ExtractionError::DecodeError("Missing protocol type".to_owned()))?;

        if !protocol_types.contains_key(&protocol_type.name) {
            return Err(ExtractionError::DecodeError(format!(
                "Unknown protocol type name: {}",
                protocol_type.name
            )));
        }

        Ok(Self {
            id: msg.id.clone(),
            protocol_type_name: protocol_type.name,
            protocol_system: protocol_system.to_owned(),
            tokens,
            contract_addresses: contract_ids,
            static_attributes,
            chain,
            change: ChangeType::try_from_message(msg.change())?,
            creation_tx: tx_hash,
            created_at: creation_ts,
        })
    }
}

impl TryFromMessage for ChangeType {
    type Args<'a> = substreams::ChangeType;

    fn try_from_message(args: Self::Args<'_>) -> Result<Self, ExtractionError> {
        match args {
            substreams::ChangeType::Creation => Ok(ChangeType::Creation),
            substreams::ChangeType::Update => Ok(ChangeType::Update),
            substreams::ChangeType::Deletion => Ok(ChangeType::Deletion),
            substreams::ChangeType::Unspecified => Err(ExtractionError::DecodeError(format!(
                "Unknown ChangeType enum member encountered: {:?}",
                args
            ))),
        }
    }
}

impl TryFromMessage for ProtocolComponentStateDelta {
    type Args<'a> = substreams::EntityChanges;

    fn try_from_message(args: Self::Args<'_>) -> Result<Self, ExtractionError> {
        let msg = args;

        let (mut updates, mut deletions) = (HashMap::new(), HashSet::new());

        for attribute in msg.attributes.into_iter() {
            match ChangeType::try_from_message(attribute.change())? {
                ChangeType::Update | ChangeType::Creation => {
                    updates.insert(attribute.name, Bytes::from(attribute.value));
                }
                ChangeType::Deletion => {
                    deletions.insert(attribute.name);
                }
            }
        }

        Ok(Self {
            component_id: msg.component_id,
            updated_attributes: updates,
            deleted_attributes: deletions,
        })
    }
}

impl TryFromMessage for ProtocolChangesWithTx {
    type Args<'a> = (
        substreams::TransactionEntityChanges,
        &'a Block,
        &'a str,
        &'a HashMap<String, ProtocolType>,
    );

    fn try_from_message(args: Self::Args<'_>) -> Result<Self, ExtractionError> {
        let (msg, block, protocol_system, protocol_types) = args;
        let tx = Transaction::try_from_message((
            msg.tx
                .expect("TransactionEntityChanges should have a transaction"),
            &block.hash.clone(),
        ))?;

        let mut new_protocol_components: HashMap<String, ProtocolComponent> = HashMap::new();
        let mut state_updates: HashMap<String, ProtocolComponentStateDelta> = HashMap::new();
        let mut component_balances: HashMap<String, HashMap<Bytes, ComponentBalance>> =
            HashMap::new();

        // First, parse the new protocol components
        for change in msg.component_changes.into_iter() {
            let component = ProtocolComponent::try_from_message((
                change.clone(),
                block.chain,
                protocol_system,
                protocol_types,
                tx.hash.clone(),
                block.ts,
            ))?;
            new_protocol_components.insert(change.id, component);
        }

        // Then, parse the state updates
        for state_msg in msg.entity_changes.into_iter() {
            let state = ProtocolComponentStateDelta::try_from_message(state_msg)?;
            // Check if a state update for the same component already exists
            // If it exists, overwrite the existing state update with the new one and log a warning
            match state_updates.entry(state.component_id.clone()) {
                Entry::Vacant(e) => {
                    e.insert(state);
                }
                Entry::Occupied(mut e) => {
                    warn!("Received two state updates for the same component. Overwriting state for component {}", e.key());
                    e.insert(state);
                }
            }
        }

        // Finally, parse the balance changes
        for balance_change in msg.balance_changes.into_iter() {
            let component_balance = ComponentBalance::try_from_message((balance_change, &tx))?;

            // Check if a balance change for the same token and component already exists
            // If it exists, overwrite the existing balance change with the new one and log a
            // warning
            let token_balances = component_balances
                .entry(component_balance.component_id.clone())
                .or_default();

            if let Some(existing_balance) =
                token_balances.insert(component_balance.token.clone(), component_balance)
            {
                warn!(
                    "Received two balance updates for the same component id: {} and token {}. Overwriting balance change",
                    existing_balance.component_id, existing_balance.token
                );
            }
        }

        Ok(Self {
            new_protocol_components,
            protocol_states: state_updates,
            balance_changes: component_balances,
            tx,
        })
    }
}

impl TryFromMessage for TxWithChanges {
    type Args<'a> =
        (substreams::TransactionChanges, &'a Block, &'a str, &'a HashMap<String, ProtocolType>);

    fn try_from_message(args: Self::Args<'_>) -> Result<Self, ExtractionError> {
        let (msg, block, protocol_system, protocol_types) = args;
        let tx = Transaction::try_from_message((
            msg.tx
                .expect("TransactionChanges should have a transaction"),
            &block.hash.clone(),
        ))?;

        let mut new_protocol_components: HashMap<ComponentId, ProtocolComponent> = HashMap::new();
        let mut account_updates: HashMap<Address, AccountDelta> = HashMap::new();
        let mut state_updates: HashMap<ComponentId, ProtocolComponentStateDelta> = HashMap::new();
        let mut balance_changes: HashMap<ComponentId, HashMap<Address, ComponentBalance>> =
            HashMap::new();
        let mut account_balance_changes: HashMap<Address, HashMap<Address, AccountBalance>> =
            HashMap::new();

        // First, parse the new protocol components
        for change in msg.component_changes.into_iter() {
            let component = ProtocolComponent::try_from_message((
                change,
                block.chain,
                protocol_system,
                protocol_types,
                tx.hash.clone(),
                block.ts,
            ))?;
            new_protocol_components.insert(component.id.clone(), component);
        }

        // Then, parse the account updates
        for contract_change in msg.contract_changes.clone().into_iter() {
            let update = AccountDelta::try_from_message((contract_change, block.chain))?;
            account_updates.insert(update.address.clone(), update);
        }

        // Then, parse the state updates
        for state_msg in msg.entity_changes.into_iter() {
            let state = ProtocolComponentStateDelta::try_from_message(state_msg)?;
            // Check if a state update for the same component already exists
            // If it exists, overwrite the existing state update with the new one and log a warning
            match state_updates.entry(state.component_id.clone()) {
                Entry::Vacant(e) => {
                    e.insert(state);
                }
                Entry::Occupied(mut e) => {
                    warn!("Received two state updates for the same component. Overwriting state for component {}", e.key());
                    e.insert(state);
                }
            }
        }

        // Finally, parse the component balance changes
        for balance_change in msg.balance_changes.into_iter() {
            let component_id = String::from_utf8(balance_change.component_id.clone())
                .map_err(|error| ExtractionError::DecodeError(error.to_string()))?;
            let token_address = Bytes::from(balance_change.token.clone());
            let balance = ComponentBalance::try_from_message((balance_change, &tx))?;

            balance_changes
                .entry(component_id)
                .or_default()
                .insert(token_address, balance);
        }

        // parse the account balance changes
        for contract_change in msg.contract_changes.into_iter() {
            for balance_change in contract_change
                .token_balances
                .into_iter()
            {
                let account_addr = contract_change.address.clone().into();
                let token_address = Bytes::from(balance_change.token.clone());
                let balance =
                    AccountBalance::try_from_message((balance_change, &account_addr, &tx))?;

                account_balance_changes
                    .entry(account_addr)
                    .or_default()
                    .insert(token_address, balance);
            }
        }

        Ok(Self {
            protocol_components: new_protocol_components,
            account_deltas: account_updates,
            state_updates,
            balance_changes,
            account_balance_changes,
            tx,
        })
    }
}

impl TryFromMessage for BlockContractChanges {
    type Args<'a> = (
        substreams::BlockContractChanges,
        &'a str,
        Chain,
        String,
        &'a HashMap<String, ProtocolType>,
        u64,
    );

    fn try_from_message(args: Self::Args<'_>) -> Result<Self, ExtractionError> {
        let (msg, extractor, chain, protocol_system, protocol_types, finalized_block_height) = args;

        if let Some(block) = msg.block {
            let block = Block::try_from_message((block, chain))?;
            let mut tx_updates = Vec::new();

            for change in msg.changes.into_iter() {
                let mut account_updates = HashMap::new();
                let mut protocol_components = HashMap::new();
                let mut balances_changes: HashMap<ComponentId, HashMap<Address, ComponentBalance>> =
                    HashMap::new();
                let mut account_balance_changes: HashMap<
                    Address,
                    HashMap<Address, AccountBalance>,
                > = HashMap::new();

                if let Some(tx) = change.tx {
                    let tx = Transaction::try_from_message((tx, &block.hash.clone()))?;
                    for contract_change in change
                        .contract_changes
                        .clone()
                        .into_iter()
                    {
                        let update = AccountDelta::try_from_message((contract_change, chain))?;
                        account_updates.insert(update.address.clone(), update);
                    }
                    for component_msg in change.component_changes.into_iter() {
                        let component = ProtocolComponent::try_from_message((
                            component_msg,
                            chain,
                            &protocol_system,
                            protocol_types,
                            tx.hash.clone(),
                            block.ts,
                        ))?;
                        protocol_components.insert(component.id.clone(), component);
                    }

                    // parse the balance changes
                    for balance_change in change.balance_changes.into_iter() {
                        let component_id =
                            String::from_utf8(balance_change.component_id.clone())
                                .map_err(|error| ExtractionError::DecodeError(error.to_string()))?;
                        let token_address = balance_change.token.clone().into();
                        let balance = ComponentBalance::try_from_message((balance_change, &tx))?;

                        balances_changes
                            .entry(component_id)
                            .or_default()
                            .insert(token_address, balance);
                    }

                    // parse the account balance changes
                    for contract_change in change.contract_changes.into_iter() {
                        for balance_change in contract_change
                            .token_balances
                            .into_iter()
                        {
                            let account_addr = contract_change.address.clone().into();
                            let token_address = Bytes::from(balance_change.token.clone());
                            let balance = AccountBalance::try_from_message((
                                balance_change,
                                &account_addr,
                                &tx,
                            ))?;

                            account_balance_changes
                                .entry(account_addr)
                                .or_default()
                                .insert(token_address, balance);
                        }
                    }

                    tx_updates.push(AccountChangesWithTx::new(
                        account_updates,
                        protocol_components,
                        balances_changes,
                        account_balance_changes,
                        tx,
                    ));
                }
            }
            tx_updates.sort_unstable_by_key(|update| update.tx.index);
            return Ok(Self::new(
                extractor.to_owned(),
                chain,
                block,
                finalized_block_height,
                false,
                tx_updates,
            ));
        }
        Err(ExtractionError::Empty)
    }
}

impl TryFromMessage for BlockEntityChanges {
    type Args<'a> = (
        substreams::BlockEntityChanges,
        &'a str,
        Chain,
        &'a str,
        &'a HashMap<String, ProtocolType>,
        u64,
    );

    fn try_from_message(args: Self::Args<'_>) -> Result<Self, ExtractionError> {
        let (msg, extractor, chain, protocol_system, protocol_types, finalized_block_height) = args;

        if let Some(block) = msg.block {
            let block = Block::try_from_message((block, chain))?;

            let mut txs_with_update = msg
                .changes
                .into_iter()
                .map(|change| {
                    change.tx.as_ref().ok_or_else(|| {
                        ExtractionError::DecodeError(
                            "TransactionEntityChanges misses a transaction".to_owned(),
                        )
                    })?;

                    ProtocolChangesWithTx::try_from_message((
                        change,
                        &block,
                        protocol_system,
                        protocol_types,
                    ))
                })
                .collect::<Result<Vec<ProtocolChangesWithTx>, ExtractionError>>()?;

            // Sort updates by transaction index
            txs_with_update.sort_unstable_by_key(|update| update.tx.index);

            Ok(Self::new(
                extractor.to_string(),
                chain,
                block,
                finalized_block_height,
                false,
                txs_with_update,
            ))
        } else {
            Err(ExtractionError::Empty)
        }
    }
}

impl TryFromMessage for BlockChanges {
    type Args<'a> =
        (substreams::BlockChanges, &'a str, Chain, &'a str, &'a HashMap<String, ProtocolType>, u64);

    fn try_from_message(args: Self::Args<'_>) -> Result<Self, ExtractionError> {
        let (msg, extractor, chain, protocol_system, protocol_types, finalized_block_height) = args;

        if let Some(block) = msg.block {
            let block = Block::try_from_message((block, chain))?;

            let txs_with_update = msg
                .changes
                .into_iter()
                .map(|change| {
                    change.tx.as_ref().ok_or_else(|| {
                        ExtractionError::DecodeError(
                            "TransactionEntityChanges misses a transaction".to_owned(),
                        )
                    })?;

                    TxWithChanges::try_from_message((
                        change,
                        &block,
                        protocol_system,
                        protocol_types,
                    ))
                })
                .collect::<Result<Vec<TxWithChanges>, ExtractionError>>()?;

            // Sort updates by transaction index
            let mut txs_with_update = txs_with_update;
            txs_with_update.sort_unstable_by_key(|update| update.tx.index);

            Ok(Self::new(
                extractor.to_string(),
                chain,
                block,
                finalized_block_height,
                false,
                txs_with_update,
            ))
        } else {
            Err(ExtractionError::Empty)
        }
    }
}

#[cfg(test)]
pub mod fixtures {
    use std::{collections::HashSet, str::FromStr};

    use prost::Message;
    use tycho_core::{models::protocol::ProtocolComponentStateDelta, Bytes};
    use tycho_storage::postgres::db_fixtures::yesterday_midnight;
    use tycho_substreams::pb::tycho::evm::v1::*;

    use crate::extractor::models::fixtures::HASH_256_0;

    pub fn pb_state_changes() -> EntityChanges {
        let res1_value = Bytes::from(1_000u64)
            .lpad(32, 0)
            .to_vec();
        let res2_value = Bytes::from(500u64).lpad(32, 0).to_vec();
        EntityChanges {
            component_id: "State1".to_owned(),
            attributes: vec![
                Attribute {
                    name: "reserve1".to_owned(),
                    value: res1_value,
                    change: ChangeType::Update.into(),
                },
                Attribute {
                    name: "reserve2".to_owned(),
                    value: res2_value,
                    change: ChangeType::Update.into(),
                },
            ],
        }
    }

    pub fn protocol_state_delta() -> ProtocolComponentStateDelta {
        let res1_value = Bytes::from(1_000u64)
            .lpad(32, 0)
            .to_vec();
        let res2_value = Bytes::from(500u64).lpad(32, 0).to_vec();
        ProtocolComponentStateDelta {
            component_id: "State1".to_string(),
            updated_attributes: vec![
                ("reserve1".to_owned(), Bytes::from(res1_value)),
                ("reserve2".to_owned(), Bytes::from(res2_value)),
            ]
            .into_iter()
            .collect(),
            deleted_attributes: HashSet::new(),
        }
    }

    pub fn pb_protocol_component() -> ProtocolComponent {
        ProtocolComponent {
            id: "d417ff54652c09bd9f31f216b1a2e5d1e28c1dce1ba840c40d16f2b4d09b5902".to_owned(),
            tokens: vec![address_from_str(DAI_ADDRESS), address_from_str(DAI_ADDRESS)],
            contracts: vec![
                Bytes::from_str("0x31fF2589Ee5275a2038beB855F44b9Be993aA804")
                    .unwrap()
                    .0
                    .to_vec(),
                address_from_str(WETH_ADDRESS),
            ],
            static_att: vec![
                Attribute {
                    name: "balance".to_owned(),
                    value: Bytes::from(100u64).lpad(32, 0).to_vec(),
                    change: ChangeType::Creation.into(),
                },
                Attribute {
                    name: "factory_address".to_owned(),
                    value: b"0x0fwe0g240g20".to_vec(),
                    change: ChangeType::Creation.into(),
                },
            ],
            change: ChangeType::Creation.into(),
            protocol_type: Some(ProtocolType {
                name: "WeightedPool".to_string(),
                financial_type: 0,
                attribute_schema: vec![],
                implementation_type: 0,
            }),
        }
    }

    pub fn pb_blocks(version: u64) -> Block {
        if version == 0 {
            panic!("Block version 0 doesn't exist. It starts at 1");
        }
        let base_ts = yesterday_midnight().timestamp() as u64;

        Block {
            number: version,
            hash: Bytes::from(version)
                .lpad(32, 0)
                .to_vec(),
            parent_hash: Bytes::from(version - 1)
                .lpad(32, 0)
                .to_vec(),
            ts: base_ts + version * 1000,
        }
    }

    pub fn pb_transactions(version: u64, index: u64) -> Transaction {
        Transaction {
            hash: Bytes::from(version * 10_000 + index)
                .lpad(32, 0)
                .to_vec(),
            from: Bytes::from(version * 100_000 + index)
                .lpad(20, 0)
                .to_vec(),
            to: Bytes::from(version * 1_000_000 + index)
                .lpad(20, 0)
                .to_vec(),
            index,
        }
    }

    const WETH_ADDRESS: &str = "C02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2";
    const USDC_ADDRESS: &str = "A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48";
    const DAI_ADDRESS: &str = "6B175474E89094C44Da98b954EedeAC495271d0F";
    const USDT_ADDRESS: &str = "dAC17F958D2ee523a2206206994597C13D831ec7";

    pub fn address_from_str(token: &str) -> Vec<u8> {
        Bytes::from_str(token)
            .unwrap()
            .0
            .to_vec()
    }

    pub fn pb_block_contract_changes(version: u8) -> BlockContractChanges {
        match version {
            0 => BlockContractChanges {
                block: Some(Block {
                    hash: Bytes::from(vec![0x31, 0x32, 0x33, 0x34])
                        .lpad(32, 0)
                        .to_vec(),
                    parent_hash: Bytes::from(vec![0x21, 0x22, 0x23, 0x24])
                        .lpad(32, 0)
                        .to_vec(),
                    number: 1,
                    ts: 1000,
                }),

                changes: vec![
                    TransactionContractChanges {
                        tx: Some(Transaction {
                            hash: Bytes::from(vec![0x11, 0x12, 0x13, 0x14])
                                .lpad(32, 0)
                                .to_vec(),
                            from: Bytes::from(vec![0x41, 0x42, 0x43, 0x44])
                                .lpad(20, 0)
                                .to_vec(),
                            to: Bytes::from(vec![0x51, 0x52, 0x53, 0x54])
                                .lpad(20, 0)
                                .to_vec(),
                            index: 2,
                        }),
                        contract_changes: vec![ContractChange {
                            address: Bytes::from(vec![0x61, 0x62, 0x63, 0x64])
                                .lpad(20, 0)
                                .to_vec(),
                            balance: Bytes::from(vec![0x71, 0x72, 0x73, 0x74])
                                .lpad(32, 0)
                                .to_vec(),
                            code: vec![0x81, 0x82, 0x83, 0x84],
                            slots: vec![
                                ContractSlot {
                                    slot: Bytes::from(vec![0xc1, 0xc2, 0xc3, 0xc4])
                                        .lpad(32, 0)
                                        .to_vec(),
                                    value: Bytes::from(vec![0xd1, 0xd2, 0xd3, 0xd4])
                                        .lpad(32, 0)
                                        .to_vec(),
                                },
                                ContractSlot {
                                    slot: Bytes::from(vec![0xa1, 0xa2, 0xa3, 0xa4])
                                        .lpad(32, 0)
                                        .to_vec(),
                                    value: Bytes::from(vec![0xb1, 0xb2, 0xb3, 0xb4])
                                        .lpad(32, 0)
                                        .to_vec(),
                                },
                            ],
                            change: ChangeType::Update.into(),
                            token_balances: vec![AccountBalanceChange {
                                token: address_from_str(WETH_ADDRESS),
                                balance: 50000000.encode_to_vec(),
                            }],
                        }],
                        component_changes: vec![ProtocolComponent {
                            id: "d417ff54652c09bd9f31f216b1a2e5d1e28c1dce1ba840c40d16f2b4d09b5902"
                                .to_owned(),
                            tokens: vec![
                                address_from_str(DAI_ADDRESS),
                                address_from_str(DAI_ADDRESS),
                            ],
                            contracts: vec![
                                address_from_str(WETH_ADDRESS),
                                address_from_str(WETH_ADDRESS),
                            ],
                            static_att: vec![
                                Attribute {
                                    name: "key1".to_owned(),
                                    value: b"value1".to_vec(),
                                    change: ChangeType::Creation.into(),
                                },
                                Attribute {
                                    name: "key2".to_owned(),
                                    value: b"value2".to_vec(),
                                    change: ChangeType::Creation.into(),
                                },
                            ],
                            change: ChangeType::Creation.into(),
                            protocol_type: Some(ProtocolType {
                                name: "WeightedPool".to_string(),
                                financial_type: 0,
                                attribute_schema: vec![],
                                implementation_type: 0,
                            }),
                        }],
                        balance_changes: vec![BalanceChange {
                            token: address_from_str(WETH_ADDRESS),
                            balance: 50000000.encode_to_vec(),
                            component_id:
                                "d417ff54652c09bd9f31f216b1a2e5d1e28c1dce1ba840c40d16f2b4d09b5902"
                                    .as_bytes()
                                    .to_vec(),
                        }],
                    },
                    TransactionContractChanges {
                        tx: Some(Transaction {
                            hash: Bytes::from(vec![0x01])
                                .lpad(32, 0)
                                .to_vec(),
                            from: Bytes::from(vec![0x41, 0x42, 0x43, 0x44])
                                .lpad(20, 0)
                                .to_vec(),
                            to: Bytes::from(vec![0x51, 0x52, 0x53, 0x54])
                                .lpad(20, 0)
                                .to_vec(),
                            index: 5,
                        }),
                        contract_changes: vec![ContractChange {
                            address: Bytes::from(vec![0x61, 0x62, 0x63, 0x64])
                                .lpad(20, 0)
                                .to_vec(),
                            balance: Bytes::from(vec![0xf1, 0xf2, 0xf3, 0xf4])
                                .lpad(32, 0)
                                .to_vec(),
                            code: vec![0x01, 0x02, 0x03, 0x04],
                            slots: vec![
                                ContractSlot {
                                    slot: Bytes::from(vec![0x91, 0x92, 0x93, 0x94])
                                        .lpad(32, 0)
                                        .to_vec(),
                                    value: Bytes::from(vec![0xa1, 0xa2, 0xa3, 0xa4])
                                        .lpad(32, 0)
                                        .to_vec(),
                                },
                                ContractSlot {
                                    slot: Bytes::from(vec![0xa1, 0xa2, 0xa3, 0xa4])
                                        .lpad(32, 0)
                                        .to_vec(),
                                    value: Bytes::from(vec![0xc1, 0xc2, 0xc3, 0xc4])
                                        .lpad(32, 0)
                                        .to_vec(),
                                },
                            ],
                            change: ChangeType::Update.into(),
                            token_balances: vec![AccountBalanceChange {
                                token: address_from_str(WETH_ADDRESS),
                                balance: 10.encode_to_vec(),
                            }],
                        }],
                        component_changes: vec![],
                        balance_changes: vec![BalanceChange {
                            token: address_from_str(WETH_ADDRESS),
                            balance: 10.encode_to_vec(),
                            component_id:
                                "d417ff54652c09bd9f31f216b1a2e5d1e28c1dce1ba840c40d16f2b4d09b5902"
                                    .to_string()
                                    .as_bytes()
                                    .to_vec(),
                        }],
                    },
                ],
            },
            1 => BlockContractChanges {
                block: Some(pb_blocks(version as u64)),
                changes: vec![TransactionContractChanges {
                    tx: Some(pb_transactions(1, 1)),
                    contract_changes: vec![ContractChange {
                        address: address_from_str("0000000000000000000000000000000000000001"),
                        balance: 1_i32.to_be_bytes().to_vec(),
                        code: 123_i32.to_be_bytes().to_vec(),
                        slots: vec![ContractSlot {
                            slot: Bytes::from("0x01").into(),
                            value: Bytes::from("0x01").into(),
                        }],
                        change: ChangeType::Creation.into(),
                        token_balances: vec![
                            AccountBalanceChange {
                                token: address_from_str(USDC_ADDRESS),
                                balance: 1_i32.to_be_bytes().to_vec(),
                            },
                            AccountBalanceChange {
                                token: address_from_str(WETH_ADDRESS),
                                balance: 1_i32.to_be_bytes().to_vec(),
                            },
                        ],
                    }],
                    component_changes: vec![ProtocolComponent {
                        id: "pc_1".to_owned(),
                        tokens: vec![
                            address_from_str(WETH_ADDRESS),
                            address_from_str(USDC_ADDRESS),
                        ],
                        contracts: vec![],
                        static_att: vec![],
                        change: ChangeType::Creation.into(),
                        protocol_type: Some(ProtocolType {
                            name: "pt_1".to_string(),
                            financial_type: 0,
                            attribute_schema: vec![],
                            implementation_type: 0,
                        }),
                    }],
                    balance_changes: vec![
                        BalanceChange {
                            token: address_from_str(USDC_ADDRESS),
                            balance: 1_i32.to_be_bytes().to_vec(),
                            component_id: "pc_1".into(),
                        },
                        BalanceChange {
                            token: address_from_str(WETH_ADDRESS),
                            balance: 1_i32.to_be_bytes().to_vec(),
                            component_id: "pc_1".into(),
                        },
                    ],
                }],
            },
            2 => BlockContractChanges {
                block: Some(pb_blocks(version as u64)),
                changes: vec![TransactionContractChanges {
                    tx: Some(pb_transactions(2, 1)),
                    contract_changes: vec![
                        ContractChange {
                            address: address_from_str("0000000000000000000000000000000000000002"),
                            balance: 2_i32.to_be_bytes().to_vec(),
                            code: 123_i32.to_be_bytes().to_vec(),
                            slots: vec![ContractSlot {
                                slot: Bytes::from("0x01").into(),
                                value: Bytes::from("0x02").into(),
                            }],
                            change: ChangeType::Creation.into(),
                            token_balances: vec![],
                        },
                        ContractChange {
                            address: address_from_str("0000000000000000000000000000000000000001"),
                            balance: 10_i32.to_be_bytes().to_vec(),
                            code: 123_i32.to_be_bytes().to_vec(),
                            slots: vec![
                                ContractSlot {
                                    slot: Bytes::from("0x01").into(),
                                    value: Bytes::from(10u8).lpad(32, 0).into(),
                                },
                                ContractSlot {
                                    slot: Bytes::from("0x02").into(),
                                    value: Bytes::from("0x0a").into(),
                                },
                            ],
                            change: ChangeType::Update.into(),
                            token_balances: vec![AccountBalanceChange {
                                token: address_from_str(WETH_ADDRESS),
                                balance: 10.encode_to_vec(),
                            }],
                        },
                    ],
                    component_changes: vec![ProtocolComponent {
                        id: "pc_2".to_owned(),
                        tokens: vec![
                            address_from_str(USDT_ADDRESS),
                            address_from_str(USDC_ADDRESS),
                        ],
                        contracts: vec![address_from_str(
                            "0000000000000000000000000000000000000002",
                        )],
                        static_att: vec![],
                        change: ChangeType::Creation.into(),
                        protocol_type: Some(ProtocolType {
                            name: "pt_1".to_string(),
                            financial_type: 0,
                            attribute_schema: vec![],
                            implementation_type: 0,
                        }),
                    }],
                    balance_changes: vec![
                        BalanceChange {
                            token: address_from_str(USDC_ADDRESS),
                            balance: 20_i32.to_be_bytes().to_vec(),
                            component_id: "pc_2".into(),
                        },
                        BalanceChange {
                            token: address_from_str(USDT_ADDRESS),
                            balance: 20_i32.to_be_bytes().to_vec(),
                            component_id: "pc_2".into(),
                        },
                        BalanceChange {
                            token: address_from_str(USDC_ADDRESS),
                            balance: 10_i32.to_be_bytes().to_vec(),
                            component_id: "pc_1".into(),
                        },
                    ],
                }],
            },
            3 => BlockContractChanges {
                block: Some(pb_blocks(version as u64)),
                changes: vec![
                    TransactionContractChanges {
                        tx: Some(pb_transactions(3, 1)),
                        contract_changes: vec![
                            ContractChange {
                                address: address_from_str(
                                    "0000000000000000000000000000000000000001",
                                ),
                                balance: 1_i32.to_be_bytes().to_vec(),
                                code: 123_i32.to_le_bytes().to_vec(),
                                slots: vec![ContractSlot {
                                    slot: Bytes::from("0x01").into(),
                                    value: Bytes::from("0x01").into(),
                                }],
                                change: ChangeType::Update.into(),
                                token_balances: vec![],
                            },
                            ContractChange {
                                address: address_from_str(
                                    "0000000000000000000000000000000000000002",
                                ),
                                balance: 20_i32.to_be_bytes().to_vec(),
                                code: 123_i32.to_le_bytes().to_vec(),
                                slots: vec![ContractSlot {
                                    slot: Bytes::from("0x02").into(),
                                    value: Bytes::from(200u8).lpad(32, 0).into(),
                                }],
                                change: ChangeType::Update.into(),
                                token_balances: vec![AccountBalanceChange {
                                    token: address_from_str(USDC_ADDRESS),
                                    balance: 1_i32.to_be_bytes().to_vec(),
                                }],
                            },
                        ],
                        component_changes: vec![],
                        balance_changes: vec![BalanceChange {
                            token: address_from_str(USDC_ADDRESS),
                            balance: 1_i32.to_be_bytes().to_vec(),
                            component_id: "pc_2".into(),
                        }],
                    },
                    TransactionContractChanges {
                        tx: Some(pb_transactions(3, 2)),
                        contract_changes: vec![ContractChange {
                            address: address_from_str("0000000000000000000000000000000000000001"),
                            balance: 1_i32.to_be_bytes().to_vec(),
                            code: 123_i32.to_le_bytes().to_vec(),
                            slots: vec![ContractSlot {
                                slot: Bytes::from("0x01").into(),
                                value: Bytes::from("0x01").into(),
                            }],
                            change: ChangeType::Update.into(),
                            token_balances: vec![AccountBalanceChange {
                                token: address_from_str(USDC_ADDRESS),
                                balance: 2_i32.to_be_bytes().to_vec(),
                            }],
                        }],
                        component_changes: vec![],
                        balance_changes: vec![
                            BalanceChange {
                                token: address_from_str(USDC_ADDRESS),
                                balance: 100_i32.to_be_bytes().to_vec(),
                                component_id: "pc_1".into(),
                            },
                            BalanceChange {
                                token: address_from_str(USDC_ADDRESS),
                                balance: 2_i32.to_be_bytes().to_vec(),
                                component_id: "pc_2".into(),
                            },
                        ],
                    },
                ],
            },
            4 => BlockContractChanges {
                block: Some(pb_blocks(version as u64)),
                changes: vec![TransactionContractChanges {
                    tx: Some(pb_transactions(4, 1)),
                    contract_changes: vec![ContractChange {
                        address: address_from_str("0000000000000000000000000000000000000001"),
                        balance: 1_i32.to_be_bytes().to_vec(),
                        code: 123_i32.to_le_bytes().to_vec(),
                        slots: vec![ContractSlot {
                            slot: Bytes::from(3u8).lpad(32, 0).into(),
                            value: Bytes::from(10u8).lpad(32, 0).into(),
                        }],
                        change: ChangeType::Update.into(),
                        token_balances: vec![],
                    }],
                    component_changes: vec![ProtocolComponent {
                        id: "pc_3".to_owned(),
                        tokens: vec![address_from_str(DAI_ADDRESS), address_from_str(USDC_ADDRESS)],
                        contracts: vec![address_from_str(
                            "0000000000000000000000000000000000000001",
                        )],
                        static_att: vec![],
                        change: ChangeType::Creation.into(),
                        protocol_type: Some(ProtocolType {
                            name: "pt_1".to_string(),
                            financial_type: 0,
                            attribute_schema: vec![],
                            implementation_type: 0,
                        }),
                    }],
                    balance_changes: vec![],
                }],
            },
            5 => BlockContractChanges {
                block: Some(pb_blocks(version as u64)),
                changes: vec![TransactionContractChanges {
                    tx: Some(pb_transactions(5, 1)),
                    contract_changes: vec![
                        ContractChange {
                            address: address_from_str("0000000000000000000000000000000000000001"),
                            balance: 1_i32.to_be_bytes().to_vec(),
                            code: 123_i32.to_le_bytes().to_vec(),
                            slots: vec![ContractSlot {
                                slot: Bytes::from("0x01").into(),
                                value: Bytes::from(10u8).lpad(32, 0).into(),
                            }],
                            change: ChangeType::Update.into(),
                            token_balances: vec![AccountBalanceChange {
                                token: address_from_str(USDC_ADDRESS),
                                balance: 1_i32.to_be_bytes().to_vec(),
                            }],
                        },
                        ContractChange {
                            address: address_from_str("0000000000000000000000000000000000000002"),
                            balance: 1_i32.to_be_bytes().to_vec(),
                            code: 123_i32.to_le_bytes().to_vec(),
                            slots: vec![ContractSlot {
                                slot: Bytes::from("0x01").into(),
                                value: Bytes::from(10u8).lpad(32, 0).into(),
                            }],
                            change: ChangeType::Update.into(),
                            token_balances: vec![AccountBalanceChange {
                                token: address_from_str(USDC_ADDRESS),
                                balance: 1_i32.to_be_bytes().to_vec(),
                            }],
                        },
                    ],
                    component_changes: vec![],
                    balance_changes: vec![
                        BalanceChange {
                            token: address_from_str(USDC_ADDRESS),
                            balance: 1_i32.to_be_bytes().to_vec(),
                            component_id: "pc_1".into(),
                        },
                        BalanceChange {
                            token: address_from_str(USDC_ADDRESS),
                            balance: 1_i32.to_be_bytes().to_vec(),
                            component_id: "pc_3".into(),
                        },
                        BalanceChange {
                            token: address_from_str(WETH_ADDRESS),
                            balance: 1_i32.to_be_bytes().to_vec(),
                            component_id: "pc_1".into(),
                        },
                    ],
                }],
            },
            _ => panic!("Requested BlockContractChanges version doesn't exist"),
        }
    }

    pub fn pb_block_entity_changes(version: u8) -> BlockEntityChanges {
        match version {
            0 => BlockEntityChanges {
                block: Some(Block {
                    hash: Bytes::zero(32).to_vec(),
                    parent_hash: Bytes::from(vec![0x21, 0x22, 0x23, 0x24])
                        .lpad(32, 0)
                        .to_vec(),
                    number: 1,
                    ts: yesterday_midnight().timestamp() as u64,
                }),
                changes: vec![
                    TransactionEntityChanges {
                        tx: Some(Transaction {
                            hash: Bytes::zero(32).to_vec(),
                            from: Bytes::zero(20).to_vec(),
                            to: Bytes::zero(20).to_vec(),
                            index: 10,
                        }),
                        entity_changes: vec![
                            EntityChanges {
                                component_id: "State1".to_owned(),
                                attributes: vec![
                                    Attribute {
                                        name: "reserve1".to_owned(),
                                        value: Bytes::from(1000u64)
                                            .lpad(32, 0)
                                            .to_vec(),
                                        change: ChangeType::Update.into(),
                                    },
                                    Attribute {
                                        name: "reserve2".to_owned(),
                                        value: Bytes::from(500u64).lpad(32, 0).to_vec(),
                                        change: ChangeType::Update.into(),
                                    },
                                    Attribute {
                                        name: "static_attribute".to_owned(),
                                        value: Bytes::from(1u64).lpad(32, 0).to_vec(),
                                        change: ChangeType::Update.into(),
                                    },
                                ],
                            },
                            EntityChanges {
                                component_id: "State2".to_owned(),
                                attributes: vec![
                                    Attribute {
                                        name: "reserve1".to_owned(),
                                        value: Bytes::from(1000u64)
                                            .lpad(32, 0)
                                            .to_vec(),
                                        change: ChangeType::Update.into(),
                                    },
                                    Attribute {
                                        name: "reserve2".to_owned(),
                                        value: Bytes::from(500u64).lpad(32, 0).to_vec(),
                                        change: ChangeType::Update.into(),
                                    },
                                    Attribute {
                                        name: "static_attribute".to_owned(),
                                        value: Bytes::from(1u64).lpad(32, 0).to_vec(),
                                        change: ChangeType::Update.into(),
                                    },
                                ],
                            },
                        ],
                        component_changes: vec![],
                        balance_changes: vec![],
                    },
                    TransactionEntityChanges {
                        tx: Some(Transaction {
                            hash: Bytes::from(vec![0x11, 0x12, 0x13, 0x14])
                                .lpad(32, 0)
                                .to_vec(),
                            from: Bytes::from(vec![0x41, 0x42, 0x43, 0x44])
                                .lpad(20, 0)
                                .to_vec(),
                            to: Bytes::from(vec![0x51, 0x52, 0x53, 0x54])
                                .lpad(20, 0)
                                .to_vec(),
                            index: 11,
                        }),
                        entity_changes: vec![EntityChanges {
                            component_id: "State1".to_owned(),
                            attributes: vec![
                                Attribute {
                                    name: "reserve".to_owned(),
                                    value: Bytes::from(600u64).lpad(32, 0).to_vec(),
                                    change: ChangeType::Update.into(),
                                },
                                Attribute {
                                    name: "new".to_owned(),
                                    value: Bytes::zero(32).to_vec(),
                                    change: ChangeType::Update.into(),
                                },
                            ],
                        }],
                        component_changes: vec![ProtocolComponent {
                            id: "Pool".to_owned(),
                            tokens: vec![
                                address_from_str(DAI_ADDRESS),
                                address_from_str(DAI_ADDRESS),
                            ],
                            contracts: vec![address_from_str(WETH_ADDRESS)],
                            static_att: vec![Attribute {
                                name: "key".to_owned(),
                                value: Bytes::from(600u64).lpad(32, 0).to_vec(),
                                change: ChangeType::Creation.into(),
                            }],
                            change: ChangeType::Creation.into(),
                            protocol_type: Some(ProtocolType {
                                name: "WeightedPool".to_string(),
                                financial_type: 0,
                                attribute_schema: vec![],
                                implementation_type: 0,
                            }),
                        }],
                        balance_changes: vec![BalanceChange {
                            token: address_from_str(DAI_ADDRESS),
                            balance: 1_i32.to_le_bytes().to_vec(),
                            component_id: "Balance1".into(),
                        }],
                    },
                ],
            },
            1 => BlockEntityChanges {
                block: Some(pb_blocks(version as u64)),
                changes: vec![TransactionEntityChanges {
                    tx: Some(pb_transactions(1, 1)),
                    entity_changes: vec![EntityChanges {
                        component_id: "pc_1".to_owned(),
                        attributes: vec![
                            Attribute {
                                name: "attr_1".to_owned(),
                                value: Bytes::from(1u64).lpad(32, 0).to_vec(),
                                change: ChangeType::Update.into(),
                            },
                            Attribute {
                                name: "attr_2".to_owned(),
                                value: Bytes::from(2u64).lpad(32, 0).to_vec(),
                                change: ChangeType::Update.into(),
                            },
                        ],
                    }],
                    component_changes: vec![ProtocolComponent {
                        id: "pc_1".to_owned(),
                        tokens: vec![
                            address_from_str(WETH_ADDRESS),
                            address_from_str(USDC_ADDRESS),
                        ],
                        contracts: vec![],
                        static_att: vec![Attribute {
                            name: "st_attr_1".to_owned(),
                            value: Bytes::from(1u64).lpad(32, 0).to_vec(),
                            change: ChangeType::Creation.into(),
                        }],
                        change: ChangeType::Creation.into(),
                        protocol_type: Some(ProtocolType {
                            name: "pt_1".to_string(),
                            financial_type: 0,
                            attribute_schema: vec![],
                            implementation_type: 0,
                        }),
                    }],
                    balance_changes: vec![BalanceChange {
                        token: address_from_str(USDC_ADDRESS),
                        balance: 1_i32.to_be_bytes().to_vec(),
                        component_id: "pc_1".into(),
                    }],
                }],
            },
            2 => BlockEntityChanges {
                block: Some(pb_blocks(version as u64)),
                changes: vec![TransactionEntityChanges {
                    tx: Some(pb_transactions(2, 1)),
                    entity_changes: vec![EntityChanges {
                        component_id: "pc_1".to_owned(),
                        attributes: vec![Attribute {
                            name: "attr_1".to_owned(),
                            value: Bytes::from(10u64).lpad(32, 0).to_vec(),
                            change: ChangeType::Update.into(),
                        }],
                    }],
                    component_changes: vec![ProtocolComponent {
                        id: "pc_2".to_owned(),
                        tokens: vec![
                            address_from_str(USDT_ADDRESS),
                            address_from_str(USDC_ADDRESS),
                        ],
                        contracts: vec![],
                        static_att: vec![],
                        change: ChangeType::Creation.into(),
                        protocol_type: Some(ProtocolType {
                            name: "pt_1".to_string(),
                            financial_type: 0,
                            attribute_schema: vec![],
                            implementation_type: 0,
                        }),
                    }],
                    balance_changes: vec![
                        BalanceChange {
                            token: address_from_str(USDC_ADDRESS),
                            balance: 1_i32.to_be_bytes().to_vec(),
                            component_id: "pc_2".into(),
                        },
                        BalanceChange {
                            token: address_from_str(USDT_ADDRESS),
                            balance: 1_i32.to_be_bytes().to_vec(),
                            component_id: "pc_2".into(),
                        },
                        BalanceChange {
                            token: address_from_str(WETH_ADDRESS),
                            balance: 1_i32.to_be_bytes().to_vec(),
                            component_id: "pc_1".into(),
                        },
                    ],
                }],
            },
            3 => BlockEntityChanges {
                block: Some(pb_blocks(version as u64)),
                changes: vec![
                    TransactionEntityChanges {
                        tx: Some(pb_transactions(3, 2)),
                        entity_changes: vec![EntityChanges {
                            component_id: "pc_1".to_owned(),
                            attributes: vec![Attribute {
                                name: "attr_1".to_owned(),
                                value: Bytes::from(1000u64)
                                    .lpad(32, 0)
                                    .to_vec(),
                                change: ChangeType::Update.into(),
                            }],
                        }],
                        component_changes: vec![],
                        balance_changes: vec![BalanceChange {
                            token: address_from_str(USDC_ADDRESS),
                            balance: 3_i32.to_be_bytes().to_vec(),
                            component_id: "pc_2".into(),
                        }],
                    },
                    TransactionEntityChanges {
                        tx: Some(pb_transactions(3, 1)),
                        entity_changes: vec![EntityChanges {
                            component_id: "pc_1".to_owned(),
                            attributes: vec![Attribute {
                                name: "attr_1".to_owned(),
                                value: Bytes::from(99999u64)
                                    .lpad(32, 0)
                                    .to_vec(),
                                change: ChangeType::Update.into(),
                            }],
                        }],
                        component_changes: vec![],
                        balance_changes: vec![
                            BalanceChange {
                                token: address_from_str(USDC_ADDRESS),
                                balance: 99999_i32.to_be_bytes().to_vec(),
                                component_id: "pc_2".into(),
                            },
                            BalanceChange {
                                token: address_from_str(WETH_ADDRESS),
                                balance: 1000_i32.to_be_bytes().to_vec(),
                                component_id: "pc_1".into(),
                            },
                        ],
                    },
                ],
            },
            4 => BlockEntityChanges {
                block: Some(pb_blocks(version as u64)),
                changes: vec![
                    TransactionEntityChanges {
                        tx: Some(pb_transactions(4, 1)),
                        entity_changes: vec![
                            EntityChanges {
                                component_id: "pc_1".to_owned(),
                                attributes: vec![Attribute {
                                    name: "attr_1".to_owned(),
                                    value: Bytes::from(10_000u64)
                                        .lpad(32, 0)
                                        .to_vec(),
                                    change: ChangeType::Update.into(),
                                }],
                            },
                            EntityChanges {
                                component_id: "pc_3".to_owned(),
                                attributes: vec![Attribute {
                                    name: "attr_1".to_owned(),
                                    value: Bytes::from(3u64).lpad(32, 0).to_vec(),
                                    change: ChangeType::Update.into(),
                                }],
                            },
                        ],
                        component_changes: vec![ProtocolComponent {
                            id: "pc_3".to_owned(),
                            tokens: vec![
                                address_from_str(DAI_ADDRESS),
                                address_from_str(WETH_ADDRESS),
                            ],
                            contracts: vec![],
                            static_att: vec![],
                            change: ChangeType::Creation.into(),
                            protocol_type: Some(ProtocolType {
                                name: "pt_2".to_string(),
                                financial_type: 0,
                                attribute_schema: vec![],
                                implementation_type: 0,
                            }),
                        }],
                        balance_changes: vec![],
                    },
                    TransactionEntityChanges {
                        tx: Some(pb_transactions(4, 2)),
                        entity_changes: vec![
                            EntityChanges {
                                component_id: "pc_3".to_owned(),
                                attributes: vec![Attribute {
                                    name: "attr_1".to_owned(),
                                    value: Bytes::from(300u64).lpad(32, 0).to_vec(),
                                    change: ChangeType::Update.into(),
                                }],
                            },
                            EntityChanges {
                                component_id: "pc_1".to_owned(),
                                attributes: vec![Attribute {
                                    name: "attr_1".to_owned(),
                                    value: Bytes::from(100_000u64)
                                        .lpad(32, 0)
                                        .to_vec(),
                                    change: ChangeType::Update.into(),
                                }],
                            },
                        ],
                        component_changes: vec![],
                        balance_changes: vec![
                            BalanceChange {
                                token: address_from_str(USDC_ADDRESS),
                                balance: 3000_i32.to_be_bytes().to_vec(),
                                component_id: "pc_3".into(),
                            },
                            BalanceChange {
                                token: address_from_str(USDC_ADDRESS),
                                balance: 1000_i32.to_be_bytes().to_vec(),
                                component_id: "pc_1".into(),
                            },
                        ],
                    },
                ],
            },
            5 => BlockEntityChanges {
                block: Some(pb_blocks(version as u64)),
                changes: vec![TransactionEntityChanges {
                    tx: Some(pb_transactions(5, 1)),
                    entity_changes: vec![EntityChanges {
                        component_id: "pc_1".to_owned(),
                        attributes: vec![Attribute {
                            name: "attr_2".to_owned(),
                            value: Bytes::from(1_000_000u64)
                                .lpad(32, 0)
                                .to_vec(),
                            change: ChangeType::Update.into(),
                        }],
                    }],
                    component_changes: vec![ProtocolComponent {
                        id: "pc_2".to_owned(),
                        tokens: vec![
                            address_from_str(USDT_ADDRESS),
                            address_from_str(USDC_ADDRESS),
                        ],
                        contracts: vec![],
                        static_att: vec![],
                        change: ChangeType::Deletion.into(),
                        protocol_type: Some(ProtocolType {
                            name: "pt_1".to_string(),
                            financial_type: 0,
                            attribute_schema: vec![],
                            implementation_type: 0,
                        }),
                    }],
                    balance_changes: vec![BalanceChange {
                        token: address_from_str(WETH_ADDRESS),
                        balance: 1000_i32.to_be_bytes().to_vec(),
                        component_id: "pc_1".into(),
                    }],
                }],
            },
            _ => panic!("Requested unknown version of block entity changes"),
        }
    }

    pub fn pb_block_scoped_data(
        msg: impl prost::Message,
        cursor: Option<&str>,
        final_block_height: Option<u64>,
    ) -> crate::pb::sf::substreams::rpc::v2::BlockScopedData {
        use crate::pb::sf::substreams::{rpc::v2::*, v1::Clock};
        let val = msg.encode_to_vec();
        BlockScopedData {
            output: Some(MapModuleOutput {
                name: "map_changes".to_owned(),
                map_output: Some(prost_types::Any {
                    type_url: "tycho.evm.v1.BlockChanges".to_owned(),
                    value: val,
                }),
                debug_info: None,
            }),
            clock: Some(Clock {
                id: HASH_256_0.to_owned(),
                number: 420,
                timestamp: Some(prost_types::Timestamp { seconds: 1000, nanos: 0 }),
            }),
            cursor: cursor
                .unwrap_or("cursor@420")
                .to_owned(),
            final_block_height: final_block_height.unwrap_or(420),
            debug_map_outputs: vec![],
            debug_store_outputs: vec![],
        }
    }

    pub fn pb_vm_block_changes(version: u8) -> BlockChanges {
        match version {
            0 => BlockChanges {
                block: Some(Block {
                    hash: vec![0x31, 0x32, 0x33, 0x34],
                    parent_hash: vec![0x21, 0x22, 0x23, 0x24],
                    number: 1,
                    ts: 1000,
                }),

                changes: vec![
                    TransactionChanges {
                        tx: Some(Transaction {
                            hash: vec![0x11, 0x12, 0x13, 0x14],
                            from: vec![0x41, 0x42, 0x43, 0x44],
                            to: vec![0x51, 0x52, 0x53, 0x54],
                            index: 2,
                        }),
                        contract_changes: vec![ContractChange {
                            address: vec![0x61, 0x62, 0x63, 0x64],
                            balance: vec![0x71, 0x72, 0x73, 0x74],
                            code: vec![0x81, 0x82, 0x83, 0x84],
                            slots: vec![
                                ContractSlot {
                                    slot: vec![0xa1, 0xa2, 0xa3, 0xa4],
                                    value: vec![0xb1, 0xb2, 0xb3, 0xb4],
                                },
                                ContractSlot {
                                    slot: vec![0xc1, 0xc2, 0xc3, 0xc4],
                                    value: vec![0xd1, 0xd2, 0xd3, 0xd4],
                                },
                            ],
                            change: ChangeType::Update.into(),
                            token_balances: vec![AccountBalanceChange {
                                token: address_from_str(WETH_ADDRESS),
                                balance: 50000000.encode_to_vec(),
                            }],
                        }],
                        entity_changes: vec![],
                        component_changes: vec![ProtocolComponent {
                            id: "d417ff54652c09bd9f31f216b1a2e5d1e28c1dce1ba840c40d16f2b4d09b5902"
                                .to_owned(),
                            tokens: vec![
                                address_from_str(DAI_ADDRESS),
                                address_from_str(DAI_ADDRESS),
                            ],
                            contracts: vec![
                                address_from_str(WETH_ADDRESS),
                                address_from_str(WETH_ADDRESS),
                            ],
                            static_att: vec![
                                Attribute {
                                    name: "key1".to_owned(),
                                    value: b"value1".to_vec(),
                                    change: ChangeType::Creation.into(),
                                },
                                Attribute {
                                    name: "key2".to_owned(),
                                    value: b"value2".to_vec(),
                                    change: ChangeType::Creation.into(),
                                },
                            ],
                            change: ChangeType::Creation.into(),
                            protocol_type: Some(ProtocolType {
                                name: "WeightedPool".to_string(),
                                financial_type: 0,
                                attribute_schema: vec![],
                                implementation_type: 0,
                            }),
                        }],
                        balance_changes: vec![BalanceChange {
                            token: address_from_str(WETH_ADDRESS),
                            balance: 50000000.encode_to_vec(),
                            component_id:
                                "d417ff54652c09bd9f31f216b1a2e5d1e28c1dce1ba840c40d16f2b4d09b5902"
                                    .as_bytes()
                                    .to_vec(),
                        }],
                    },
                    TransactionChanges {
                        tx: Some(Transaction {
                            hash: vec![
                                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
                            ],
                            from: vec![0x41, 0x42, 0x43, 0x44],
                            to: vec![0x51, 0x52, 0x53, 0x54],
                            index: 5,
                        }),
                        contract_changes: vec![ContractChange {
                            address: vec![0x61, 0x62, 0x63, 0x64],
                            balance: vec![0xf1, 0xf2, 0xf3, 0xf4],
                            code: vec![0x01, 0x02, 0x03, 0x04],
                            slots: vec![
                                ContractSlot {
                                    slot: vec![0x91, 0x92, 0x93, 0x94],
                                    value: vec![0xa1, 0xa2, 0xa3, 0xa4],
                                },
                                ContractSlot {
                                    slot: vec![0xa1, 0xa2, 0xa3, 0xa4],
                                    value: vec![0xc1, 0xc2, 0xc3, 0xc4],
                                },
                            ],
                            change: ChangeType::Update.into(),
                            token_balances: vec![AccountBalanceChange {
                                token: address_from_str(WETH_ADDRESS),
                                balance: 10.encode_to_vec(),
                            }],
                        }],
                        entity_changes: vec![],
                        component_changes: vec![],
                        balance_changes: vec![BalanceChange {
                            token: address_from_str(WETH_ADDRESS),
                            balance: 10.encode_to_vec(),
                            component_id:
                                "d417ff54652c09bd9f31f216b1a2e5d1e28c1dce1ba840c40d16f2b4d09b5902"
                                    .to_string()
                                    .as_bytes()
                                    .to_vec(),
                        }],
                    },
                ],
            },
            1 => BlockChanges {
                block: Some(pb_blocks(version as u64)),
                changes: vec![TransactionChanges {
                    tx: Some(pb_transactions(1, 1)),
                    contract_changes: vec![ContractChange {
                        address: address_from_str("0000000000000000000000000000000000000001"),
                        balance: 1_i32.to_be_bytes().to_vec(),
                        code: 123_i32.to_be_bytes().to_vec(),
                        slots: vec![ContractSlot {
                            slot: Bytes::from("0x01").into(),
                            value: Bytes::from("0x01").into(),
                        }],
                        change: ChangeType::Creation.into(),
                        token_balances: vec![
                            AccountBalanceChange {
                                token: address_from_str(WETH_ADDRESS),
                                balance: 1_i32.to_be_bytes().to_vec(),
                            },
                            AccountBalanceChange {
                                token: address_from_str(USDC_ADDRESS),
                                balance: 1_i32.to_be_bytes().to_vec(),
                            },
                        ],
                    }],
                    entity_changes: vec![],
                    component_changes: vec![ProtocolComponent {
                        id: "pc_1".to_owned(),
                        tokens: vec![
                            address_from_str(WETH_ADDRESS),
                            address_from_str(USDC_ADDRESS),
                        ],
                        contracts: vec![],
                        static_att: vec![],
                        change: ChangeType::Creation.into(),
                        protocol_type: Some(ProtocolType {
                            name: "pt_1".to_string(),
                            financial_type: 0,
                            attribute_schema: vec![],
                            implementation_type: 0,
                        }),
                    }],
                    balance_changes: vec![
                        BalanceChange {
                            token: address_from_str(USDC_ADDRESS),
                            balance: 1_i32.to_be_bytes().to_vec(),
                            component_id: "pc_1".into(),
                        },
                        BalanceChange {
                            token: address_from_str(WETH_ADDRESS),
                            balance: 1_i32.to_be_bytes().to_vec(),
                            component_id: "pc_1".into(),
                        },
                    ],
                }],
            },
            2 => BlockChanges {
                block: Some(pb_blocks(version as u64)),
                changes: vec![TransactionChanges {
                    tx: Some(pb_transactions(2, 1)),
                    contract_changes: vec![
                        ContractChange {
                            address: address_from_str("0000000000000000000000000000000000000002"),
                            balance: 2_i32.to_be_bytes().to_vec(),
                            code: 123_i32.to_be_bytes().to_vec(),
                            slots: vec![ContractSlot {
                                slot: Bytes::from("0x01").into(),
                                value: Bytes::from("0x02").into(),
                            }],
                            change: ChangeType::Creation.into(),
                            token_balances: vec![
                                AccountBalanceChange {
                                    token: address_from_str(USDT_ADDRESS),
                                    balance: 20_i32.to_be_bytes().to_vec(),
                                },
                                AccountBalanceChange {
                                    token: address_from_str(USDC_ADDRESS),
                                    balance: 20_i32.to_be_bytes().to_vec(),
                                },
                            ],
                        },
                        ContractChange {
                            address: address_from_str("0000000000000000000000000000000000000001"),
                            balance: 10_i32.to_be_bytes().to_vec(),
                            code: 123_i32.to_be_bytes().to_vec(),
                            slots: vec![
                                ContractSlot {
                                    slot: Bytes::from("0x01").into(),
                                    value: Bytes::from("0x10").into(),
                                },
                                ContractSlot {
                                    slot: Bytes::from("0x02").into(),
                                    value: Bytes::from("0x0a").into(),
                                },
                            ],
                            change: ChangeType::Update.into(),
                            token_balances: vec![AccountBalanceChange {
                                token: address_from_str(USDC_ADDRESS),
                                balance: 10_i32.to_be_bytes().to_vec(),
                            }],
                        },
                    ],
                    entity_changes: vec![],
                    component_changes: vec![ProtocolComponent {
                        id: "pc_2".to_owned(),
                        tokens: vec![
                            address_from_str(USDT_ADDRESS),
                            address_from_str(USDC_ADDRESS),
                        ],
                        contracts: vec![address_from_str(
                            "0000000000000000000000000000000000000002",
                        )],
                        static_att: vec![],
                        change: ChangeType::Creation.into(),
                        protocol_type: Some(ProtocolType {
                            name: "pt_1".to_string(),
                            financial_type: 0,
                            attribute_schema: vec![],
                            implementation_type: 0,
                        }),
                    }],
                    balance_changes: vec![
                        BalanceChange {
                            token: address_from_str(USDC_ADDRESS),
                            balance: 20_i32.to_be_bytes().to_vec(),
                            component_id: "pc_2".into(),
                        },
                        BalanceChange {
                            token: address_from_str(USDT_ADDRESS),
                            balance: 20_i32.to_be_bytes().to_vec(),
                            component_id: "pc_2".into(),
                        },
                        BalanceChange {
                            token: address_from_str(USDC_ADDRESS),
                            balance: 10_i32.to_be_bytes().to_vec(),
                            component_id: "pc_1".into(),
                        },
                    ],
                }],
            },
            3 => BlockChanges {
                block: Some(pb_blocks(version as u64)),
                changes: vec![
                    TransactionChanges {
                        tx: Some(pb_transactions(3, 1)),
                        contract_changes: vec![
                            ContractChange {
                                address: address_from_str(
                                    "0000000000000000000000000000000000000001",
                                ),
                                balance: 1_i32.to_be_bytes().to_vec(),
                                code: 123_i32.to_le_bytes().to_vec(),
                                slots: vec![ContractSlot {
                                    slot: Bytes::from("0x01").into(),
                                    value: Bytes::from("0x01").into(),
                                }],
                                change: ChangeType::Update.into(),
                                token_balances: vec![],
                            },
                            ContractChange {
                                address: address_from_str(
                                    "0000000000000000000000000000000000000002",
                                ),
                                balance: 20_i32.to_be_bytes().to_vec(),
                                code: 123_i32.to_le_bytes().to_vec(),
                                slots: vec![ContractSlot {
                                    slot: Bytes::from("0x02").into(),
                                    value: Bytes::from("0xc8").into(),
                                }],
                                change: ChangeType::Update.into(),
                                token_balances: vec![AccountBalanceChange {
                                    token: address_from_str(USDC_ADDRESS),
                                    balance: 1_i32.to_be_bytes().to_vec(),
                                }],
                            },
                        ],
                        entity_changes: vec![],
                        component_changes: vec![],
                        balance_changes: vec![BalanceChange {
                            token: address_from_str(USDC_ADDRESS),
                            balance: 1_i32.to_be_bytes().to_vec(),
                            component_id: "pc_2".into(),
                        }],
                    },
                    TransactionChanges {
                        tx: Some(pb_transactions(3, 2)),
                        contract_changes: vec![ContractChange {
                            address: address_from_str("0000000000000000000000000000000000000001"),
                            balance: 1_i32.to_be_bytes().to_vec(),
                            code: 123_i32.to_le_bytes().to_vec(),
                            slots: vec![ContractSlot {
                                slot: Bytes::from("0x01").into(),
                                value: Bytes::from("0x01").into(),
                            }],
                            change: ChangeType::Update.into(),
                            token_balances: vec![AccountBalanceChange {
                                token: address_from_str(USDC_ADDRESS),
                                balance: 100_i32.to_be_bytes().to_vec(),
                            }],
                        }],
                        entity_changes: vec![],
                        component_changes: vec![],
                        balance_changes: vec![
                            BalanceChange {
                                token: address_from_str(USDC_ADDRESS),
                                balance: 100_i32.to_be_bytes().to_vec(),
                                component_id: "pc_1".into(),
                            },
                            BalanceChange {
                                token: address_from_str(USDC_ADDRESS),
                                balance: 2_i32.to_be_bytes().to_vec(),
                                component_id: "pc_2".into(),
                            },
                        ],
                    },
                ],
            },
            4 => BlockChanges {
                block: Some(pb_blocks(version as u64)),
                changes: vec![TransactionChanges {
                    tx: Some(pb_transactions(4, 1)),
                    contract_changes: vec![ContractChange {
                        address: address_from_str("0000000000000000000000000000000000000001"),
                        balance: 1_i32.to_be_bytes().to_vec(),
                        code: 123_i32.to_le_bytes().to_vec(),
                        slots: vec![ContractSlot {
                            slot: Bytes::from("0x03").into(),
                            value: Bytes::from("0x10").into(),
                        }],
                        change: ChangeType::Update.into(),
                        token_balances: vec![],
                    }],
                    entity_changes: vec![],
                    component_changes: vec![ProtocolComponent {
                        id: "pc_3".to_owned(),
                        tokens: vec![address_from_str(DAI_ADDRESS), address_from_str(USDC_ADDRESS)],
                        contracts: vec![address_from_str(
                            "0000000000000000000000000000000000000001",
                        )],
                        static_att: vec![],
                        change: ChangeType::Creation.into(),
                        protocol_type: Some(ProtocolType {
                            name: "pt_1".to_string(),
                            financial_type: 0,
                            attribute_schema: vec![],
                            implementation_type: 0,
                        }),
                    }],
                    balance_changes: vec![],
                }],
            },
            5 => BlockChanges {
                block: Some(pb_blocks(version as u64)),
                changes: vec![TransactionChanges {
                    tx: Some(pb_transactions(5, 1)),
                    contract_changes: vec![
                        ContractChange {
                            address: address_from_str("0000000000000000000000000000000000000001"),
                            balance: 1_i32.to_be_bytes().to_vec(),
                            code: 123_i32.to_le_bytes().to_vec(),
                            slots: vec![ContractSlot {
                                slot: Bytes::from("0x01").into(),
                                value: Bytes::from("0x10").into(),
                            }],
                            change: ChangeType::Update.into(),
                            token_balances: vec![
                                AccountBalanceChange {
                                    token: address_from_str(USDC_ADDRESS),
                                    balance: 100_i32.to_be_bytes().to_vec(),
                                },
                                AccountBalanceChange {
                                    token: address_from_str(WETH_ADDRESS),
                                    balance: 100_i32.to_be_bytes().to_vec(),
                                },
                            ],
                        },
                        ContractChange {
                            address: address_from_str("0000000000000000000000000000000000000002"),
                            balance: 1_i32.to_be_bytes().to_vec(),
                            code: 123_i32.to_le_bytes().to_vec(),
                            slots: vec![ContractSlot {
                                slot: Bytes::from("0x01").into(),
                                value: Bytes::from("0x10").into(),
                            }],
                            change: ChangeType::Update.into(),
                            token_balances: vec![AccountBalanceChange {
                                token: address_from_str(USDC_ADDRESS),
                                balance: 1_i32.to_be_bytes().to_vec(),
                            }],
                        },
                    ],
                    entity_changes: vec![],
                    component_changes: vec![],
                    balance_changes: vec![
                        BalanceChange {
                            token: address_from_str(USDC_ADDRESS),
                            balance: 1_i32.to_be_bytes().to_vec(),
                            component_id: "pc_1".into(),
                        },
                        BalanceChange {
                            token: address_from_str(USDC_ADDRESS),
                            balance: 1_i32.to_be_bytes().to_vec(),
                            component_id: "pc_3".into(),
                        },
                        BalanceChange {
                            token: address_from_str(WETH_ADDRESS),
                            balance: 1_i32.to_be_bytes().to_vec(),
                            component_id: "pc_1".into(),
                        },
                    ],
                }],
            },
            _ => panic!("Requested BlockChanges version doesn't exist"),
        }
    }

    pub fn pb_native_block_changes(version: u8) -> BlockChanges {
        match version {
            0 => BlockChanges {
                block: Some(Block {
                    hash: vec![0x0, 0x0, 0x0, 0x0],
                    parent_hash: vec![0x21, 0x22, 0x23, 0x24],
                    number: 1,
                    ts: yesterday_midnight().timestamp() as u64,
                }),
                changes: vec![
                    TransactionChanges {
                        tx: Some(Transaction {
                            hash: vec![0x0, 0x0, 0x0, 0x0],
                            from: vec![0x0, 0x0, 0x0, 0x0],
                            to: vec![0x0, 0x0, 0x0, 0x0],
                            index: 10,
                        }),
                        contract_changes: vec![],
                        entity_changes: vec![
                            EntityChanges {
                                component_id: "State1".to_owned(),
                                attributes: vec![
                                    Attribute {
                                        name: "reserve".to_owned(),
                                        value: Bytes::from(1_000u64)
                                            .lpad(32, 0)
                                            .to_vec(),
                                        change: ChangeType::Update.into(),
                                    },
                                    Attribute {
                                        name: "static_attribute".to_owned(),
                                        value: Bytes::from(1u64).lpad(32, 0).to_vec(),
                                        change: ChangeType::Update.into(),
                                    },
                                ],
                            },
                            EntityChanges {
                                component_id: "State2".to_owned(),
                                attributes: vec![
                                    Attribute {
                                        name: "reserve".to_owned(),
                                        value: Bytes::from(1_000u64)
                                            .lpad(32, 0)
                                            .to_vec(),
                                        change: ChangeType::Update.into(),
                                    },
                                    Attribute {
                                        name: "static_attribute".to_owned(),
                                        value: Bytes::from(1u64).lpad(32, 0).to_vec(),
                                        change: ChangeType::Update.into(),
                                    },
                                ],
                            },
                        ],
                        component_changes: vec![],
                        balance_changes: vec![],
                    },
                    TransactionChanges {
                        tx: Some(Transaction {
                            hash: vec![0x11, 0x12, 0x13, 0x14],
                            from: vec![0x41, 0x42, 0x43, 0x44],
                            to: vec![0x51, 0x52, 0x53, 0x54],
                            index: 11,
                        }),
                        contract_changes: vec![],
                        entity_changes: vec![EntityChanges {
                            component_id: "State1".to_owned(),
                            attributes: vec![
                                Attribute {
                                    name: "reserve".to_owned(),
                                    value: Bytes::from(600u64).lpad(32, 0).to_vec(),
                                    change: ChangeType::Update.into(),
                                },
                                Attribute {
                                    name: "new".to_owned(),
                                    value: Bytes::zero(32).to_vec(),
                                    change: ChangeType::Update.into(),
                                },
                            ],
                        }],
                        component_changes: vec![ProtocolComponent {
                            id: "Pool".to_owned(),
                            tokens: vec![
                                address_from_str(DAI_ADDRESS),
                                address_from_str(DAI_ADDRESS),
                            ],
                            contracts: vec![address_from_str(WETH_ADDRESS)],
                            static_att: vec![Attribute {
                                name: "key".to_owned(),
                                value: Bytes::from(600u64).lpad(32, 0).to_vec(),
                                change: ChangeType::Creation.into(),
                            }],
                            change: ChangeType::Creation.into(),
                            protocol_type: Some(ProtocolType {
                                name: "WeightedPool".to_string(),
                                financial_type: 0,
                                attribute_schema: vec![],
                                implementation_type: 0,
                            }),
                        }],
                        balance_changes: vec![BalanceChange {
                            token: address_from_str(DAI_ADDRESS),
                            balance: 1_i32.to_le_bytes().to_vec(),
                            component_id: "Balance1".into(),
                        }],
                    },
                ],
            },
            1 => BlockChanges {
                block: Some(pb_blocks(version as u64)),
                changes: vec![TransactionChanges {
                    tx: Some(pb_transactions(1, 1)),
                    contract_changes: vec![],
                    entity_changes: vec![EntityChanges {
                        component_id: "pc_1".to_owned(),
                        attributes: vec![
                            Attribute {
                                name: "attr_1".to_owned(),
                                value: Bytes::from(1u64).lpad(32, 0).to_vec(),
                                change: ChangeType::Update.into(),
                            },
                            Attribute {
                                name: "attr_2".to_owned(),
                                value: Bytes::from(2u64).lpad(32, 0).to_vec(),
                                change: ChangeType::Update.into(),
                            },
                        ],
                    }],
                    component_changes: vec![ProtocolComponent {
                        id: "pc_1".to_owned(),
                        tokens: vec![
                            address_from_str(WETH_ADDRESS),
                            address_from_str(USDC_ADDRESS),
                        ],
                        contracts: vec![],
                        static_att: vec![Attribute {
                            name: "st_attr_1".to_owned(),
                            value: Bytes::from(1u64).lpad(32, 0).to_vec(),
                            change: ChangeType::Creation.into(),
                        }],
                        change: ChangeType::Creation.into(),
                        protocol_type: Some(ProtocolType {
                            name: "pt_1".to_string(),
                            financial_type: 0,
                            attribute_schema: vec![],
                            implementation_type: 0,
                        }),
                    }],
                    balance_changes: vec![BalanceChange {
                        token: address_from_str(USDC_ADDRESS),
                        balance: 1_i32.to_be_bytes().to_vec(),
                        component_id: "pc_1".into(),
                    }],
                }],
            },
            2 => BlockChanges {
                block: Some(pb_blocks(version as u64)),
                changes: vec![TransactionChanges {
                    tx: Some(pb_transactions(2, 1)),
                    contract_changes: vec![],
                    entity_changes: vec![EntityChanges {
                        component_id: "pc_1".to_owned(),
                        attributes: vec![Attribute {
                            name: "attr_1".to_owned(),
                            value: Bytes::from(10u64).lpad(32, 0).to_vec(),
                            change: ChangeType::Update.into(),
                        }],
                    }],
                    component_changes: vec![ProtocolComponent {
                        id: "pc_2".to_owned(),
                        tokens: vec![
                            address_from_str(USDT_ADDRESS),
                            address_from_str(USDC_ADDRESS),
                        ],
                        contracts: vec![],
                        static_att: vec![],
                        change: ChangeType::Creation.into(),
                        protocol_type: Some(ProtocolType {
                            name: "pt_1".to_string(),
                            financial_type: 0,
                            attribute_schema: vec![],
                            implementation_type: 0,
                        }),
                    }],
                    balance_changes: vec![
                        BalanceChange {
                            token: address_from_str(USDC_ADDRESS),
                            balance: 1_i32.to_be_bytes().to_vec(),
                            component_id: "pc_2".into(),
                        },
                        BalanceChange {
                            token: address_from_str(USDT_ADDRESS),
                            balance: 1_i32.to_be_bytes().to_vec(),
                            component_id: "pc_2".into(),
                        },
                        BalanceChange {
                            token: address_from_str(WETH_ADDRESS),
                            balance: 1_i32.to_be_bytes().to_vec(),
                            component_id: "pc_1".into(),
                        },
                    ],
                }],
            },
            3 => BlockChanges {
                block: Some(pb_blocks(version as u64)),
                changes: vec![
                    TransactionChanges {
                        tx: Some(pb_transactions(3, 2)),
                        contract_changes: vec![],
                        entity_changes: vec![EntityChanges {
                            component_id: "pc_1".to_owned(),
                            attributes: vec![Attribute {
                                name: "attr_1".to_owned(),
                                value: Bytes::from(1_000u64)
                                    .lpad(32, 0)
                                    .to_vec(),
                                change: ChangeType::Update.into(),
                            }],
                        }],
                        component_changes: vec![],
                        balance_changes: vec![BalanceChange {
                            token: address_from_str(USDC_ADDRESS),
                            balance: 3_i32.to_be_bytes().to_vec(),
                            component_id: "pc_2".into(),
                        }],
                    },
                    TransactionChanges {
                        tx: Some(pb_transactions(3, 1)),
                        contract_changes: vec![],
                        entity_changes: vec![EntityChanges {
                            component_id: "pc_1".to_owned(),
                            attributes: vec![Attribute {
                                name: "attr_1".to_owned(),
                                value: Bytes::from(99_999u64)
                                    .lpad(32, 0)
                                    .to_vec(),
                                change: ChangeType::Update.into(),
                            }],
                        }],
                        component_changes: vec![],
                        balance_changes: vec![
                            BalanceChange {
                                token: address_from_str(USDC_ADDRESS),
                                balance: 99999_i32.to_be_bytes().to_vec(),
                                component_id: "pc_2".into(),
                            },
                            BalanceChange {
                                token: address_from_str(WETH_ADDRESS),
                                balance: 1000_i32.to_be_bytes().to_vec(),
                                component_id: "pc_1".into(),
                            },
                        ],
                    },
                ],
            },
            4 => BlockChanges {
                block: Some(pb_blocks(version as u64)),
                changes: vec![
                    TransactionChanges {
                        tx: Some(pb_transactions(4, 1)),
                        contract_changes: vec![],
                        entity_changes: vec![
                            EntityChanges {
                                component_id: "pc_1".to_owned(),
                                attributes: vec![Attribute {
                                    name: "attr_1".to_owned(),
                                    value: Bytes::from(10_000u64)
                                        .lpad(32, 0)
                                        .to_vec(),
                                    change: ChangeType::Update.into(),
                                }],
                            },
                            EntityChanges {
                                component_id: "pc_3".to_owned(),
                                attributes: vec![Attribute {
                                    name: "attr_1".to_owned(),
                                    value: Bytes::from(3u64).lpad(32, 0).to_vec(),
                                    change: ChangeType::Update.into(),
                                }],
                            },
                        ],
                        component_changes: vec![ProtocolComponent {
                            id: "pc_3".to_owned(),
                            tokens: vec![
                                address_from_str(DAI_ADDRESS),
                                address_from_str(WETH_ADDRESS),
                            ],
                            contracts: vec![],
                            static_att: vec![],
                            change: ChangeType::Creation.into(),
                            protocol_type: Some(ProtocolType {
                                name: "pt_2".to_string(),
                                financial_type: 0,
                                attribute_schema: vec![],
                                implementation_type: 0,
                            }),
                        }],
                        balance_changes: vec![],
                    },
                    TransactionChanges {
                        tx: Some(pb_transactions(4, 2)),
                        contract_changes: vec![],
                        entity_changes: vec![
                            EntityChanges {
                                component_id: "pc_3".to_owned(),
                                attributes: vec![Attribute {
                                    name: "attr_1".to_owned(),
                                    value: Bytes::from(30u64).lpad(32, 0).to_vec(),
                                    change: ChangeType::Update.into(),
                                }],
                            },
                            EntityChanges {
                                component_id: "pc_1".to_owned(),
                                attributes: vec![Attribute {
                                    name: "attr_1".to_owned(),
                                    value: Bytes::from(100_000u64)
                                        .lpad(32, 0)
                                        .to_vec(),
                                    change: ChangeType::Update.into(),
                                }],
                            },
                        ],
                        component_changes: vec![],
                        balance_changes: vec![
                            BalanceChange {
                                token: address_from_str(USDC_ADDRESS),
                                balance: 3000_i32.to_be_bytes().to_vec(),
                                component_id: "pc_3".into(),
                            },
                            BalanceChange {
                                token: address_from_str(USDC_ADDRESS),
                                balance: 1000_i32.to_be_bytes().to_vec(),
                                component_id: "pc_1".into(),
                            },
                        ],
                    },
                ],
            },
            5 => BlockChanges {
                block: Some(pb_blocks(version as u64)),
                changes: vec![TransactionChanges {
                    tx: Some(pb_transactions(5, 1)),
                    contract_changes: vec![],
                    entity_changes: vec![EntityChanges {
                        component_id: "pc_1".to_owned(),
                        attributes: vec![Attribute {
                            name: "attr_2".to_owned(),
                            value: Bytes::from(1_000_000u64)
                                .lpad(32, 0)
                                .to_vec(),
                            change: ChangeType::Update.into(),
                        }],
                    }],
                    component_changes: vec![ProtocolComponent {
                        id: "pc_2".to_owned(),
                        tokens: vec![
                            address_from_str(USDT_ADDRESS),
                            address_from_str(USDC_ADDRESS),
                        ],
                        contracts: vec![],
                        static_att: vec![],
                        change: ChangeType::Deletion.into(),
                        protocol_type: Some(ProtocolType {
                            name: "pt_1".to_string(),
                            financial_type: 0,
                            attribute_schema: vec![],
                            implementation_type: 0,
                        }),
                    }],
                    balance_changes: vec![BalanceChange {
                        token: address_from_str(WETH_ADDRESS),
                        balance: 1000_i32.to_be_bytes().to_vec(),
                        component_id: "pc_1".into(),
                    }],
                }],
            },
            _ => panic!("Requested unknown version of block entity changes"),
        }
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use rstest::rstest;

    use super::*;
    use crate::extractor::models::fixtures::{
        block_entity_changes, block_state_changes, create_transaction,
    };

    #[test]
    fn test_parse_protocol_state_update() {
        let msg = fixtures::pb_state_changes();

        let res = ProtocolComponentStateDelta::try_from_message(msg).unwrap();

        assert_eq!(res, fixtures::protocol_state_delta());
    }

    #[rstest]
    fn test_parse_protocol_component() {
        let msg = fixtures::pb_protocol_component();

        let expected_chain = Chain::Ethereum;
        let expected_protocol_system = "ambient".to_string();
        let expected_attribute_map: HashMap<String, Bytes> = vec![
            ("balance".to_string(), Bytes::from(100u64).lpad(32, 0)),
            ("factory_address".to_string(), Bytes::from(b"0x0fwe0g240g20".to_vec())),
        ]
        .into_iter()
        .collect();

        let protocol_type_id = "WeightedPool".to_string();
        let protocol_types: HashMap<String, ProtocolType> =
            HashMap::from([(protocol_type_id.clone(), ProtocolType::default())]);

        // Call the try_from_message method
        let result = ProtocolComponent::try_from_message((
            msg,
            expected_chain,
            &expected_protocol_system,
            &protocol_types,
            Bytes::from_str("0x0e22048af8040c102d96d14b0988c6195ffda24021de4d856801553aa468bcac")
                .unwrap(),
            Default::default(),
        ));

        // Assert the result
        assert!(result.is_ok());

        // Unwrap the result for further assertions
        let protocol_component = result.unwrap();

        // Assert specific properties of the protocol component
        assert_eq!(
            protocol_component.id,
            "d417ff54652c09bd9f31f216b1a2e5d1e28c1dce1ba840c40d16f2b4d09b5902".to_string()
        );
        assert_eq!(protocol_component.protocol_system, expected_protocol_system);
        assert_eq!(protocol_component.protocol_type_name, protocol_type_id);
        assert_eq!(protocol_component.chain, expected_chain);
        assert_eq!(
            protocol_component.tokens,
            vec![
                Bytes::from_str("6B175474E89094C44Da98b954EedeAC495271d0F").unwrap(),
                Bytes::from_str("6B175474E89094C44Da98b954EedeAC495271d0F").unwrap(),
            ]
        );
        assert_eq!(
            protocol_component.contract_addresses,
            vec![
                Bytes::from_str("31fF2589Ee5275a2038beB855F44b9Be993aA804").unwrap(),
                Bytes::from_str("C02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2").unwrap(),
            ]
        );
        assert_eq!(protocol_component.static_attributes, expected_attribute_map);
    }

    pub fn transaction() -> Transaction {
        create_transaction(
            "0000000000000000000000000000000000000000000000000000000011121314",
            "0000000000000000000000000000000000000000000000000000000031323334",
            2,
        )
    }

    #[rstest]
    fn test_parse_component_balance() {
        let tx = transaction();
        let expected_balance: f64 = 3000.0;
        let msg_balance = expected_balance.to_le_bytes().to_vec();

        let expected_token = Bytes::from_str("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2").unwrap();
        let msg_token = expected_token.0.to_vec();
        let expected_component_id =
            "d417ff54652c09bd9f31f216b1a2e5d1e28c1dce1ba840c40d16f2b4d09b5902";
        let msg_component_id = expected_component_id
            .as_bytes()
            .to_vec();
        let msg = substreams::BalanceChange {
            balance: msg_balance.to_vec(),
            token: msg_token,
            component_id: msg_component_id,
        };
        let from_message = ComponentBalance::try_from_message((msg, &tx)).unwrap();

        assert_eq!(from_message.balance, msg_balance);
        assert_eq!(from_message.modify_tx, tx.hash);
        assert_eq!(from_message.token, expected_token);
        assert_eq!(from_message.component_id, expected_component_id);
    }

    #[test]
    fn test_parse_block_contract_changes() {
        let msg = fixtures::pb_block_contract_changes(0);

        let res = BlockContractChanges::try_from_message((
            msg,
            "test",
            Chain::Ethereum,
            "ambient".to_string(),
            &HashMap::from([("WeightedPool".to_string(), ProtocolType::default())]),
            0,
        ))
        .unwrap();
        assert_eq!(res, block_state_changes());
    }

    #[test]
    fn test_block_entity_changes_parse_msg() {
        let msg = fixtures::pb_block_entity_changes(0);

        let res = BlockEntityChanges::try_from_message((
            msg,
            "test",
            Chain::Ethereum,
            "ambient",
            &HashMap::from([
                ("Pool".to_string(), ProtocolType::default()),
                ("WeightedPool".to_string(), ProtocolType::default()),
            ]),
            420,
        ))
        .unwrap();
        assert_eq!(res, block_entity_changes());
    }
}
