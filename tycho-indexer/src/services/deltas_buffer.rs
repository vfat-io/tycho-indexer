use crate::extractor::{
    evm::{BlockAccountChanges, BlockEntityChangesResult},
    revert_buffer::RevertBuffer,
    runner::MessageSender,
};
use chrono::NaiveDateTime;
use ethers::prelude::StreamExt;
use futures03::stream;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio_stream::wrappers::ReceiverStream;
use tycho_core::models::{
    blockchain::{NativeBlockAggregate, VmBlockAggregate},
    contract::Contract,
    protocol::ProtocolComponentState,
    NormalisedMessage,
};

type NativeRevertBuffer = Arc<Mutex<RevertBuffer<NativeBlockAggregate>>>;
type VmRevertBuffer = Arc<Mutex<RevertBuffer<VmBlockAggregate>>>;

pub struct PendingDeltas {
    native: HashMap<String, NativeRevertBuffer>,
    vm: HashMap<String, VmRevertBuffer>,
}

impl PendingDeltas {
    pub fn insert(&self, message: Arc<dyn NormalisedMessage>) {
        let maybe_native: Option<NativeBlockAggregate> = message
            .as_any()
            .downcast_ref::<BlockEntityChangesResult>()
            .map(|msg| msg.into());
        let maybe_vm: Option<VmBlockAggregate> = message
            .as_any()
            .downcast_ref::<BlockAccountChanges>()
            .map(|msg| msg.into());
        match (maybe_native, maybe_vm) {
            (Some(msg), None) => {
                if let Some(buffer) = self.native.get(&msg.extractor) {
                    let mut guard = buffer
                        .lock()
                        .expect("failed to acquire lock!");
                    if msg.revert {
                        guard
                            .purge(msg.block.hash.clone())
                            .expect("failed to purge");
                    } else {
                        guard
                            .insert_block(msg.clone())
                            .expect("failed to insert");
                        // TODO: remove corresponding final block
                    }
                }
            }
            (None, Some(msg)) => {
                if let Some(buffer) = self.vm.get(&msg.extractor) {
                    let mut guard = buffer
                        .lock()
                        .expect("failed to acquire lock");
                    if msg.revert {
                        guard
                            .purge(msg.block.hash.clone())
                            .expect("failed to purge");
                    } else {
                        guard
                            .insert_block(msg.clone())
                            .expect("failed to insert");
                        // TODO: remove corresponding final block
                    }
                }
            }
            _ => panic!("Invalid message type."),
        }
    }

    pub fn update_native_states(
        &self,
        db_states: &mut [ProtocolComponentState],
        version: NaiveDateTime,
    ) {
        for state in db_states {
            self.update_native_state(state, version);
        }
    }

    fn update_native_state(
        &self,
        db_state: &mut ProtocolComponentState,
        version: NaiveDateTime,
    ) -> bool {
        let mut change_found = false;
        for buffer in self.native.values() {
            let guard = buffer
                .lock()
                .expect("failed to acquire lock");
            for entry in guard
                // TODO: get ideally block height or allow filtering by timestamp
                .get_block_range(None, None)
                .unwrap()
            {
                if let Some(delta) = entry.deltas.get(&db_state.component_id) {
                    db_state
                        .apply_state_delta(delta)
                        .expect("failed to apply state delta");
                    change_found = true;
                }

                if let Some(delta) = entry
                    .component_balances
                    .get(&db_state.component_id)
                {
                    db_state
                        .apply_balance_delta(delta)
                        .expect("failed to apply balance deltas");
                    change_found = true
                }
            }

            if change_found {
                // if we found some changes no need to check other extractor's buffer
                break;
            }
        }
        change_found
    }

    fn update_vm_state(&self, db_state: &mut Contract, version: NaiveDateTime) -> bool {
        let mut change_found = false;
        for buffer in self.vm.values() {
            let guard = buffer
                .lock()
                .expect("failed to acquire lock");
            for entry in guard
                // TODO: get ideally block height or allow filtering by timestamp
                .get_block_range(None, None)
                .unwrap()
            {
                if let Some(delta) = entry.deltas.get(&db_state.address) {
                    db_state.apply_contract_delta(delta);
                    change_found = true;
                }

                for balances in entry.component_balances.values() {
                    if let Some(update) = balances.get(&db_state.address) {
                        db_state.apply_balance_delta(update);
                        change_found = true;
                    }
                }
            }

            if change_found {
                // if we found some changes no need to check other extractor's buffer
                break;
            }
        }

        change_found
    }

    pub async fn get_new_components(&self) {
        let mut new_components = Vec::new();
        for buffer in self.native.values() {
            let guard = buffer
                .lock()
                .expect("failed to acquire lock");
            for entry in guard
                .get_block_range(None, None)
                .unwrap()
            {
                new_components.extend(entry.new_components.clone());
            }
        }

        for buffer in self.vm.values() {
            let guard = buffer
                .lock()
                .expect("failed to acquire lock");
            for entry in guard
                .get_block_range(None, None)
                .unwrap()
            {
                new_components.extend(entry.new_components.clone());
            }
        }
    }

    pub async fn run(
        self,
        extractors: impl IntoIterator<Item = Arc<dyn MessageSender>>,
    ) -> anyhow::Result<()> {
        let mut rxs = Vec::new();
        for extractor in extractors.into_iter() {
            let res = extractor.subscribe().await?;
            rxs.push(res);
        }

        let all_messages = stream::select_all(rxs.into_iter().map(ReceiverStream::new));

        // What happens if an extractor restarts - it might just end here and be dropped?
        // Ideally the Runner should never restart.
        all_messages
            .for_each(|message| async { self.insert(message) })
            .await;

        Ok(())
    }
}
