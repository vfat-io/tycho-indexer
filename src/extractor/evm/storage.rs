use std::collections::HashMap;

use crate::{
    extractor::{
        evm,
        evm::utils::{parse_id_h160, parse_u256_slot_entry, u256_to_bytes},
    },
    models::Chain,
    storage::{
        AddressRef, BalanceRef, CodeRef, ContractDelta, ContractId, ContractStore,
        StorableContract, StorageError, TxHashRef,
    },
};

use chrono::NaiveDateTime;
use ethers::prelude::*;

pub mod pg {
    use crate::storage::postgres::orm;

    use super::*;

    impl StorableContract<orm::Contract, orm::NewContract, i64> for evm::Account {
        fn from_storage(
            val: orm::Contract,
            chain: Chain,
            balance_modify_tx: TxHashRef,
            code_modify_tx: TxHashRef,
            creation_tx: Option<TxHashRef>,
        ) -> Self {
            evm::Account::new(
                chain,
                // TODO: from_slice may panic
                H160::from_slice(&val.account.address),
                val.account.title.clone(),
                HashMap::new(),
                U256::from_big_endian(&val.balance.balance),
                val.code.code,
                H256::from_slice(&val.code.hash),
                H256::from_slice(balance_modify_tx),
                H256::from_slice(code_modify_tx),
                creation_tx.map(H256::from_slice),
            )
        }

        fn to_storage(
            &self,
            chain_id: i64,
            creation_ts: NaiveDateTime,
            tx_id: Option<i64>,
        ) -> orm::NewContract {
            orm::NewContract {
                title: self.title.clone(),
                address: self.address.as_bytes().to_vec(),
                chain_id,
                creation_tx: tx_id,
                created_at: Some(creation_ts),
                deleted_at: None,
                balance: u256_to_bytes(&self.balance),
                code: self.code.clone(),
                code_hash: self.code_hash.as_bytes().to_vec(),
            }
        }

        fn chain(&self) -> Chain {
            self.chain
        }

        fn creation_tx(&self) -> Option<TxHashRef> {
            self.creation_tx
                .as_ref()
                .map(|h| h.as_bytes())
        }

        fn address(&self) -> AddressRef {
            self.address.as_bytes()
        }

        fn store(&self) -> ContractStore {
            self.slots
                .iter()
                .map(|(s, v)| (u256_to_bytes(s), Some(u256_to_bytes(v))))
                .collect()
        }

        fn set_store(&mut self, store: &ContractStore) -> Result<(), StorageError> {
            self.slots = store
                .iter()
                .map(|(rk, rv)| {
                    parse_u256_slot_entry(rk, rv.as_deref())
                        .map_err(|err| StorageError::DecodeError(err))
                })
                .collect::<Result<HashMap<_, _>, _>>()?;
            Ok(())
        }
    }

    impl ContractDelta for evm::AccountUpdate {
        fn contract_id(&self) -> ContractId {
            ContractId::new(self.chain, self.address.as_bytes().to_vec())
        }

        fn dirty_balance(&self) -> Option<Vec<u8>> {
            self.balance.map(|b| u256_to_bytes(&b))
        }

        fn dirty_code(&self) -> Option<&[u8]> {
            self.code.as_deref()
        }

        fn dirty_slots(&self) -> ContractStore {
            self.slots
                .iter()
                .map(|(s, v)| (u256_to_bytes(s), Some(u256_to_bytes(v))))
                .collect()
        }

        fn from_storage(
            chain: Chain,
            address: AddressRef,
            slots: Option<&ContractStore>,
            balance: Option<BalanceRef>,
            code: Option<CodeRef>,
        ) -> Result<Self, StorageError> {
            let slots = slots
                .map(|s| {
                    s.iter()
                        .map(|(s, v)| {
                            parse_u256_slot_entry(s, v.as_deref())
                                .map_err(|err| StorageError::DecodeError(err))
                        })
                        .collect::<Result<HashMap<U256, U256>, StorageError>>()
                })
                .unwrap_or_else(|| Ok(HashMap::new()))?;

            let update = evm::AccountUpdate::new(
                parse_id_h160(address).map_err(|err| StorageError::DecodeError(err))?,
                chain,
                slots,
                balance.map(U256::from_big_endian),
                code.map(|v| v.to_vec()),
            );
            Ok(update)
        }
    }
}
