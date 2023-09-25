use std::collections::HashMap;

use crate::{
    extractor::{
        evm,
        evm::utils::{parse_u256_slot_entry, TryDecode},
    },
    models::Chain,
    storage::{
        ContractDelta, ContractId, ContractStore, StorableBlock, StorableContract,
        StorableTransaction, StorageError,
    },
};

use chrono::NaiveDateTime;

pub mod pg {
    use ethers::types::{H160, H256, U256};

    use crate::storage::{postgres::orm, Address, Balance, BlockHash, ChangeType, Code, TxHash};

    use super::*;

    impl From<evm::Account> for evm::AccountUpdate {
        fn from(value: evm::Account) -> Self {
            evm::AccountUpdate::new(
                value.address,
                value.chain,
                value.slots,
                Some(value.balance),
                Some(value.code),
                ChangeType::Creation,
            )
        }
    }

    impl StorableBlock<orm::Block, orm::NewBlock, i64> for evm::Block {
        fn from_storage(val: orm::Block, chain: Chain) -> Result<Self, StorageError> {
            Ok(evm::Block {
                number: val.number as u64,
                hash: H256::try_decode(&val.hash, "block hash")
                    .map_err(StorageError::DecodeError)?,
                parent_hash: H256::try_decode(&val.parent_hash, "parent hash")
                    .map_err(|err| StorageError::DecodeError(err.to_string()))?,
                chain,
                ts: val.ts,
            })
        }

        fn to_storage(&self, chain_id: i64) -> orm::NewBlock {
            orm::NewBlock {
                hash: self.hash.into(),
                parent_hash: self.parent_hash.into(),
                chain_id,
                main: false,
                number: self.number as i64,
                ts: self.ts,
            }
        }

        fn chain(&self) -> &Chain {
            &self.chain
        }
    }

    impl StorableTransaction<orm::Transaction, orm::NewTransaction, i64> for evm::Transaction {
        fn from_storage(
            val: orm::Transaction,
            block_hash: &BlockHash,
        ) -> Result<Self, StorageError> {
            let to = if !val.to.is_empty() {
                Some(H160::try_decode(&val.to, "tx receiver").map_err(StorageError::DecodeError)?)
            } else {
                None
            };
            Ok(Self::new(
                H256::try_decode(&val.hash, "tx hash").map_err(StorageError::DecodeError)?,
                H256::try_decode(block_hash, "tx block hash").map_err(StorageError::DecodeError)?,
                H160::try_decode(&val.from, "tx sender").map_err(StorageError::DecodeError)?,
                to,
                val.index as u64,
            ))
        }

        fn to_storage(&self, block_id: i64) -> orm::NewTransaction {
            let to: Address = self
                .to
                .map(|v| v.into())
                .unwrap_or_default();
            orm::NewTransaction {
                hash: self.hash.into(),
                block_id,
                from: self.from.into(),
                to,
                index: self.index as i64,
            }
        }

        fn block_hash(&self) -> BlockHash {
            self.block_hash.into()
        }

        fn hash(&self) -> BlockHash {
            self.hash.into()
        }
    }

    impl StorableContract<orm::Contract, orm::NewContract, i64> for evm::Account {
        fn from_storage(
            val: orm::Contract,
            chain: Chain,
            balance_modify_tx: &TxHash,
            code_modify_tx: &TxHash,
            creation_tx: Option<&TxHash>,
        ) -> Result<Self, StorageError> {
            Ok(evm::Account::new(
                chain,
                H160::try_decode(&val.account.address, "address")
                    .map_err(StorageError::DecodeError)?,
                val.account.title.clone(),
                HashMap::new(),
                U256::try_decode(&val.balance.balance, "balance")
                    .map_err(StorageError::DecodeError)?,
                val.code.code,
                H256::try_decode(&val.code.hash, "code hash").map_err(StorageError::DecodeError)?,
                H256::try_decode(balance_modify_tx, "tx hash")
                    .map_err(StorageError::DecodeError)?,
                H256::try_decode(code_modify_tx, "tx hash").map_err(StorageError::DecodeError)?,
                match creation_tx {
                    Some(v) => H256::try_decode(v, "tx hash")
                        .map(Some)
                        .map_err(StorageError::DecodeError)?,
                    _ => None,
                },
            ))
        }

        fn to_storage(
            &self,
            chain_id: i64,
            creation_ts: NaiveDateTime,
            tx_id: Option<i64>,
        ) -> orm::NewContract {
            orm::NewContract {
                title: self.title.clone(),
                address: self.address.into(),
                chain_id,
                creation_tx: tx_id,
                created_at: Some(creation_ts),
                deleted_at: None,
                balance: self.balance.into(),
                code: self.code.clone(),
                code_hash: self.code_hash.into(),
            }
        }

        fn chain(&self) -> &Chain {
            &self.chain
        }

        fn creation_tx(&self) -> Option<TxHash> {
            self.creation_tx
                .map(|v| v.into())
        }

        fn address(&self) -> Address {
            self.address.into()
        }

        fn store(&self) -> ContractStore {
            self.slots
                .iter()
                .map(|(s, v)| (s.clone().into(), Some(v.clone().into())))
                .collect()
        }

        fn set_store(&mut self, store: &ContractStore) -> Result<(), StorageError> {
            self.slots = store
                .iter()
                .map(|(rk, rv)| {
                    parse_u256_slot_entry(rk, rv.as_ref()).map_err(StorageError::DecodeError)
                })
                .collect::<Result<HashMap<_, _>, _>>()?;
            Ok(())
        }
    }

    impl ContractDelta for evm::AccountUpdate {
        fn contract_id(&self) -> ContractId {
            ContractId::new(self.chain, self.address.into())
        }

        fn dirty_balance(&self) -> Option<Balance> {
            self.balance.map(|b| b.into())
        }

        fn dirty_code(&self) -> Option<&Code> {
            self.code.as_ref()
        }

        fn dirty_slots(&self) -> ContractStore {
            self.slots
                .iter()
                .map(|(s, v)| (s.clone().into(), Some(v.clone().into())))
                .collect()

            }

        fn from_storage(
            chain: &Chain,
            address: &Address,
            slots: Option<&ContractStore>,
            balance: Option<&Balance>,
            code: Option<&Code>,
            change: ChangeType,
        ) -> Result<Self, StorageError> {
            let slots = slots
                .map(|s| {
                    s.iter()
                        .map(|(s, v)| {
                            parse_u256_slot_entry(s, v.as_ref()).map_err(StorageError::DecodeError)
                        })
                        .collect::<Result<HashMap<U256, U256>, StorageError>>()
                })
                .unwrap_or_else(|| Ok(HashMap::new()))?;

            let update = evm::AccountUpdate::new(
                H160::try_decode(address, "address").map_err(StorageError::DecodeError)?,
                chain.clone(),
                slots,
                match balance {
                    // match expr is required so the error can be raised
                    Some(v) => U256::try_decode(v, "balance")
                        .map(Some)
                        .map_err(|err| StorageError::DecodeError(err.to_string()))?,
                    _ => None,
                },
                code.cloned(),
                change,
            );
            Ok(update)
        }
    }
}
