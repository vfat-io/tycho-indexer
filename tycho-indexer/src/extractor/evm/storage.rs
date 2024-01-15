#![allow(unused_variables)]

use std::collections::HashMap;

use chrono::NaiveDateTime;

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

pub mod pg {
    use crate::extractor::evm::utils::pad_and_parse_h160;
    use ethers::types::{H160, H256, U256};
    use serde_json::Value;

    use crate::{
        hex_bytes::Bytes,
        storage::{
            postgres::{
                orm,
                orm::{NewToken, Token},
            },
            Address, Balance, BlockHash, ChangeType, Code, StorableProtocolState, StorableToken,
            TxHash,
        },
    };

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
            self.creation_tx.map(|v| v.into())
        }

        fn address(&self) -> Address {
            self.address.into()
        }

        fn store(&self) -> ContractStore {
            self.slots
                .iter()
                .map(|(s, v)| ((*s).into(), Some((*v).into())))
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
                *chain,
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
                .map(|(s, v)| ((*s).into(), Some((*v).into())))
                .collect()
        }
    }

    impl StorableToken<orm::Token, orm::NewToken, i64> for evm::ERC20Token {
        fn from_storage(val: Token, contract: ContractId) -> Result<Self, StorageError> {
            let address =
                pad_and_parse_h160(contract.address()).map_err(StorageError::DecodeError)?;
            Ok(evm::ERC20Token::new(
                address,
                val.symbol,
                val.decimals as u32,
                val.tax as u64,
                val.gas
                    .into_iter()
                    .map(|item| item.map(|i| i as u64))
                    .collect(),
                contract.chain,
            ))
        }

        fn to_storage(&self, contract_id: i64) -> orm::NewToken {
            NewToken {
                account_id: contract_id,
                symbol: self.symbol.clone(),
                decimals: self.decimals as i32,
                tax: self.tax as i64,
                gas: self
                    .gas
                    .clone()
                    .into_iter()
                    .map(|item| item.map(|i| i as i64))
                    .collect(),
            }
        }
    }

    impl StorableProtocolState<orm::ProtocolState, orm::NewProtocolState, i64> for evm::ProtocolState {
        fn from_storage(
            val: orm::ProtocolState,
            component_id: String,
            tx_hash: &TxHash,
        ) -> Result<Self, StorageError> {
            let mut attr: HashMap<String, Bytes> = HashMap::new();
            if let Some(Value::Object(state)) = &val.state {
                for (k, v) in state.iter() {
                    if let Value::String(s) = v {
                        attr.insert(k.clone(), Bytes::from(s.as_str()));
                    }
                }
            }
            Ok(evm::ProtocolState::new(
                component_id,
                attr,
                H256::try_decode(tx_hash, "tx hash").map_err(StorageError::DecodeError)?,
            ))
        }

        fn to_storage(
            &self,
            protocol_component_id: i64,
            tx_id: i64,
            block_ts: NaiveDateTime,
        ) -> orm::NewProtocolState {
            orm::NewProtocolState {
                protocol_component_id,
                state: self.convert_attributes_to_json(),
                modify_tx: tx_id,
                tvl: None,
                inertias: None,
                valid_from: block_ts,
                valid_to: None,
            }
        }
    }

    impl evm::ProtocolState {
        fn convert_attributes_to_json(&self) -> Option<serde_json::Value> {
            // Convert Bytes to String and then to serde_json Value
            let serialized_map: HashMap<String, serde_json::Value> = self
                .updated_attributes
                .iter()
                .map(|(k, v)| {
                    let s = hex::encode(v);
                    (k.clone(), serde_json::Value::String(s))
                })
                .collect();

            // Convert HashMap<String, serde_json::Value> to serde_json::Value struct
            match serde_json::to_value(serialized_map) {
                Ok(value) => Some(value),
                Err(_) => None,
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        extractor::evm::{utils::pad_and_parse_h160, ERC20Token},
        storage::{postgres::orm::Token, Address, StorableToken},
    };

    #[test]
    fn test_storable_token_from_storage() {
        let token_address: Address = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2".into();
        let orm_token = Token {
            id: 1,
            account_id: 1,
            symbol: String::from("WETH"),
            decimals: 18,
            tax: 0,
            gas: vec![Some(64), None],
            inserted_ts: Default::default(),
            modified_ts: Default::default(),
        };
        let contract_id = ContractId::new(Chain::Ethereum, token_address.clone());
        let result = ERC20Token::from_storage(orm_token, contract_id);
        assert!(result.is_ok());

        let token = result.unwrap();
        assert_eq!(token.address, pad_and_parse_h160(&token_address).unwrap());
        assert_eq!(token.symbol, String::from("WETH"));
        assert_eq!(token.decimals, 18);
        assert_eq!(token.gas, vec![Some(64), None]);
    }
    #[test]
    fn test_storable_token_to_storage() {
        let token_address = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2".into();
        let erc_token = ERC20Token {
            address: pad_and_parse_h160(&token_address).unwrap(),
            symbol: "WETH".into(),
            decimals: 18,
            tax: 0,
            gas: vec![Some(64), None],
            chain: Chain::Ethereum,
        };

        let new_token = erc_token.to_storage(22);
        assert_eq!(new_token.account_id, 22);
        assert_eq!(new_token.symbol, erc_token.symbol);
        assert_eq!(new_token.decimals, erc_token.decimals as i32);
        assert_eq!(new_token.gas, vec![Some(64), None]);
    }
}
