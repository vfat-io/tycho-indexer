use crate::extractor::evm::{AccountUpdate, Block};
use async_trait::async_trait;
use ethers::{
    middleware::Middleware,
    prelude::{BlockId, Http, Provider, H160, H256, U256},
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, error::Error};
use tracing::trace;
use tycho_core::{
    models::{Chain, ChangeType},
    Bytes,
};

pub struct ContractExtractor {
    addresses_per_block: HashMap<u64, Vec<H160>>,
    provider: Provider<Http>,
    chain: Chain,
}

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait EVMContractExtractor {
    async fn new(
        node_url: &str,
        chain: Chain,
        address_block_pairs: Vec<(H160, u64)>,
    ) -> Result<Self, Box<dyn std::error::Error>>
    where
        Self: Sized;
    async fn register_contracts(
        &mut self,
        address_block_pairs: Vec<(H160, u64)>,
    ) -> Result<(), Box<dyn std::error::Error>>;
    async fn process(
        &self,
        block: Block,
    ) -> Result<HashMap<H160, AccountUpdate>, Box<dyn std::error::Error>>;
}

#[async_trait]
impl EVMContractExtractor for ContractExtractor {
    async fn new(
        node_url: &str,
        chain: Chain,
        address_block_pairs: Vec<(H160, u64)>,
    ) -> Result<Self, Box<dyn std::error::Error>>
    where
        Self: Sized,
    {
        let provider = Provider::<Http>::try_from(node_url);
        return match provider {
            Ok(p) => {
                let addresses_per_block: HashMap<u64, Vec<H160>> = address_block_pairs
                    .into_iter()
                    .fold(HashMap::new(), |mut acc, (addr, block)| {
                        acc.entry(block).or_default().push(addr);
                        acc
                    });
                Ok(Self { addresses_per_block, provider: p, chain })
            }
            Err(e) => Err(Box::new(e)),
        };
    }

    async fn register_contracts(
        &mut self,
        address_block_pairs: Vec<(H160, u64)>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        for (address, block) in address_block_pairs {
            self.addresses_per_block
                .entry(block)
                .or_default()
                .push(address);
        }
        Ok(())
    }

    async fn process(&self, block: Block) -> Result<HashMap<H160, AccountUpdate>, Box<dyn Error>> {
        let mut updates = HashMap::new();

        match self
            .addresses_per_block
            .get(&block.number)
        {
            Some(addresses) => {
                for address in addresses {
                    trace!(contract=?address, block_number=?block.number, block_hash=?block.hash, "Extracting contract code and storage" );
                    let block_id = Some(BlockId::from(block.number));
                    println!("Block Id: {:?}", block_id);

                    let balance = Some(
                        self.provider
                            .get_balance(*address, block_id)
                            .await?,
                    );
                    println!("Balance: {:?}", balance);

                    let code = self
                        .provider
                        .get_code(*address, block_id)
                        .await?;

                    let code: Option<Bytes> = Some(Bytes::from(code.to_vec()));

                    println!("Code: {:?}", code);
                    let slots = self
                        .get_storage_range(*address, block.hash)
                        .await?;

                    updates.insert(
                        *address,
                        AccountUpdate {
                            address: *address,
                            chain: self.chain,
                            slots,
                            balance,
                            code,
                            change: ChangeType::Creation,
                        },
                    );
                }
                return Ok(updates);
            }
            None => Ok(HashMap::new()),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct StorageEntry {
    key: H256,
    value: H256,
}

#[derive(Serialize, Deserialize, Debug)]
struct StorageRange {
    storage: HashMap<H256, StorageEntry>,
    next_key: Option<H256>,
}

impl ContractExtractor {
    async fn get_storage_range(
        &self,
        address: H160,
        block: H256,
    ) -> Result<HashMap<U256, U256>, Box<dyn std::error::Error>> {
        let mut all_slots = HashMap::new();
        let mut start_key = H256::zero();
        let block = format!("0x{:x}", block);
        loop {
            let params = serde_json::json!([
                block, 0, // transaction index, 0 for the state at the end of the block
                address, start_key, 1024 // limit
            ]);

            trace!("Requesting storage range for {:?}, block: {:?}", address, block);
            let result: StorageRange = self
                .provider
                .request("debug_storageRangeAt", params)
                .await?;

            for (_, entry) in result.storage {
                all_slots
                    .insert(U256::from(entry.key.as_bytes()), U256::from(entry.value.as_bytes()));
            }

            if let Some(next_key) = result.next_key {
                start_key = next_key;
            } else {
                break;
            }
        }

        Ok(all_slots)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[tokio::test]
    #[ignore]
    // This test requires a real RPC URL
    async fn test_contract_extractor() -> Result<(), Box<dyn std::error::Error>> {
        let block_hash =
            H256::from_str("0x7f70ac678819e24c4947a3a95fdab886083892a18ba1a962ebaac31455584042")
                .expect("valid block hash");
        let block_number: u64 = 20378314;

        let contracts: Vec<(H160, u64)> = vec![(
            H160::from_str("0xba12222222228d8ba445958a75a0704d566bf2c8").expect("valid address"),
            block_number,
        )];
        let node = std::env::var("RPC_URL").expect("RPC URL must be set for testing");
        println!("Using node: {}", node);

        let block = Block {
            number: block_number,
            hash: block_hash,
            parent_hash: Default::default(),
            chain: Chain::Ethereum,
            ts: Default::default(),
        };
        let extractor = ContractExtractor::new(&node, Chain::Ethereum, contracts).await?;
        let updates = extractor.process(block).await?;

        assert_eq!(updates.len(), 1);
        let update = updates
            .get(
                &H160::from_str("0xba12222222228d8ba445958a75a0704d566bf2c8")
                    .expect("valid address"),
            )
            .expect("update exists");

        assert!(!update.slots.is_empty());

        Ok(())
    }
}
