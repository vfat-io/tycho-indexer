use crate::{extractor::evm::ERC20Token, models::Chain};
use async_trait::async_trait;
use ethers::{abi::Abi, contract::Contract, prelude::Provider, providers::Http, types::H160};
use serde_json::from_str;
use std::sync::Arc;
use tracing::instrument;

#[derive(Debug, Clone)]
pub struct TokenPreProcessor {
    client: Arc<Provider<Http>>,
    erc20_abi: Abi,
}
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait TokenPreProcessorTrait: Send + Sync {
    async fn get_tokens(&self, addresses: Vec<H160>) -> Vec<ERC20Token>;
}

const ABI_STR: &str = include_str!("./abi/erc20.json");

impl TokenPreProcessor {
    pub fn new(client: Provider<Http>) -> Self {
        let abi = from_str::<Abi>(ABI_STR).expect("Unable to parse ABI");
        TokenPreProcessor { client: Arc::new(client), erc20_abi: abi }
    }
}

#[async_trait]
impl TokenPreProcessorTrait for TokenPreProcessor {
    #[instrument]
    async fn get_tokens(&self, addresses: Vec<H160>) -> Vec<ERC20Token> {
        let mut tokens_info = Vec::new();

        for address in addresses {
            let contract = Contract::new(address, self.erc20_abi.clone(), self.client.clone());

            let symbol = contract
                .method("symbol", ())
                .expect("Error preparing request")
                .call()
                .await;

            let decimals: Result<u8, _> = contract
                .method("decimals", ())
                .expect("Error preparing request")
                .call()
                .await;

            let (symbol, decimals, quality) = match (symbol, decimals) {
                (Ok(symbol), Ok(decimals)) => (symbol, decimals, 100),
                (Ok(symbol), Err(_)) => (symbol, 18, 0),
                (Err(_), Ok(decimals)) => (address.to_string(), decimals, 0),
                (Err(_), Err(_)) => (address.to_string(), 18, 0),
            };
            tokens_info.push(ERC20Token {
                address,
                symbol: symbol.replace('\0', ""),
                decimals: decimals.into(),
                tax: 0,
                gas: vec![],
                chain: Chain::Ethereum,
                quality,
            });
        }

        tokens_info
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ethers::{
        providers::{Http, Provider},
        types::H160,
    };
    use std::{env, str::FromStr};

    #[tokio::test]
    #[ignore]
    // This test requires a real RPC URL
    async fn test_get_tokens() {
        let rpc_url = env::var("ETH_RPC_URL").expect("ETH_RPC_URL is not set");
        let client: Provider<Http> =
            Provider::<Http>::try_from(rpc_url).expect("Error creating HTTP provider");
        let processor = TokenPreProcessor::new(client);

        let weth_address: &str = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2";
        let usdc_address: &str = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48";
        let fake_address: &str = "0xA0b86991c7456b36c1d19D4a2e9Eb0cE3606eB48";
        let addresses = vec![
            H160::from_str(weth_address).unwrap(),
            H160::from_str(usdc_address).unwrap(),
            H160::from_str(fake_address).unwrap(),
        ];

        let results = processor.get_tokens(addresses).await;
        assert_eq!(results.len(), 3);
        let relevant_attrs: Vec<(String, u32, u32)> = results
            .iter()
            .map(|t| (t.symbol.clone(), t.decimals, t.quality))
            .collect();
        assert_eq!(
            relevant_attrs,
            vec![
                ("WETH".to_string(), 18, 100),
                ("USDC".to_string(), 6, 100),
                ("0xa0b8…eb48".to_string(), 18, 0)
            ]
        );
    }
}
