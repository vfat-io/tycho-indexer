use crate::{extractor::evm::ERC20Token, models::Chain};
use ethers::{abi::Abi, contract::Contract, prelude::Middleware, types::H160};
use serde_json::from_str;
use std::{fs, sync::Arc};

pub struct TokenPreProcessor<M: Middleware> {
    pub client: Arc<M>,
}

impl<M: Middleware> TokenPreProcessor<M> {
    pub fn new(client: M) -> Self {
        TokenPreProcessor { client: Arc::new(client) }
    }

    pub async fn get_tokens(
        &self,
        addresses: Vec<H160>,
    ) -> Result<Vec<ERC20Token>, Box<dyn std::error::Error>> {
        let mut tokens_info = Vec::new();

        let abi_str = fs::read_to_string("src/extractor/evm/abi/erc20.json")
            .expect("Unable to read ABI file");
        let abi = from_str::<Abi>(&abi_str).expect("Unable to parse ABI");

        for address in addresses {
            let contract = Contract::new(address, abi.clone(), self.client.clone());

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

            match (symbol, decimals) {
                (Ok(symbol), Ok(decimals)) => tokens_info.push(ERC20Token {
                    address,
                    symbol,
                    decimals: decimals.into(),
                    tax: 0,
                    gas: vec![],
                    chain: Chain::Ethereum,
                }),
                (Err(e), _) | (_, Err(e)) => println!("Error fetching token data: {:?}", e),
            }
        }

        Ok(tokens_info)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ethers::{
        prelude::{MockProvider, Provider},
        types::H160,
    };
    use std::str::FromStr;

    #[tokio::test]
    async fn test_get_tokens() {
        let (provider, mock) = Provider::mocked();
        mock.push::<u8, _>(6);
        mock.push::<String, _>("USDC".to_string());
        mock.push::<u8, _>(18);
        mock.push::<String, _>("WETH".to_string());

        /*mock.push_response(MockResponse::Value(serde_json::json!({
                         "jsonrpc": "2.0",
                         "id": 1,
                         "result": 6})));
        mock.push_response(MockResponse::Value(serde_json::json!({
                         "jsonrpc": "2.0",
                         "id": 1,
                         "result": "USDC"})));
        mock.push_response(MockResponse::Value(serde_json::json!({
                         "jsonrpc": "2.0",
                         "id": 1,
                         "result": 18})));
        mock.push_response(MockResponse::Value(serde_json::json!({
                 "jsonrpc": "2.0",
                 "id": 1,
                 "result": "WETH"})));*/

        let processor = TokenPreProcessor::<Provider<MockProvider>>::new(provider);

        let weth_address: &str = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2";
        let usdc_address: &str = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48";
        let addresses =
            vec![H160::from_str(weth_address).unwrap(), H160::from_str(usdc_address).unwrap()];

        let results = processor
            .get_tokens(addresses)
            .await
            .unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].symbol, "WETH");
        assert_eq!(results[0].decimals, 18);
        assert_eq!(results[1].symbol, "USDC");
        assert_eq!(results[1].decimals, 6);
    }
}
