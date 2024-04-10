use std::{str::FromStr, sync::Arc};

use anyhow::Result;
use ethcontract::{H160, U256};
use ethrpc::{http::HttpTransport, Web3, Web3Transport};
use reqwest::Client;
use token_analyzer::{
    trace_call::{TokenOwnerFinding, TraceCallDetector},
    BadTokenDetecting,
};
use url::Url;

struct TokenFinder {}

#[async_trait::async_trait]
impl TokenOwnerFinding for TokenFinder {
    async fn find_owner(&self, _token: H160, _min_balance: U256) -> Result<Option<(H160, U256)>> {
        Ok(Some((
            H160::from_str("0x7d766B06e7164Be4196EE62E6036c9FCFF68107d").unwrap(),
            U256::from_dec_str("11184000000").unwrap(),
        )))
    }
}

#[tokio::main]
async fn main() -> Result<(), ()> {
    let transport = Web3Transport::new(HttpTransport::new(
        Client::new(),
        Url::from_str(
            "https://ethereum-mainnet.core.chainstack.com/71bdd37d35f18d55fed5cc5d138a8fac",
        )
        .unwrap(),
        "transport".to_owned(),
    ));
    let w3 = Web3::new(transport);
    let tf = TokenFinder {};
    let trace_call = TraceCallDetector {
        web3: w3,
        finder: Arc::new(tf),
        settlement_contract: H160::from_str("0xc9f2e6ea1637E499406986ac50ddC92401ce1f58").unwrap(),
    };

    let quality = trace_call
        .detect(H160::from_str("0x45804880De22913dAFE09f4980848ECE6EcbAf78").unwrap())
        .await
        .unwrap();

    println!("{:?}", quality);
    Ok(())
}
