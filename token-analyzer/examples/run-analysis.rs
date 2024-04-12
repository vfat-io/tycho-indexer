/// To run: cargo run --example run-analysis
use std::{collections::HashMap, str::FromStr, sync::Arc};

use anyhow::Result;
use ethers::types::{H160, U256};
use ethrpc::{http::HttpTransport, Web3, Web3Transport};
use reqwest::Client;
use token_analyzer::{trace_call::TraceCallDetector, BadTokenDetecting, TokenFinder};
use url::Url;
use web3::types::BlockNumber;

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
    let tf = TokenFinder::new(HashMap::from([(
        H160::from_str("0x3A9FfF453d50D4Ac52A6890647b823379ba36B9E").unwrap(),
        (
            H160::from_str("0x260E069deAd76baAC587B5141bB606Ef8b9Bab6c").unwrap(),
            U256::from_dec_str("13042252617814040589").unwrap(),
        ),
    )]));

    let trace_call = TraceCallDetector {
        web3: w3,
        finder: Arc::new(tf),
        settlement_contract: H160::from_str("0xc9f2e6ea1637E499406986ac50ddC92401ce1f58").unwrap(),
    };

    let quality = trace_call
        .detect(
            H160::from_str("0x3A9FfF453d50D4Ac52A6890647b823379ba36B9E").unwrap(),
            BlockNumber::Latest,
        )
        .await
        .unwrap();

    println!("{:?}", quality);
    Ok(())
}
