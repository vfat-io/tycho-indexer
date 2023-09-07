//! This module contains Tycho RPC implementation

use std::sync::Arc;

use crate::{
    extractor::evm::{self, Account},
    models::Chain,
    rpc::deserialization_helpers::{chain_from_str, hex_to_bytes},
    storage::{
        postgres::PostgresGateway, BlockIdentifier, BlockOrTimestamp, ContractId,
        ContractStateGateway,
    },
};
use chrono::{NaiveDateTime, Utc};
use diesel_async::AsyncPgConnection;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::Mutex;
use tracing::error;

use actix_web::{
    web::{self, Data},
    App, HttpResponse, HttpServer, Responder,
};

pub mod deserialization_helpers;
struct RequestHandler {
    db_gw: PostgresGateway<evm::Block, evm::Transaction>,
    db_connection: AsyncPgConnection,
}

impl RequestHandler {
    async fn get_state(&mut self, request: &StateRequestBody) -> StateRequestResponse {
        //TODO: handle when no contract is specified with filters
        let at = match &request.version.block {
            Some(b) => BlockOrTimestamp::Block(BlockIdentifier::Hash(b.hash.clone())),
            None => BlockOrTimestamp::Timestamp(request.version.timestamp),
        };

        let mut accounts: Vec<Account> = Vec::new();

        if let Some(contract_ids) = &request.contract_ids {
            for contract_id in contract_ids {
                match self
                    .db_gw
                    .get_contract(contract_id, &Some(&at), &mut self.db_connection)
                    .await
                {
                    Ok(contract_state) => accounts.push(contract_state),
                    Err(e) => {
                        error!("Error while getting contract state {}", e)
                    }
                }
            }
        }

        StateRequestResponse { accounts }
    }
}

#[derive(Serialize)]
struct StateRequestResponse {
    accounts: Vec<Account>,
}

#[derive(Error, Debug)]
pub enum RpcError {
    #[error("Failed to parse JSON: {0}")]
    ParseError(serde_json::Error),
}

impl From<serde_json::Error> for RpcError {
    fn from(err: serde_json::Error) -> RpcError {
        RpcError::ParseError(err)
    }
}

#[derive(Debug, Deserialize, PartialEq)]
struct StateRequestBody {
    #[serde(rename = "contractIds")]
    contract_ids: Option<Vec<ContractId>>,
    #[serde(default = "Version::default")]
    version: Version,
    #[serde(rename = "chain", default = "default_chain")]
    chain: Chain,
    #[serde(rename = "tvlGt", default = "default_gt_filter_value")]
    tvl_gt: i32,
    #[serde(rename = "intertiaMinGt", default = "default_gt_filter_value")]
    intertia_min_gt: i32,
}

fn default_gt_filter_value() -> i32 {
    1
}

fn default_chain() -> Chain {
    Chain::Ethereum
}

async fn handle_post(
    handler: web::Data<Arc<Mutex<RequestHandler>>>,
    contract_state: web::Json<StateRequestBody>,
) -> impl Responder {
    let mut handler = handler.lock().await;
    let response = handler.get_state(&contract_state).await;

    HttpResponse::Ok().json(response)
}

#[derive(Debug, Deserialize, PartialEq)]
struct Version {
    timestamp: NaiveDateTime,
    block: Option<Block>,
}

impl Default for Version {
    fn default() -> Self {
        Version { timestamp: Utc::now().naive_utc(), block: None }
    }
}

#[derive(Debug, Deserialize, PartialEq)]
struct Block {
    #[serde(deserialize_with = "hex_to_bytes")]
    hash: Vec<u8>,
    #[serde(rename = "parentHash", deserialize_with = "hex_to_bytes")]
    parent_hash: Vec<u8>,
    #[serde(deserialize_with = "chain_from_str")]
    chain: Chain,
    number: i64,
}

fn parse_state_request(json_str: &str) -> Result<StateRequestBody, RpcError> {
    let request_body: StateRequestBody = serde_json::from_str(json_str)?;
    Ok(request_body)
}

#[actix_web::main]
async fn run(req_handler: web::Data<Arc<Mutex<RequestHandler>>>) -> std::io::Result<()> {
    HttpServer::new(move || {
        App::new()
            .app_data(req_handler.clone())
            .route("/contract_state", web::post().to(handle_post))
    })
    .bind("127.0.0.1:8000")?
    .run()
    .await
}

#[cfg(test)]
mod tests {
    use crate::storage::postgres::db_fixtures;
    use diesel_async::AsyncConnection;
    use ethers::types::{H160, H256, U256};
    use std::{collections::HashMap, str::FromStr};

    use super::*;

    #[test]
    fn test_parse_state_request() {
        let json_str = r#"
        {
            "contractIds": [
                {
                    "address": "0xb4eccE46b8D4e4abFd03C9B806276A6735C9c092",
                    "chain": "ethereum"
                }
            ],
            "version": {
                "timestamp": "2069-01-01T04:20:00",
                "block": {
                    "hash": "0x24101f9cb26cd09425b52da10e8c2f56ede94089a8bbe0f31f1cda5f4daa52c4",
                    "parentHash": "0x8d75152454e60413efe758cc424bfd339897062d7e658f302765eb7b50971815",
                    "number": 213,
                    "chain": "ethereum"
                }
            }
        }
        "#;

        let result = parse_state_request(json_str).unwrap();

        let contract0 = hex::decode("b4eccE46b8D4e4abFd03C9B806276A6735C9c092").unwrap();
        let block_hash =
            hex::decode("24101f9cb26cd09425b52da10e8c2f56ede94089a8bbe0f31f1cda5f4daa52c4")
                .unwrap();
        let parent_block_hash =
            hex::decode("8d75152454e60413efe758cc424bfd339897062d7e658f302765eb7b50971815")
                .unwrap();
        let block_number = 213;

        let expected_timestamp =
            NaiveDateTime::parse_from_str("2069-01-01T04:20:00", "%Y-%m-%dT%H:%M:%S").unwrap();

        let expected = StateRequestBody {
            contract_ids: Some(vec![ContractId::new(Chain::Ethereum, contract0)]),
            version: Version {
                timestamp: expected_timestamp,
                block: Some(Block {
                    hash: block_hash,
                    parent_hash: parent_block_hash,
                    chain: Chain::Ethereum,
                    number: block_number,
                }),
            },
            chain: Chain::Ethereum,
            tvl_gt: 1,
            intertia_min_gt: 1,
        };

        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_state_request_no_contract_specified() {
        let json_str = r#"
    {
        "version": {
            "timestamp": "2069-01-01T04:20:00",
            "block": {
                "hash": "0x24101f9cb26cd09425b52da10e8c2f56ede94089a8bbe0f31f1cda5f4daa52c4",
                "parentHash": "0x8d75152454e60413efe758cc424bfd339897062d7e658f302765eb7b50971815",
                "number": 213,
                "chain": "ethereum"
            }
        }
    }
    "#;

        let result = parse_state_request(json_str).unwrap();

        let block_hash =
            hex::decode("24101f9cb26cd09425b52da10e8c2f56ede94089a8bbe0f31f1cda5f4daa52c4")
                .unwrap();
        let parent_block_hash =
            hex::decode("8d75152454e60413efe758cc424bfd339897062d7e658f302765eb7b50971815")
                .unwrap();
        let block_number = 213;
        let expected_timestamp =
            NaiveDateTime::parse_from_str("2069-01-01T04:20:00", "%Y-%m-%dT%H:%M:%S").unwrap();

        let expected = StateRequestBody {
            contract_ids: None,
            version: Version {
                timestamp: expected_timestamp,
                block: Some(Block {
                    hash: block_hash,
                    parent_hash: parent_block_hash,
                    chain: Chain::Ethereum,
                    number: block_number,
                }),
            },
            chain: Chain::Ethereum,
            tvl_gt: 1,
            intertia_min_gt: 1,
        };

        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_state_request_no_version_specified() {
        let json_str = r#"
    {
        "contractIds": [
            {
                "address": "0xb4eccE46b8D4e4abFd03C9B806276A6735C9c092",
                "chain": "ethereum"
            }
        ]
    }
    "#;

        let result = parse_state_request(json_str).unwrap();

        let contract0 = hex::decode("b4eccE46b8D4e4abFd03C9B806276A6735C9c092").unwrap();

        let expected = StateRequestBody {
            contract_ids: Some(vec![ContractId::new(Chain::Ethereum, contract0)]),
            version: Version { timestamp: Utc::now().naive_utc(), block: None },
            chain: Chain::Ethereum,
            tvl_gt: 1,
            intertia_min_gt: 1,
        };

        let time_difference = expected
            .version
            .timestamp
            .timestamp_millis()
            - result
                .version
                .timestamp
                .timestamp_millis();

        // Allowing a small time delta (1 second)
        assert!(time_difference <= 1000);
        assert_eq!(result.contract_ids, expected.contract_ids);
        assert_eq!(result.version.block, expected.version.block);
    }

    pub async fn setup_account(conn: &mut AsyncPgConnection) -> String {
        // Adds fixtures: chain, block, transaction, account, account_balance
        let acc_address = "6B175474E89094C44Da98b954EedeAC495271d0F";
        let chain_id = db_fixtures::insert_chain(conn, "ethereum").await;
        let blk = db_fixtures::insert_blocks(conn, chain_id).await;
        db_fixtures::insert_txns(
            conn,
            &[
                (
                    blk[0],
                    1i64,
                    "0xbb7e16d797a9e2fbc537e30f91ed3d27a254dd9578aa4c3af3e5f0d3e8130945",
                ),
                (
                    blk[1],
                    1i64,
                    "0x3108322284d0a89a7accb288d1a94384d499504fe7e04441b0706c7628dee7b7",
                ),
            ],
        )
        .await;

        let addr = hex::encode(H256::random().as_bytes());
        let tx_data = [(blk[1], 1234, addr.as_str())];
        let tid = db_fixtures::insert_txns(conn, &tx_data).await;

        // Insert account and balances
        let acc_id =
            db_fixtures::insert_account(conn, acc_address, "account0", chain_id, None).await;

        db_fixtures::insert_account_balances(conn, tid[0], acc_id).await;
        let contract_code = hex::decode("1234").unwrap();
        db_fixtures::insert_contract_code(conn, acc_id, tid[0], contract_code).await;
        acc_address.to_string()
    }

    #[tokio::test]
    async fn test_get_state() {
        let db_url = std::env::var("DATABASE_URL").unwrap();
        let mut conn = AsyncPgConnection::establish(&db_url)
            .await
            .unwrap();
        conn.begin_test_transaction()
            .await
            .unwrap();
        let acc_address = setup_account(&mut conn).await;

        let db_gtw =
            PostgresGateway::<evm::Block, evm::Transaction>::from_connection(&mut conn).await;
        let mut req_handler = RequestHandler { db_gw: db_gtw, db_connection: conn };

        let code = hex::decode("1234").unwrap();
        let code_hash = H256::from_slice(&ethers::utils::keccak256(&code));
        let expected = Account::new(
            Chain::Ethereum,
            H160::from_str("6B175474E89094C44Da98b954EedeAC495271d0F").unwrap(),
            "account0".to_owned(),
            HashMap::new(),
            U256::from(100),
            code,
            code_hash,
            H256::zero(),
            None,
        );

        let request = StateRequestBody {
            contract_ids: Some(vec![ContractId::new(
                Chain::Ethereum,
                hex::decode(acc_address).unwrap(),
            )]),
            version: Version { timestamp: Utc::now().naive_utc(), block: None },
            chain: Chain::Ethereum,
            tvl_gt: 1,
            intertia_min_gt: 1,
        };

        let state = req_handler.get_state(&request).await;

        assert_eq!(state.accounts[0], expected);
    }
}
