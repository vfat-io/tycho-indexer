//! This module contains Tycho RPC implementation

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum RpcError {
    #[error("Failed to parse JSON: {0}")]
    SerdeJsonError(serde_json::Error),
}

impl From<serde_json::Error> for RpcError {
    fn from(err: serde_json::Error) -> RpcError {
        RpcError::SerdeJsonError(err)
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct StateRequestBody {
    #[serde(rename = "contractIds")]
    contract_ids: Vec<ContractId>,
    version: Version,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct ContractId {
    address: String,
    chain: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Version {
    timestamp: String,
    block: Block,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Block {
    hash: String,
    #[serde(rename = "parentHash")]
    parent_hash: String,
    chain: String,
    number: i64,
}

fn parse_state_request(json_str: &str) -> Result<StateRequestBody, RpcError> {
    let request_body: StateRequestBody = serde_json::from_str(json_str)?;

    Ok(request_body)
}

#[cfg(test)]
mod tests {
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

        let contract0 = "0xb4eccE46b8D4e4abFd03C9B806276A6735C9c092".to_string();
        let block_hash =
            "0x24101f9cb26cd09425b52da10e8c2f56ede94089a8bbe0f31f1cda5f4daa52c4".to_string();
        let parent_block_hash =
            "0x8d75152454e60413efe758cc424bfd339897062d7e658f302765eb7b50971815".to_string();
        let block_number = 213;

        let expected = StateRequestBody {
            contract_ids: vec![ContractId {
                chain: "ethereum".to_string(),
                address: contract0,
            }],
            version: Version {
                timestamp: "2069-01-01T04:20:00".to_string(),
                block: Block {
                    hash: block_hash,
                    parent_hash: parent_block_hash,
                    chain: "ethereum".to_string(),
                    number: block_number,
                },
            },
        };

        assert_eq!(result, expected);
    }
}
