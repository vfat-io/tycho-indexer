//! This module contains Tycho RPC implementation

use crate::models::Chain;
use crate::rpc::deserialization_helpers::{chain_from_str, hex_to_bytes};
use chrono::Utc;
use serde::Deserialize;
use thiserror::Error;

pub mod deserialization_helpers;

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
}

#[derive(Debug, Deserialize, PartialEq)]
struct Version {
    timestamp: String,
    block: Option<Block>,
}

impl Default for Version {
    fn default() -> Self {
        Version {
            timestamp: Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string(),
            block: None,
        }
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

        let contract0 = hex::decode("b4eccE46b8D4e4abFd03C9B806276A6735C9c092").unwrap();
        let block_hash =
            hex::decode("24101f9cb26cd09425b52da10e8c2f56ede94089a8bbe0f31f1cda5f4daa52c4")
                .unwrap();
        let parent_block_hash =
            hex::decode("8d75152454e60413efe758cc424bfd339897062d7e658f302765eb7b50971815")
                .unwrap();
        let block_number = 213;

        let expected = StateRequestBody {
            contract_ids: Some(vec![ContractId::new(Chain::Ethereum, contract0)]),
            version: Version {
                timestamp: "2069-01-01T04:20:00".to_string(),
                block: Some(Block {
                    hash: block_hash,
                    parent_hash: parent_block_hash,
                    chain: Chain::Ethereum,
                    number: block_number,
                }),
            },
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

        let expected = StateRequestBody {
            contract_ids: None,
            version: Version {
                timestamp: "2069-01-01T04:20:00".to_string(),
                block: Some(Block {
                    hash: block_hash,
                    parent_hash: parent_block_hash,
                    chain: Chain::Ethereum,
                    number: block_number,
                }),
                },
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
        let current_timestamp = Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string();

        let contract0 = hex::decode("b4eccE46b8D4e4abFd03C9B806276A6735C9c092").unwrap();

        let expected = StateRequestBody {
            contract_ids: Some(vec![ContractId::new(Chain::Ethereum, contract0)]),
            version: Version {
                timestamp: current_timestamp,
                block: None,
            },
        };

        assert_eq!(result, expected);
    }
}
