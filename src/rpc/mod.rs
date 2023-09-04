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

#[derive(Debug, Serialize, Deserialize)]
struct StateRequestBody {
    contract_ids: Vec<ContractId>,
    version: Version,
}

#[derive(Debug, Serialize, Deserialize)]
struct ContractId {
    address: String,
    chain: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Version {
    timestamp: String,
    block: Block,
}

#[derive(Debug, Serialize, Deserialize)]
struct Block {
    hash: String,
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
            "contract_ids": [
                {
                    "address": "0xb4eccE46b8D4e4abFd03C9B806276A6735C9c092",
                    "chain": "ethereum"
                }
            ],
            "version": {
                "timestamp": "2069-01-01T04:20:00",
                "block": {
                    "parentHash": "0x8d75152454e60413efe758cc424bfd339897062d7e658f302765eb7b50971815",
                    "number": 213,
                    "chain": "ethereum"
                }
            }
        }
        "#;

        let result = parse_state_request(json_str);

        match result {
            Ok(body) => {
                assert_eq!(
                    body.contract_ids[0].address,
                    "0xb4eccE46b8D4e4abFd03C9B806276A6735C9c092"
                );
                assert_eq!(body.contract_ids[0].chain, "ethereum");
                assert_eq!(
                    body.version.block.hash,
                    "0xd76628379905b342fe3f40a4aa2ef60747fb61e3f10e1c0052313aafc0a73566"
                );
                assert_eq!(body.version.block.chain, "ethereum");
                assert_eq!(body.version.block.number, 213);
            }
            Err(err) => panic!("Parsing failed: {}", err),
        }
    }
}
