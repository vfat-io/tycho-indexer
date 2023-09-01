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
    number: BlockNumber,
}

#[derive(Debug, Serialize, Deserialize)]
struct BlockNumber {
    chain: String,
    number: i64,
}

fn parse_state_request(json_str: &str) -> Result<StateRequestBody, RpcError> {
    let request_body: StateRequestBody = serde_json::from_str(json_str)?;

    Ok(request_body)
}
