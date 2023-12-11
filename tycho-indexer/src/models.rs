use std::collections::HashMap;

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use strum_macros::{Display, EnumString};

use crate::hex_bytes::Bytes;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, EnumString, Display, Default,
)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
pub enum Chain {
    #[default]
    Ethereum,
    Starknet,
    ZkSync,
}

#[allow(dead_code)]
pub enum ProtocolSystem {
    Ambient,
}

#[allow(dead_code)]
pub enum ImplementationType {
    Vm,
    Custom,
}

#[allow(dead_code)]
pub enum FinancialType {
    Swap,
    Lend,
    Leverage,
    Psm,
}

#[allow(dead_code)]
pub struct ProtocolType {
    name: String,
    attribute_schema: serde_json::Value,
    financial_type: FinancialType,
    implementation_type: ImplementationType,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct ExtractorIdentity {
    pub chain: Chain,
    pub name: String,
}

impl ExtractorIdentity {
    pub fn new(chain: Chain, name: &str) -> Self {
        Self { chain, name: name.to_owned() }
    }
}

impl std::fmt::Display for ExtractorIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.chain, self.name)
    }
}

#[derive(Debug, PartialEq)]
pub struct ExtractionState {
    pub name: String,
    pub chain: Chain,
    pub attributes: serde_json::Value,
    pub cursor: Vec<u8>,
}

impl ExtractionState {
    pub fn new(
        name: String,
        chain: Chain,
        attributes: Option<serde_json::Value>,
        cursor: &[u8],
    ) -> Self {
        ExtractionState {
            name,
            chain,
            attributes: attributes.unwrap_or_default(),
            cursor: cursor.to_vec(),
        }
    }
}

#[typetag::serde(tag = "type")]
pub trait NormalisedMessage: std::fmt::Debug + std::fmt::Display + Send + Sync + 'static {
    fn source(&self) -> ExtractorIdentity;
}

#[allow(dead_code)]
pub struct ProtocolComponent<T> {
    // an id for this component, could be hex repr of contract address
    id: String,
    // what system this component belongs to
    protocol_system: ProtocolSystem,
    // more metadata information about the components general type (swap, lend, bridge, etc.)
    protocol_type: ProtocolType,
    // Blockchain the component belongs to
    chain: Chain,
    // holds the tokens tradable
    tokens: Vec<T>,
    // ID's referring to related contracts
    contract_ids: Vec<String>,
    // allows to express some validation over the attributes if necessary
    attribute_schema: Bytes,
}

impl ProtocolComponent<String> {
    #[allow(dead_code)]
    pub fn try_from_message(
        msg: substreams::ProtocolComponent,
        protocol_system: ProtocolSystem,
        protocol_type: ProtocolType,
        chain: Chain,
    ) -> Result<Self, ExtractionError> {
        let id = String::from_utf8(msg.id)
            .map_err(|error| ExtractionError::DecodeError(error.to_string()))
            .unwrap();
        let tokens = msg
            .tokens
            .into_iter()
            .map(|t| {
                String::from_utf8(t)
                    .map_err(|error| ExtractionError::DecodeError(error.to_string()))
                    .unwrap()
            })
            .collect::<Vec<_>>();

        let contract_ids = msg
            .contracts
            .into_iter()
            .map(|contract_id| {
                String::from_utf8(contract_id)
                    .map_err(|error| ExtractionError::DecodeError(error.to_string()))
                    .unwrap()
            })
            .collect::<Vec<_>>();

        return Ok(Self {
            id,
            protocol_type,
            protocol_system,
            tokens,
            contract_ids,
            attribute_schema: Bytes::default(),
            chain,
        });
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct TvlChange<T> {
    token: T,
    new_balance: f64,
    // tx where the this balance was observed
    tx: String,
}

impl TvlChange<String> {
    #[allow(dead_code)]
    pub fn try_from_message(
        msg: substreams::TvlUpdate,
        tx: &Transaction,
    ) -> Result<Self, ExtractionError> {
        Ok(Self {
            token: String::from_utf8(msg.token)
                .map_err(|error| ExtractionError::DecodeError(error.to_string()))?,
            new_balance: f64::from_bits(u64::from_le_bytes(msg.balance.try_into().unwrap())),
            tx: tx.hash.to_string(),
        })
    }
}

pub struct ProtocolState {
    // associates the back to a component, which has metadata like type, tokens , etc.
    component_id: String,
    // holds all the protocol specific attributes, validates by the components schema
    attributes: HashMap<String, Bytes>,
    // via transaction, we can trace back when this state became valid
    modify_tx: Bytes,
}
