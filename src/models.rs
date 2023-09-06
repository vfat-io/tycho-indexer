use thiserror::Error;

#[derive(Error, Debug)]
pub enum ChainError {
    #[error("Unknown blockchain value: {0}")]
    UnknownChain(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Chain {
    Ethereum,
    Starknet,
    ZkSync,
}

impl TryFrom<String> for Chain {
    type Error = ChainError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            "ethereum" => Ok(Chain::Ethereum),
            "starknet" => Ok(Chain::Starknet),
            "zksync" => Ok(Chain::ZkSync),
            _ => Err(ChainError::UnknownChain(value)),
        }
    }
}

impl ToString for Chain {
    fn to_string(&self) -> String {
        format!("{:?}", self).to_lowercase()
    }
}

pub enum ProtocolSystem {
    Ambient,
}

pub enum ImplementationType {
    Vm,
    Custom,
}

pub enum FinancialType {
    Swap,
    Lend,
    Leverage,
    Psm,
}

pub struct ProtocolType {
    name: String,
    attribute_schema: serde_json::Value,
    financial_type: FinancialType,
    implementation_type: ImplementationType,
}

pub struct ExtractorIdentity {
    pub chain: Chain,
    pub name: String,
}

#[derive(Debug)]
pub struct ExtractionState {
    pub name: String,
    pub chain: Chain,
    pub attributes: serde_json::Value,
    pub cursor: Vec<u8>,
}

pub trait NormalisedMessage {
    fn source(&self) -> ExtractorIdentity;
}
