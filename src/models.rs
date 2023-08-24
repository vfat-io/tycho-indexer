#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Chain {
    Ethereum,
    Starknet,
    ZkSync,
}

impl From<String> for Chain {
    fn from(value: String) -> Self {
        if value == "ethereum" {
            Chain::Ethereum
        } else if value == "starknet" {
            Chain::Starknet
        } else if value == "zksync" {
            Chain::ZkSync
        } else {
            panic!("Can't interpret {} as chain!", value);
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
