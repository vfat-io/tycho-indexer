pub enum Chain {
    Ethereum,
    Starknet,
    ZkSync,
}

pub enum ProtocolSystem{
    Ambient,
}

pub struct ExtractorIdentity {
    pub chain: Chain,
    pub name: String,
}

pub struct ExtractorInstance {
    pub name: String,
    pub chain: Chain,
    pub attributes: serde_json::Value,
    pub cursos: Vec<u8>,
}

pub trait NormalisedMessage {
    fn source(&self) -> ExtractorIdentity;
}
