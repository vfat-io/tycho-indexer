pub enum Chain {
    Ethereum,
    Starknet,
    ZkSync,
}

pub struct ExtractorIdentity {
    pub chain: Chain,
    pub name: String,
}

pub trait NormalisedMessage {
    fn source(&self) -> ExtractorIdentity;
}
