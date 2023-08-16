use std::sync::Arc;

use super::{Extractor, VMStateGatewayType};

struct AmbientPool {}

struct ERC20Token {}

struct EVMBlock {}

struct EVMTransaction {}

struct EVMAccount {}

pub struct AmbientContractExtractor {
    gateway: VMStateGatewayType<
        AmbientPool,
        ERC20Token,
        EVMBlock,
        EVMTransaction,
        EVMAccount,
        Vec<u8>,
        Vec<u8>,
    >,
}
