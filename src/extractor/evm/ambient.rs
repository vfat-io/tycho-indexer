use std::error::Error;

use async_trait::async_trait;
use ethers::types::U256;

use crate::extractor::evm;
use crate::extractor::ExtractionError;
use crate::extractor::Extractor;
use crate::extractor::VMStateGatewayType;
use crate::models::ExtractorIdentity;
use crate::pb::sf::substreams::rpc::v2::BlockScopedData;
use crate::pb::sf::substreams::rpc::v2::BlockUndoSignal;
use crate::pb::sf::substreams::rpc::v2::ModulesProgress;

pub struct AmbientContractExtractor {
    gateway: VMStateGatewayType<
        evm::SwapPool,
        evm::ERC20Token,
        evm::Block,
        evm::Transaction,
        evm::Account,
        Vec<u8>,
        Vec<u8>,
    >,
}

#[async_trait]
impl Extractor for AmbientContractExtractor {
    type Message = evm::AccountUpdate;
    type Block = evm::Block;
    type Transaction = evm::Transaction;
    type Token = evm::ERC20Token;
    type ProtocolComponent = evm::SwapPool;
    type ContractState = evm::Account;
    type Slot = U256;
    type Value = U256;

    fn get_id(&self) -> ExtractorIdentity {
        todo!()
    }

    async fn setup(
        gateway: VMStateGatewayType<
            Self::Block,
            Self::Transaction,
            Self::Token,
            Self::ProtocolComponent,
            Self::ContractState,
            Self::Slot,
            Self::Value,
        >,
    ) -> Result<Box<Self>, Box<dyn Error>> {
        todo!()
    }

    async fn handle_tick_scoped_data(
        &self,
        inp: BlockScopedData,
    ) -> Result<Option<Self::Message>, ExtractionError> {
        todo!()
    }

    async fn handle_revert(
        &self,
        inp: BlockUndoSignal,
    ) -> Result<Option<Self::Message>, ExtractionError> {
        todo!()
    }

    async fn handle_progress(&self, inp: ModulesProgress) -> Result<(), ExtractionError> {
        todo!()
    }
}
