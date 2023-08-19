pub mod evm;

use crate::models::Chain;
use crate::storage::{
    ChainGateway, ContractStateGateway, ExtractorInstanceGateway, ProtocolGateway,
};
use crate::{
    models::{ExtractorIdentity, NormalisedMessage},
    pb::sf::substreams::rpc::v2::{BlockScopedData, BlockUndoSignal, ModulesProgress},
};
use async_trait::async_trait;
use std::error::Error;
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ExtractionError {}

trait VMStateGateway:
    ExtractorInstanceGateway + ChainGateway + ProtocolGateway + ContractStateGateway + Send + Sync
{
}

type VMStateGatewayType<B, TX, T, P, C, S, V> = Arc<
    dyn VMStateGateway<
        Block = B,
        Transaction = TX,
        Token = T,
        ProtocolComponent = P,
        ContractState = C,
        Slot = S,
        Value = V,
    >,
>;

#[async_trait]
trait Extractor {
    type Message: NormalisedMessage;
    type Block;
    type Transaction;
    type Token;
    type ProtocolComponent;
    type ContractState;
    type Slot;
    type Value;

    fn get_id(&self) -> ExtractorIdentity;

    async fn setup(
        name: &str,
        chain: Chain,
        gateway: VMStateGatewayType<
            Self::Block,
            Self::Transaction,
            Self::Token,
            Self::ProtocolComponent,
            Self::ContractState,
            Self::Slot,
            Self::Value,
        >,
    ) -> Result<Box<Self>, Box<dyn Error>>;

    async fn handle_tick_scoped_data(
        &self,
        inp: BlockScopedData,
    ) -> Result<Option<Self::Message>, ExtractionError>;

    async fn handle_revert(
        &self,
        inp: BlockUndoSignal,
    ) -> Result<Option<Self::Message>, ExtractionError>;

    async fn handle_progress(&self, inp: ModulesProgress) -> Result<(), ExtractionError>;
}
