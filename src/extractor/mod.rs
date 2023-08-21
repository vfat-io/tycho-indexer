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
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ExtractionError {
    #[error("Extractor setup failed: {0}")]
    Setup(String),
    #[error("Unexpected extraction error: {0}")]
    Unkown(String),
}

#[async_trait]
trait Extractor: Sized {
    type Message: NormalisedMessage;
    type Gateway: Send + Sync;

    fn get_id(&self) -> ExtractorIdentity;

    async fn setup(
        name: &str,
        chain: Chain,
        gateway: Self::Gateway,
    ) -> Result<Self, ExtractionError>;

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
