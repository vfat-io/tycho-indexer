pub mod evm;
pub mod runner;

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
pub trait Extractor<G, M>: Send + Sync
where
    G: Send + Sync,
    M: NormalisedMessage,
{
    fn get_id(&self) -> ExtractorIdentity;

    async fn get_cursor(&self) -> String;

    async fn handle_tick_scoped_data(
        &self,
        inp: BlockScopedData,
    ) -> Result<Option<M>, ExtractionError>;

    async fn handle_revert(&self, inp: BlockUndoSignal) -> Result<Option<M>, ExtractionError>;

    async fn handle_progress(&self, inp: ModulesProgress) -> Result<(), ExtractionError>;
}
