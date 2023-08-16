use async_trait::async_trait;
use thiserror::Error;

use crate::{
    models::{ExtractorIdentity, NormalisedMessage},
    pb::sf::substreams::rpc::v2::{BlockScopedData, BlockUndoSignal, ModulesProgress},
};

#[derive(Error, Debug)]
pub enum ExtractionError {}

#[async_trait]
trait Extractor {
    type Message: NormalisedMessage;

    fn get_id(&self) -> ExtractorIdentity;

    async fn setup() -> Self;

    async fn handle_tick_scoped_data(
        &mut self,
        inp: BlockScopedData,
    ) -> Result<Option<Self::Message>, ExtractionError>;

    async fn handle_revert(
        &mut self,
        inp: BlockUndoSignal,
    ) -> Result<Option<Self::Message>, ExtractionError>;

    async fn handle_progress(&mut self, inp: ModulesProgress) -> Result<(), ExtractionError>;
}
