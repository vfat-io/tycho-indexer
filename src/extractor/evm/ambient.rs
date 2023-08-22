use futures03::lock::Mutex;
use std::error::Error;
use std::sync::Arc;

use async_trait::async_trait;
use ethers::types::U256;

use crate::extractor::evm;
use crate::extractor::ExtractionError;
use crate::extractor::Extractor;
use crate::extractor::VMStateGatewayType;
use crate::models::Chain;
use crate::models::ExtractorIdentity;
use crate::pb::sf::substreams::rpc::v2::BlockScopedData;
use crate::pb::sf::substreams::rpc::v2::BlockUndoSignal;
use crate::pb::sf::substreams::rpc::v2::ModulesProgress;

struct Inner {
    cursor: Vec<u8>,
}

pub struct AmbientContractExtractor<DB> {
    gateway: VMStateGatewayType<
        DB,
        evm::Block,
        evm::Transaction,
        evm::ERC20Token,
        evm::SwapPool,
        evm::Account,
        U256,
        U256,
    >,
    inner: Arc<Mutex<Inner>>,
}

impl<DB> AmbientContractExtractor<DB> {
    async fn update_cursor(&self, cursor: String) {
        let cursor_bytes: Vec<u8> = cursor.into();
        let mut state = self.inner.lock().await;
        state.cursor = cursor_bytes;
    }
}

#[async_trait]
impl<DB> Extractor<DB> for AmbientContractExtractor<DB> {
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
        name: &str,
        chain: Chain,
        gateway: VMStateGatewayType<
            DB,
            Self::Block,
            Self::Transaction,
            Self::Token,
            Self::ProtocolComponent,
            Self::ContractState,
            Self::Slot,
            Self::Value,
        >,
    ) -> Result<Box<Self>, Box<dyn Error>> {
        // check if this extractor has state
        todo!()
    }

    async fn handle_tick_scoped_data(
        &self,
        inp: BlockScopedData,
    ) -> Result<Option<Self::Message>, ExtractionError> {
        let _data = inp.output.as_ref().unwrap().map_output.as_ref().unwrap();
        // let msg = Message::decode::<Changes>(data.value.as_slice()).unwrap();
        self.update_cursor(inp.cursor).await;
        todo!()
    }

    async fn handle_revert(
        &self,
        inp: BlockUndoSignal,
    ) -> Result<Option<Self::Message>, ExtractionError> {
        self.update_cursor(inp.last_valid_cursor).await;
        todo!()
    }

    async fn handle_progress(&self, inp: ModulesProgress) -> Result<(), ExtractionError> {
        todo!()
    }
}
