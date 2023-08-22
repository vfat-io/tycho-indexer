use diesel_async::pooled_connection::bb8::Pool;
use diesel_async::AsyncPgConnection;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;

use super::EVMStateGateway;
use crate::extractor::evm;
use crate::extractor::ExtractionError;
use crate::extractor::Extractor;
use crate::models::Chain;
use crate::models::ExtractorIdentity;
use crate::pb::sf::substreams::rpc::v2::BlockScopedData;
use crate::pb::sf::substreams::rpc::v2::BlockUndoSignal;
use crate::pb::sf::substreams::rpc::v2::ModulesProgress;
use crate::storage::BlockIdentifier;
use crate::storage::StorageError;

struct Inner {
    cursor: Vec<u8>,
}

pub struct AmbientContractExtractor<G> {
    gateway: G,
    name: String,
    chain: Chain,
    inner: Arc<Mutex<Inner>>,
}

impl<DB> AmbientContractExtractor<DB> {
    async fn update_cursor(&self, cursor: String) {
        let cursor_bytes: Vec<u8> = cursor.into();
        let mut state = self.inner.lock().await;
        state.cursor = cursor_bytes;
    }
}

struct AmbientPgGateway {
    pool: Pool<AsyncPgConnection>,
    state_gateway: EVMStateGateway<AsyncPgConnection>,
}

#[async_trait]
trait AmbientGateway: Send + Sync {
    async fn get_cursor(&self, name: &str, chain: Chain) -> Result<Vec<u8>, StorageError>;
    async fn upsert_contract(
        &self,
        changes: evm::BlockStateChanges,
        new_cursor: &[u8],
    ) -> Result<(), StorageError>;
    async fn revert_contract(
        &self,
        to: BlockIdentifier,
        new_cursor: &[u8],
    ) -> Result<evm::AccountUpdate, StorageError>;
}

#[async_trait]
impl<G> Extractor<G, evm::AccountUpdate> for AmbientContractExtractor<G>
where
    G: AmbientGateway,
{
    fn get_id(&self) -> ExtractorIdentity {
        ExtractorIdentity {
            chain: self.chain,
            name: self.name.to_owned(),
        }
    }

    async fn get_cursor(&self) -> String {
        String::from_utf8(self.inner.lock().await.cursor.clone()).expect("Cursor is utf8")
    }

    async fn handle_tick_scoped_data(
        &self,
        inp: BlockScopedData,
    ) -> Result<Option<evm::AccountUpdate>, ExtractionError> {
        let _data = inp.output.as_ref().unwrap().map_output.as_ref().unwrap();
        // let msg = Message::decode::<Changes>(data.value.as_slice()).unwrap();
        self.update_cursor(inp.cursor).await;
        todo!()
    }

    async fn handle_revert(
        &self,
        inp: BlockUndoSignal,
    ) -> Result<Option<evm::AccountUpdate>, ExtractionError> {
        self.update_cursor(inp.last_valid_cursor).await;
        todo!()
    }

    async fn handle_progress(&self, inp: ModulesProgress) -> Result<(), ExtractionError> {
        todo!()
    }
}

async fn setup<G: AmbientGateway>(
    name: &str,
    chain: Chain,
    gateway: G,
) -> Result<AmbientContractExtractor<G>, ExtractionError> {
    // check if this extractor has state
    let res = match gateway.get_cursor(name, chain).await {
        Err(StorageError::NotFound(_, _)) => AmbientContractExtractor {
            gateway,
            name: name.to_owned(),
            chain,
            inner: Arc::new(Mutex::new(Inner { cursor: Vec::new() })),
        },
        Ok(cursor) => AmbientContractExtractor {
            gateway,
            name: name.to_owned(),
            chain,
            inner: Arc::new(Mutex::new(Inner { cursor })),
        },
        Err(err) => return Err(ExtractionError::Setup(err.to_string())),
    };
    Ok(res)
}
