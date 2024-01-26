use crate::{
    extractor::{
        evm,
        evm::{AccountUpdate, Block},
    },
    hex_bytes::Bytes,
    models::{Chain, ExtractionState},
    storage::{
        postgres::cache::CachedGateway, BlockIdentifier, StorableProtocolComponent, StorageError,
    },
};
use async_trait::async_trait;
use diesel_async::{pooled_connection::deadpool::Pool, AsyncPgConnection};
use mockall::automock;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info, instrument};

// TODO: Maybe use the same Inner as AmbientExtractor
pub struct Inner {
    cursor: Vec<u8>,
    last_processed_block: Option<Block>,
}

pub struct NativeContractExtractor<G> {
    gateway: G,
    name: String,
    chain: Chain,
    protocol_system: String,
    inner: Arc<Mutex<Inner>>,
}

impl<DB> NativeContractExtractor<DB> {
    async fn update_cursor(&self, cursor: String) {
        let cursor_bytes: Vec<u8> = cursor.into();
        let mut state = self.inner.lock().await;
        state.cursor = cursor_bytes;
    }

    async fn update_last_processed_block(&self, block: Block) {
        let mut state = self.inner.lock().await;
        state.last_processed_block = Some(block);
    }
}

pub struct NativePgGateway {
    name: String,
    chain: Chain,
    pool: Pool<AsyncPgConnection>,
    state_gateway: CachedGateway,
}

#[automock]
#[async_trait]
pub trait NativeGateway: Send + Sync {
    async fn get_cursor(&self) -> Result<Vec<u8>, StorageError>;
    async fn upsert_contract(
        &self,
        changes: &evm::BlockEntityChanges,
        new_cursor: &str,
    ) -> Result<(), StorageError>;

    async fn revert(
        &self,
        current: Option<BlockIdentifier>,
        to: &BlockIdentifier,
        new_cursor: &str,
    ) -> Result<evm::BlockEntityChangesResult, StorageError>;
}

impl NativePgGateway {
    pub fn new(
        name: String,
        chain: Chain,
        pool: Pool<AsyncPgConnection>,
        state_gateway: CachedGateway,
    ) -> Self {
        Self { name: name.to_owned(), chain, pool, state_gateway }
    }

    #[instrument(skip_all)]
    async fn save_cursor(&self, block: &Block, new_cursor: &str) -> Result<(), StorageError> {
        let state =
            ExtractionState::new(self.name.to_string(), self.chain, None, new_cursor.as_bytes());
        self.state_gateway
            .save_state(block, &state)
            .await?;
        Ok(())
    }

    #[instrument(skip_all, fields(chain = % self.chain, name = % self.name, block_number = % changes.block.number))]
    async fn forward(
        &self,
        changes: &evm::BlockEntityChanges,
        new_cursor: &str,
    ) -> Result<(), StorageError> {
        debug!("Upserting block");
        self.state_gateway
            .upsert_block(&changes.block)
            .await?;

        // All the transactions should already sorted by index
        for tx in changes.state_updates.iter() {
            self.state_gateway
                .upsert_tx(&changes.block, &tx.tx)
                .await?;
            if tx.has_new_protocols() {
                for (component_id, new_protocol_component) in tx.new_protocol_components.iter() {
                    debug!(component_id = %component_id, "New protocol component found at");
                    self.state_gateway
                        .insert_component(&changes.block, &new_protocol_component)
                        .await?;
                }
            }

            if tx.has_state_changes() {
                for (component_id, state_change) in tx.protocol_states.iter() {
                    debug!(component_id = %component_id, "State change found at");
                    self.state_gateway
                        .upsert_state_change(&changes.block, &state_change)
                        .await?;
                }
            }

            if tx.has_balance_changes() {
                for (component_id, tokens) in tx.balance_changes.iter() {
                    for (token, tvl_change) in tokens {
                        self.state_gateway
                            .upsert_tvl_change(&changes.block, &token, &tvl_change, &component_id)
                            .await?;
                    }
                }
            }
        }

        self.save_cursor(&changes.block, new_cursor)
            .await?;

        Result::<(), StorageError>::Ok(())
    }
}
