use diesel_async::{pooled_connection::bb8::Pool, AsyncPgConnection};
use ethers::types::{H160, H256};
use prost::Message;
use std::{collections::HashMap, str::FromStr, sync::Arc};

use async_trait::async_trait;
use tokio::sync::Mutex;

use super::EVMStateGateway;
use crate::{
    extractor::{evm, ExtractionError, Extractor},
    models::{Chain, ExtractorIdentity},
    pb::{
        sf::substreams::rpc::v2::{BlockScopedData, BlockUndoSignal, ModulesProgress},
        tycho::evm::v1::BlockContractChanges,
    },
    storage::{BlockIdentifier, StorageError},
};

struct Inner {
    cursor: Vec<u8>,
    contracts: HashMap<H160, evm::Account>,
}

pub struct AmbientContractExtractor<G> {
    gateway: G,
    name: String,
    chain: Chain,
    // TODO: There is not reason this needs to be shared
    // try removing the Mutex
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
pub trait AmbientGateway: Send + Sync {
    async fn get_cursor(&self, name: &str, chain: Chain) -> Result<Vec<u8>, StorageError>;
    async fn upsert_contract(
        &self,
        changes: evm::BlockStateChanges,
        new_cursor: &str,
    ) -> Result<(), StorageError>;

    async fn revert(
        &self,
        to: BlockIdentifier,
        new_cursor: &str,
    ) -> Result<evm::AccountUpdate, StorageError>;
}

impl<G> AmbientContractExtractor<G> where G: AmbientGateway {}

#[async_trait]
impl<G> Extractor<G, evm::AccountUpdate> for AmbientContractExtractor<G>
where
    G: AmbientGateway,
{
    fn get_id(&self) -> ExtractorIdentity {
        ExtractorIdentity { chain: self.chain, name: self.name.to_owned() }
    }

    async fn get_cursor(&self) -> String {
        String::from_utf8(self.inner.lock().await.cursor.clone()).expect("Cursor is utf8")
    }

    async fn handle_tick_scoped_data(
        &self,
        inp: BlockScopedData,
    ) -> Result<Option<evm::AccountUpdate>, ExtractionError> {
        let _data = inp
            .output
            .as_ref()
            .unwrap()
            .map_output
            .as_ref()
            .unwrap();

        let msg = evm::BlockStateChanges::try_from_message(
            BlockContractChanges::decode(_data.value.as_slice()).unwrap(),
            &self.name,
            self.chain,
        )?;

        self.gateway
            .upsert_contract(msg, inp.cursor.as_ref())
            .await?;

        self.update_cursor(inp.cursor).await;
        todo!()
    }

    async fn handle_revert(
        &self,
        inp: BlockUndoSignal,
    ) -> Result<Option<evm::AccountUpdate>, ExtractionError> {
        let block_ref = inp
            .last_valid_block
            .ok_or_else(|| ExtractionError::DecodeError("Revert without block ref".into()))?;
        let block_hash = H256::from_str(&block_ref.id).map_err(|err| {
            ExtractionError::DecodeError(format!(
                "Failed to parse {} as block hash: {}",
                block_ref.id, err
            ))
        })?;
        let account_update = self
            .gateway
            .revert(
                BlockIdentifier::Hash(block_hash.as_bytes().to_vec()),
                inp.last_valid_cursor.as_ref(),
            )
            .await?;
        self.update_cursor(inp.last_valid_cursor)
            .await;
        Ok(Some(account_update))
    }

    async fn handle_progress(&self, _inp: ModulesProgress) -> Result<(), ExtractionError> {
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
            inner: Arc::new(Mutex::new(Inner { cursor: Vec::new(), contracts: HashMap::new() })),
        },
        Ok(cursor) => AmbientContractExtractor {
            gateway,
            name: name.to_owned(),
            chain,
            // TODO: load contract here instead.
            inner: Arc::new(Mutex::new(Inner { cursor, contracts: HashMap::new() })),
        },
        Err(err) => return Err(ExtractionError::Setup(err.to_string())),
    };
    Ok(res)
}
