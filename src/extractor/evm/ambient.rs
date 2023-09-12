use diesel_async::{
    pooled_connection::bb8::Pool, scoped_futures::ScopedFutureExt, AsyncConnection,
    AsyncPgConnection,
};
use ethers::types::{H160, H256, U256};
use prost::Message;
use serde_json::json;
use std::{collections::HashMap, str::FromStr, sync::Arc};

use async_trait::async_trait;
use tokio::sync::Mutex;

use super::EVMStateGateway;
use crate::{
    extractor::{evm, ExtractionError, Extractor},
    models::{Chain, ExtractionState, ExtractorIdentity},
    pb::{
        sf::substreams::rpc::v2::{BlockScopedData, BlockUndoSignal, ModulesProgress},
        tycho::evm::v1::BlockContractChanges,
    },
    storage::{
        postgres::contract_state::{parse_u256_slot_entry, u256_to_bytes},
        BlockIdentifier, BlockOrTimestamp, ContractId, StorageError, Version, VersionKind,
    },
};

const AMBIENT_CONTRACT: [u8; 20] = hex_literal::hex!("aaaaaaaaa24eeeb8d57d431224f73832bc34f688");

struct Inner {
    cursor: Vec<u8>,
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
    name: String,
    chain: Chain,
    pool: Pool<AsyncPgConnection>,
    state_gateway: EVMStateGateway<AsyncPgConnection>,
    contract_cache: Mutex<HashMap<H160, evm::Account>>,
}

#[async_trait]
pub trait AmbientGateway: Send + Sync {
    async fn get_cursor(&self, name: &str, chain: Chain) -> Result<Vec<u8>, StorageError>;
    async fn upsert_contract(
        &self,
        changes: &evm::BlockStateChanges,
        new_cursor: &str,
    ) -> Result<(), StorageError>;

    async fn revert(
        &self,
        to: BlockIdentifier,
        new_cursor: &str,
    ) -> Result<evm::BlockAccountChanges, StorageError>;
}

impl AmbientPgGateway {
    async fn save_cursor(
        &self,
        new_cursor: &str,
        conn: &mut AsyncPgConnection,
    ) -> Result<(), StorageError> {
        let state = ExtractionState {
            name: self.name.clone(),
            chain: self.chain,
            attributes: json!(null),
            cursor: new_cursor.as_bytes().to_vec(),
        };
        self.state_gateway
            .save_state(&state, conn)
            .await?;
        Ok(())
    }

    async fn forward(
        &self,
        changes: &evm::BlockStateChanges,
        new_cursor: &str,
        conn: &mut AsyncPgConnection,
    ) -> Result<(), StorageError> {
        let contract_id = ContractId::new(Chain::Ethereum, AMBIENT_CONTRACT.to_vec());
        self.state_gateway
            .upsert_block(&changes.block, conn)
            .await?;
        // TODO: handle account not found case in that case
        // transaction.to should be None else error.
        let mut account = self
            .state_gateway
            .get_contract(&contract_id, &None, conn)
            .await?;
        for update in changes.tx_updates.iter() {
            account.transition(update);
            self.state_gateway
                .upsert_tx(&update.tx, conn)
                .await?;
            self.state_gateway
                .upsert_contract(&account, conn)
                .await?;
        }
        let slot_changes = changes
            .tx_updates
            .iter()
            .map(|update| {
                (
                    update.tx.hash.as_bytes(),
                    [(update.address, &update.slots)]
                        .iter()
                        .map(|(addr, slots)| {
                            (
                                addr.as_bytes().to_vec(),
                                slots
                                    .iter()
                                    .map(|(s, v)| (u256_to_bytes(s), Some(u256_to_bytes(v))))
                                    .collect::<HashMap<_, _>>(),
                            )
                        })
                        .collect::<HashMap<Vec<u8>, HashMap<Vec<u8>, Option<Vec<u8>>>>>(),
                )
            })
            .collect::<Vec<_>>();
        self.state_gateway
            .upsert_slots(slot_changes.as_slice(), conn)
            .await?;
        self.save_cursor(new_cursor, conn)
            .await?;
        Result::<(), StorageError>::Ok(())
    }
}

#[async_trait]
impl AmbientGateway for AmbientPgGateway {
    async fn get_cursor(&self, name: &str, chain: Chain) -> Result<Vec<u8>, StorageError> {
        let mut conn = self.pool.get().await.unwrap();
        let state = self
            .state_gateway
            .get_state(name, chain, &mut conn)
            .await?;
        Ok(state.cursor)
    }
    async fn upsert_contract(
        &self,
        changes: &evm::BlockStateChanges,
        new_cursor: &str,
    ) -> Result<(), StorageError> {
        let mut conn = self.pool.get().await.unwrap();
        conn.transaction(|conn| {
            async move {
                self.forward(changes, new_cursor, conn)
                    .await
            }
            .scope_boxed()
        })
        .await?;
        Ok(())
    }

    // How should we handle this ideally we want a single query for reverted
    // transactions but doing synchronising that that might be complex and slow
    // - So everyone should request reverts as they need them, the call is
    // cached for a few blocks second requester will directly hit the cache.
    // Problem: During revert, I am not interested in inserting things by tx
    // I simply want to generate a diff in a client digestible format, as
    // quickly as possible:
    // Currently balance and code updates are not exposed by gateway. Following
    // changes are necessary to make this more ergonomic: Change keep
    // get_contract methods that return full or partial accounts, change upsert
    // and delta methods to return account update entities.
    // Bunch of housekeeping missing - e.g. how do we know which was the latest
    // extracted version for an extractor? I mean we know the cursor but we
    // don't know yet how that relates to a block.. Do we need that in the DB?
    // Can the extractor not decide for itself whether it is syncing or not? In
    // this case requesting latest version can be deceiving. Responses form the
    // service should always contains some kind of time reference.
    async fn revert(
        &self,
        to: BlockIdentifier,
        new_cursor: &str,
    ) -> Result<evm::BlockAccountChanges, StorageError> {
        let mut conn = self.pool.get().await.unwrap();
        let res = conn
            .transaction(|conn| {
                async move {
                    let block = self
                        .state_gateway
                        .get_block(&to, conn)
                        .await?;
                    let target = BlockOrTimestamp::Block(to);
                    // TODO: everything below is due to unergonomic interface,
                    // but without this step the required distinction between
                    // input and output models would not have been very
                    // clear/hard to predict!

                    let address = H160::from(AMBIENT_CONTRACT);
                    let target_version = Version(target, VersionKind::Last);
                    // Retrieve contracts at target version and put into hash map
                    let account = self
                        .state_gateway
                        .get_contracts(
                            self.chain,
                            Some(&[AMBIENT_CONTRACT.as_slice()]),
                            Some(&target_version),
                            false,
                            conn,
                        )
                        .await?;
                    let account = account
                        .into_iter()
                        .map(|a| (a.address, a))
                        .collect::<HashMap<_, _>>();
                    let account = account
                        .get(&address)
                        .expect("ambient account exists");

                    // retrieve deltas
                    let mut delta = self
                        .state_gateway
                        .get_slots_delta(self.chain, None, &target_version.0, conn)
                        .await?
                        .into_iter()
                        .map(|(addr_raw, slots_raw)| {
                            let address = H160::from_slice(addr_raw.as_slice());
                            let slots = slots_raw
                                .iter()
                                .map(|(rk, rv)| parse_u256_slot_entry(rk, rv.as_deref()))
                                .collect::<Result<HashMap<U256, U256>, StorageError>>()?;
                            Ok((address, slots))
                        })
                        .collect::<Result<HashMap<H160, HashMap<_, _>>, StorageError>>()?;
                    let delta = delta
                        .remove(&address)
                        .unwrap_or_else(HashMap::new);

                    // create a single account update
                    let account_updates = [(
                        address,
                        evm::AccountUpdate::new(
                            address,
                            self.chain,
                            delta,
                            Some(account.balance),
                            Some(account.code.clone()),
                        ),
                    )]
                    .into_iter()
                    .collect::<HashMap<_, _>>();

                    self.save_cursor(new_cursor, conn)
                        .await?;

                    let changes = evm::BlockAccountChanges {
                        chain: self.chain,
                        extractor: self.name.clone(),
                        block,
                        account_updates,
                        new_pools: HashMap::new(),
                    };
                    Result::<evm::BlockAccountChanges, StorageError>::Ok(changes)
                }
                .scope_boxed()
            })
            .await?;
        Ok(res)
    }
}

// We need to decide whether we want to retrieve accounts from the gateway or account updates.
// Alternatively we could use things as they are an introduce a wrapper type for slots.
// One way or another it seems that we are lacking one generic parameters.
// We can easily convert from AccountUpdate to Account and given a precious Account we can forward
// using AccountUpdate.

impl<G> AmbientContractExtractor<G>
where
    G: AmbientGateway,
{
    async fn new(name: &str, chain: Chain, gateway: G) -> Result<Self, ExtractionError> {
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
}

#[async_trait]
impl<G> Extractor<G, evm::BlockAccountChanges> for AmbientContractExtractor<G>
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
    ) -> Result<Option<evm::BlockAccountChanges>, ExtractionError> {
        let _data = inp
            .output
            .as_ref()
            .unwrap()
            .map_output
            .as_ref()
            .unwrap();

        let msg = match evm::BlockStateChanges::try_from_message(
            BlockContractChanges::decode(_data.value.as_slice())?,
            &self.name,
            self.chain,
        ) {
            Ok(changes) => changes,
            Err(ExtractionError::Empty) => return Ok(None),
            Err(e) => return Err(e),
        };

        self.gateway
            .upsert_contract(&msg, inp.cursor.as_ref())
            .await?;

        self.update_cursor(inp.cursor).await;
        Ok(Some(msg.merge_updates()?))
    }

    async fn handle_revert(
        &self,
        inp: BlockUndoSignal,
    ) -> Result<Option<evm::BlockAccountChanges>, ExtractionError> {
        let block_ref = inp
            .last_valid_block
            .ok_or_else(|| ExtractionError::DecodeError("Revert without block ref".into()))?;
        let block_hash = H256::from_str(&block_ref.id).map_err(|err| {
            ExtractionError::DecodeError(format!(
                "Failed to parse {} as block hash: {}",
                block_ref.id, err
            ))
        })?;
        let changes = self
            .gateway
            .revert(
                BlockIdentifier::Hash(block_hash.as_bytes().to_vec()),
                inp.last_valid_cursor.as_ref(),
            )
            .await?;
        self.update_cursor(inp.last_valid_cursor)
            .await;
        // TODO: Once changes.account_updates can be empty return None here if
        // it is
        Ok(Some(changes))
    }

    async fn handle_progress(&self, _inp: ModulesProgress) -> Result<(), ExtractionError> {
        todo!()
    }
}
