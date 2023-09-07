use std::collections::{hash_map::Entry, HashSet};

use async_trait::async_trait;
use chrono::{NaiveDateTime, Utc};
use diesel_async::RunQueryDsl;
use ethers::types::{H160, H256, U256};

use crate::{
    extractor::evm,
    storage::{
        AccountToContractStore, BlockIdentifier, BlockOrTimestamp, ContractId,
        ContractStateGateway, ContractStore, SlotChangeSet, StorableBlock, StorableContract,
        StorableTransaction, Version, VersionKind,
    },
};

use super::*;

fn u256_to_bytes(v: &U256) -> Vec<u8> {
    let mut bytes32 = [0u8; 32];
    v.to_big_endian(&mut bytes32);
    bytes32.to_vec()
}

impl StorableContract<orm::Contract, orm::NewContract, i64> for evm::Account {
    fn from_storage(
        val: orm::Contract,
        chain: Chain,
        balance_modify_tx: &[u8],
        code_modify_tx: &[u8],
        creation_tx: Option<&[u8]>,
    ) -> Self {
        evm::Account::new(
            chain,
            H160::from_slice(&val.account.address),
            val.account.title.clone(),
            HashMap::new(),
            U256::from_big_endian(&val.balance.balance),
            val.code.code,
            H256::from_slice(&val.code.hash),
            H256::from_slice(balance_modify_tx),
            H256::from_slice(code_modify_tx),
            creation_tx.map(H256::from_slice),
        )
    }

    fn to_storage(
        &self,
        chain_id: i64,
        creation_ts: NaiveDateTime,
        tx_id: Option<i64>,
    ) -> orm::NewContract {
        orm::NewContract {
            title: self.title.clone(),
            address: self.address.as_bytes().to_vec(),
            chain_id,
            creation_tx: tx_id,
            created_at: Some(creation_ts),
            deleted_at: None,
            balance: u256_to_bytes(&self.balance),
            code: self.code.clone(),
            code_hash: self.code_hash.as_bytes().to_vec(),
        }
    }

    fn chain(&self) -> Chain {
        self.chain
    }

    fn creation_tx(&self) -> Option<&[u8]> {
        self.creation_tx
            .as_ref()
            .map(|h| h.as_bytes())
    }

    fn address(&self) -> &[u8] {
        self.address.as_bytes()
    }

    fn balance_modify_tx(&self) -> &[u8] {
        self.balance_modify_tx.as_bytes()
    }

    fn code_modify_tx(&self) -> &[u8] {
        self.code_modify_tx.as_bytes()
    }

    fn store(&self) -> ContractStore {
        self.slots
            .iter()
            .map(|(s, v)| (u256_to_bytes(s), Some(u256_to_bytes(v))))
            .collect()
    }

    fn set_store(&mut self, store: &ContractStore) -> Result<(), StorageError> {
        self.slots = store
            .iter()
            .map(|(rk, rv)| parse_u256_slot_entry(rk, rv.as_deref()))
            .collect::<Result<HashMap<_, _>, _>>()?;
        Ok(())
    }
}

#[async_trait]
impl<B, TX, A> ContractStateGateway for PostgresGateway<B, TX, A>
where
    B: StorableBlock<orm::Block, orm::NewBlock> + Send + Sync + 'static,
    TX: StorableTransaction<orm::Transaction, orm::NewTransaction, i64> + Send + Sync + 'static,
    A: StorableContract<orm::Contract, orm::NewContract, i64>,
{
    type DB = AsyncPgConnection;
    type Transaction = TX;
    type ContractState = A;

    async fn get_contract(
        &self,
        id: &ContractId,
        version: &Option<&Version>,
        db: &mut Self::DB,
    ) -> Result<Self::ContractState, StorageError> {
        let account_orm: orm::Account = orm::Account::by_id(id, db)
            .await
            .map_err(|err| {
                StorageError::from_diesel(err, "Account", &hex::encode(&id.address), None)
            })?;
        let version_ts = version_to_ts(version, db).await?;

        // TODO: we need to make sure we get the last change here according to VersionKind::Last
        let (balance_tx, balance_orm) = schema::account_balance::table
            .inner_join(schema::transaction::table)
            .filter(schema::account_balance::account_id.eq(account_orm.id))
            .select((schema::transaction::hash, orm::AccountBalance::as_select()))
            .filter(schema::account_balance::valid_from.le(version_ts))
            .filter(
                schema::account_balance::valid_to
                    .gt(Some(version_ts))
                    .or(schema::account_balance::valid_to.is_null()),
            )
            .first::<(Vec<u8>, orm::AccountBalance)>(db)
            .await?;

        // TODO: we need to make sure we get the last change here according to VersionKind::Last
        let (code_tx, code_orm) = schema::contract_code::table
            .inner_join(schema::transaction::table)
            .filter(schema::contract_code::account_id.eq(account_orm.id))
            .select((schema::transaction::hash, orm::ContractCode::as_select()))
            .filter(schema::contract_code::valid_from.le(version_ts))
            .filter(
                schema::contract_code::valid_to
                    .gt(Some(version_ts))
                    .or(schema::contract_code::valid_to.is_null()),
            )
            .first::<(Vec<u8>, orm::ContractCode)>(db)
            .await?;

        let creation_tx = match account_orm.creation_tx {
            Some(tx) => schema::transaction::table
                .filter(schema::transaction::id.eq(tx))
                .select(schema::transaction::hash)
                .first::<Vec<u8>>(db)
                .await
                .ok(),
            None => None,
        };

        let chain_id = account_orm.chain_id;
        let account = Self::ContractState::from_storage(
            orm::Contract { account: account_orm, balance: balance_orm, code: code_orm },
            self.get_chain(chain_id),
            &balance_tx,
            &code_tx,
            creation_tx.as_deref(),
        );
        Ok(account)
    }

    async fn get_contracts(
        &self,
        chain: Chain,
        ids: Option<&[&[u8]]>,
        version: Option<&Version>,
        conn: &mut Self::DB,
    ) -> Result<Vec<Self::ContractState>, StorageError> {
        let chain_id = self.get_chain_id(chain);
        let version_ts = version_to_ts(version, conn).await?;
        let accounts = {
            use schema::account::dsl::*;
            let mut q = account
                .filter(chain_id.eq(chain_id))
                .filter(
                    deleted_at
                        .is_null()
                        .or(deleted_at.lt(version_ts)),
                )
                .order_by(id)
                .select(orm::Account::as_select())
                .into_boxed();

            // if user passed any contract ids filter by those
            // else get all contracts
            if let Some(contract_ids) = ids {
                q = q.filter(address.eq_any(contract_ids));
            }
            q.get_results::<orm::Account>(conn)
                .await?
        };

        // take all ids and query both code and storage
        let account_ids = accounts
            .iter()
            .map(|a| a.id)
            .collect::<HashSet<_>>();

        let balances = {
            use schema::account_balance::dsl::*;
            account_balance
                .inner_join(schema::transaction::table)
                .filter(account_id.eq_any(&account_ids))
                .filter(valid_from.le(version_ts))
                .filter(
                    valid_to
                        .is_null()
                        .or(valid_to.gt(version_ts)),
                )
                .order_by((account_id, schema::transaction::index.desc()))
                .select(orm::AccountBalance::as_select())
                .distinct_on(account_id)
                .get_results::<orm::AccountBalance>(conn)
                .await?
        };

        let codes = {
            use schema::contract_code::dsl::*;
            contract_code
                .inner_join(schema::transaction::table)
                .filter(account_id.eq_any(&account_ids))
                .filter(valid_from.le(version_ts))
                .filter(
                    valid_to
                        .is_null()
                        .or(valid_to.gt(version_ts)),
                )
                .order_by((account_id, schema::transaction::index.desc()))
                .select(orm::ContractCode::as_select())
                .distinct_on(account_id)
                .get_results::<orm::ContractCode>(conn)
                .await?
        };

        // retrieve txhashes
        let tx_ids: HashSet<i64> = accounts
            .iter()
            .filter(|a| a.creation_tx.is_some())
            .map(|a| a.creation_tx.unwrap())
            .chain(codes.iter().map(|c| c.modify_tx))
            // TODO: make balance.modify_tx not nullable
            .chain(
                balances
                    .iter()
                    .map(|b| b.modify_tx.unwrap()),
            )
            .collect();

        let txn_hashes: HashMap<i64, Vec<u8>> = {
            use schema::transaction::dsl::*;
            transaction
                .filter(id.eq_any(tx_ids))
                .select((id, hash))
                .get_results::<(i64, Vec<u8>)>(conn)
                .await?
                .into_iter()
                .collect()
        };

        // TODO: before zipping we should assert that the lengths match!
        accounts
            .into_iter()
            .zip(
                balances
                    .into_iter()
                    .zip(codes.into_iter()),
            )
            .map(|(account, (balance, code))| -> Result<Self::ContractState, StorageError> {
                if !(account.id == balance.account_id && balance.account_id == code.account_id) {
                    return Err(StorageError::DecodeError(format!(
                        "Identity mismatch - while retrieving entries for account id: {} \
                            encountered balance for id {} and code for id {}",
                        &account.id, &balance.account_id, &code.account_id
                    )))
                }

                let balance_tx = txn_hashes
                    .get(
                        &balance
                            .modify_tx
                            .expect("balance tx not null"),
                    )
                    .ok_or_else(|| {
                        StorageError::DecodeError(format!(
                            "Could not find balance transaction for 0x{}",
                            hex::encode(&account.address)
                        ))
                    })?;
                let code_tx = txn_hashes
                    .get(&code.modify_tx)
                    .ok_or_else(|| {
                        StorageError::DecodeError(format!(
                            "Could not find code transaction for 0x{}",
                            hex::encode(&account.address)
                        ))
                    })?;
                let creation_tx = account
                    .creation_tx
                    .as_ref()
                    .map_or(None, |tx_id| {
                        txn_hashes
                            .get(tx_id)
                            .ok_or_else(|| {
                                StorageError::DecodeError(format!(
                                    "Could not find creation transaction for 0x{}",
                                    hex::encode(&account.address)
                                ))
                            })
                            .ok()
                    });

                let contract_orm = orm::Contract { account, balance, code };

                let contract = Self::ContractState::from_storage(
                    contract_orm,
                    self.get_chain(chain_id),
                    balance_tx,
                    code_tx,
                    creation_tx.map(|h| h.as_slice()),
                );
                Ok(contract)
            })
            .collect()
    }

    async fn add_contract(
        &self,
        new: &Self::ContractState,
        db: &mut Self::DB,
    ) -> Result<(), StorageError> {
        let chain_id = self.get_chain_id(new.chain());
        let mut txns = HashSet::new();
        txns.insert(new.code_modify_tx());
        txns.insert(new.balance_modify_tx());
        if let Some(h) = new.creation_tx() {
            txns.insert(h);
        }
        let tx_data: HashMap<Vec<u8>, (i64, NaiveDateTime)> = schema::transaction::table
            .inner_join(schema::block::table)
            .filter(schema::transaction::hash.eq_any(&txns))
            .select((schema::transaction::hash, (schema::transaction::id, schema::block::ts)))
            .get_results::<(Vec<u8>, (i64, NaiveDateTime))>(db)
            .await?
            .into_iter()
            .collect();

        let (creation_tx_id, created_ts) = if let Some(h) = new.creation_tx() {
            // TODO error handling for missing related tx entity
            let (tx_id, ts) = tx_data
                .get(h)
                .expect("creation tx present");
            (Some(*tx_id), *ts)
        } else {
            (None, chrono::Utc::now().naive_utc())
        };

        let new_contract = new.to_storage(chain_id, created_ts, creation_tx_id);
        let hex_addr = hex::encode(new.address());
        let account_id = diesel::insert_into(schema::account::table)
            .values(new_contract.new_account())
            .returning(schema::account::id)
            .get_result::<i64>(db)
            .await
            .map_err(|err| StorageError::from_diesel(err, "Account", &hex_addr, None))?;

        let (balance_modify_tx_id, balance_modify_ts) = tx_data
            .get(new.balance_modify_tx())
            .expect("balance tx present");
        diesel::insert_into(schema::account_balance::table)
            .values(new_contract.new_balance(account_id, *balance_modify_tx_id, *balance_modify_ts))
            .execute(db)
            .await
            .map_err(|err| StorageError::from_diesel(err, "AccountBalance", &hex_addr, None))?;

        let (code_modify_tx_id, code_modify_ts) = tx_data
            .get(new.code_modify_tx())
            .expect("code tx present");
        diesel::insert_into(schema::contract_code::table)
            .values(new_contract.new_code(account_id, *code_modify_tx_id, *code_modify_ts))
            .execute(db)
            .await
            .map_err(|err| StorageError::from_diesel(err, "ContractCode", &hex_addr, None))?;

        Ok(())
    }

    async fn delete_contract(
        &self,
        id: &ContractId,
        at_tx: &Self::Transaction,
        conn: &mut AsyncPgConnection,
    ) -> Result<(), StorageError> {
        let account = orm::Account::by_id(id, conn)
            .await
            .map_err(|err| StorageError::from_diesel(err, "Account", &id.to_string(), None))?;
        let tx = orm::Transaction::by_hash(at_tx.hash(), conn)
            .await
            .map_err(|err| {
                StorageError::from_diesel(
                    err,
                    "Account",
                    &hex::encode(at_tx.hash()),
                    Some("Transaction".to_owned()),
                )
            })?;
        let block_ts = schema::block::table
            .filter(schema::block::id.eq(tx.block_id))
            .select(schema::block::ts)
            .first::<NaiveDateTime>(conn)
            .await?;
        if let Some(tx_id) = account.deletion_tx {
            if tx.id != tx_id {
                return Err(StorageError::Unexpected(format!(
                    "Account {} was already deleted at {:?}!",
                    hex::encode(account.address),
                    account.deleted_at,
                )))
            }
            // Noop if called twice on deleted contract
            return Ok(())
        };
        diesel::update(schema::account::table.filter(schema::account::id.eq(account.id)))
            .set((schema::account::deletion_tx.eq(tx.id), schema::account::deleted_at.eq(block_ts)))
            .execute(conn)
            .await?;
        diesel::update(
            schema::contract_storage::table
                .filter(schema::contract_storage::account_id.eq(account.id)),
        )
        .set(schema::contract_storage::valid_to.eq(block_ts))
        .execute(conn)
        .await?;

        diesel::update(
            schema::account_balance::table
                .filter(schema::account_balance::account_id.eq(account.id)),
        )
        .set(schema::account_balance::valid_to.eq(block_ts))
        .execute(conn)
        .await?;

        diesel::update(
            schema::contract_code::table.filter(schema::contract_code::account_id.eq(account.id)),
        )
        .set(schema::contract_code::valid_to.eq(block_ts))
        .execute(conn)
        .await?;
        Ok(())
    }

    async fn get_contract_slots(
        &self,
        chain: Chain,
        contracts: Option<&[&[u8]]>,
        at: Option<&Version>,
        conn: &mut Self::DB,
    ) -> Result<HashMap<Vec<u8>, ContractStore>, StorageError> {
        let version_ts = if let Some(Version(version, kind)) = at {
            if !matches!(kind, VersionKind::Last) {
                return Err(StorageError::Unsupported(format!(
                    "Unsupported version kind: {:?}",
                    kind
                )))
            }
            version_to_ts(&Some(version), conn).await?
        } else {
            Utc::now().naive_utc()
        };

        let slots = {
            use schema::{account, contract_storage::dsl::*};

            let chain_id = self.get_chain_id(chain);
            let mut q = contract_storage
                .inner_join(account::table)
                .filter(account::chain_id.eq(chain_id))
                .filter(
                    valid_from.le(version_ts).and(
                        valid_to
                            .gt(version_ts)
                            .or(valid_to.is_null()),
                    ),
                )
                .order_by((account::id, slot, valid_from.desc(), ordinal.desc()))
                .select((account::id, slot, value))
                .distinct_on((account::id, slot))
                .into_boxed();
            if let Some(addresses) = contracts {
                let filter_val: HashSet<_> = addresses.iter().collect();
                q = q.filter(account::address.eq_any(filter_val));
            }
            q.get_results::<(i64, Vec<u8>, Option<Vec<u8>>)>(conn)
                .await?
        };
        let accounts = orm::Account::get_addresses_by_id(slots.iter().map(|(cid, _, _)| cid), conn)
            .await?
            .into_iter()
            .collect::<HashMap<i64, Vec<u8>>>();
        construct_account_to_contract_store(slots.into_iter(), accounts)
    }

    async fn upsert_slots(
        &self,
        slots: SlotChangeSet<'_, Self::Transaction>,
        conn: &mut Self::DB,
    ) -> Result<(), StorageError> {
        let txns: HashSet<_> = slots
            .iter()
            .map(|(tx, _)| tx.hash())
            .collect();
        let tx_ids: HashMap<Vec<u8>, (i64, i64, NaiveDateTime)> = schema::transaction::table
            .inner_join(schema::block::table)
            .filter(schema::transaction::hash.eq_any(txns))
            .select((
                schema::transaction::hash,
                (schema::transaction::id, schema::transaction::index, schema::block::ts),
            ))
            .get_results::<(Vec<u8>, (i64, i64, NaiveDateTime))>(conn)
            .await?
            .into_iter()
            .collect();
        let accounts: HashSet<_> = slots
            .iter()
            .flat_map(|(_, contract_slots)| contract_slots.keys())
            .collect();
        let account_ids: HashMap<Vec<u8>, i64> = schema::account::table
            .filter(schema::account::address.eq_any(accounts))
            .select((schema::account::address, schema::account::id))
            .get_results::<(Vec<u8>, i64)>(conn)
            .await?
            .into_iter()
            .collect();

        let mut new_entries = Vec::new();
        for (tx, contract_storage) in slots.iter() {
            let txhash = tx.hash();
            let (modify_tx, tx_index, block_ts) = tx_ids.get(txhash).ok_or_else(|| {
                StorageError::NoRelatedEntity(
                    "Transaction".into(),
                    "ContractStorage".into(),
                    hex::encode(txhash),
                )
            })?;
            for (address, storage) in contract_storage.iter() {
                let account_id = account_ids
                    .get(address)
                    .ok_or_else(|| {
                        StorageError::NoRelatedEntity(
                            "Account".into(),
                            "ContractStorage".into(),
                            hex::encode(address),
                        )
                    })?;
                for (slot, value) in storage.iter() {
                    new_entries.push(orm::NewSlot {
                        slot,
                        value: value.as_ref(),
                        account_id: *account_id,
                        modify_tx: *modify_tx,
                        ordinal: *tx_index,
                        valid_from: *block_ts,
                    })
                }
            }
        }
        diesel::insert_into(schema::contract_storage::table)
            .values(&new_entries)
            .execute(conn)
            .await?;
        Ok(())
    }

    async fn get_slots_delta(
        &self,
        chain: Chain,
        start_version: Option<&BlockOrTimestamp>,
        target_version: &BlockOrTimestamp,
        conn: &mut AsyncPgConnection,
    ) -> Result<AccountToContractStore, StorageError> {
        let chain_id = self.get_chain_id(chain);
        // To support blocks as versions, we need to ingest all blocks, else the
        // below method can error for any blocks that are not present.
        let start_version_ts = version_to_ts(&start_version, conn).await?;
        let target_version_ts = version_to_ts(&Some(target_version), conn).await?;

        let changed_values = if start_version_ts <= target_version_ts {
            // Going forward
            //                  ]     changes to forward   ]
            // -----------------|--------------------------|
            //                start                     target
            // We query for changes between start and target version. Then sort
            // these by account and slot by change time in a desending matter
            // (latest change first). Next we deduplicate by account and slot.
            // Finally we select the value column to give us the latest value
            // within the version range.
            schema::contract_storage::table
                .inner_join(schema::account::table.inner_join(schema::chain::table))
                .filter(schema::chain::id.eq(chain_id))
                .filter(schema::contract_storage::valid_from.gt(start_version_ts))
                .filter(schema::contract_storage::valid_from.le(target_version_ts))
                .order_by((
                    schema::account::id,
                    schema::contract_storage::slot,
                    schema::contract_storage::valid_from.desc(),
                    schema::contract_storage::ordinal.desc(),
                ))
                .select((
                    schema::account::id,
                    schema::contract_storage::slot,
                    schema::contract_storage::value,
                ))
                .distinct_on((schema::account::id, schema::contract_storage::slot))
                .get_results::<(i64, Vec<u8>, Option<Vec<u8>>)>(conn)
                .await?
        } else {
            // Going backwards
            //                  ]     changes to revert    ]
            // -----------------|--------------------------|
            //                target                     start
            // We query for changes between target and start version. Then sort
            // these for each account and slot by change time in an ascending
            // manner. Next, we deduplicate by taking the first row for each
            // account and slot. Finally we select the previous_value column to
            // give us the value before this first change within the version
            // range.
            schema::contract_storage::table
                .inner_join(schema::account::table.inner_join(schema::chain::table))
                .filter(schema::chain::id.eq(chain_id))
                .filter(schema::contract_storage::valid_from.gt(target_version_ts))
                .filter(schema::contract_storage::valid_from.le(start_version_ts))
                .order_by((
                    schema::account::id.asc(),
                    schema::contract_storage::slot.asc(),
                    schema::contract_storage::valid_from.asc(),
                    schema::contract_storage::ordinal.asc(),
                ))
                .select((
                    schema::account::id,
                    schema::contract_storage::slot,
                    schema::contract_storage::previous_value,
                ))
                .distinct_on((schema::account::id, schema::contract_storage::slot))
                .get_results::<(i64, Vec<u8>, Option<Vec<u8>>)>(conn)
                .await?
        };

        // We retrieve account addresses separately because this is more
        // efficient for the most common cases. In the most common case, only a
        // handful of accounts that we are interested in will have had changes
        // that need to be reverted. The previous query only returns duplicated
        // account ids, which are lighweight (8 byte vs 20 for addresses), once
        // deduplicated we only fetch the associated addresses. These addresses
        // are considered immutable, so if necessary we could event cache these
        // locally.
        // In the worst case each changed slot is changed on a different
        // account. On mainnet that would be at max 300 contracts/slots, which
        // although not ideal is still bearable.
        let account_addresses = schema::account::table
            .filter(
                schema::account::id.eq_any(
                    changed_values
                        .iter()
                        .map(|(cid, _, _)| cid),
                ),
            )
            .select((schema::account::id, schema::account::address))
            .get_results::<(i64, Vec<u8>)>(conn)
            .await
            .map_err(StorageError::from)?
            .into_iter()
            .collect::<HashMap<i64, Vec<u8>>>();

        construct_account_to_contract_store(changed_values.into_iter(), account_addresses)
    }

    async fn revert_contract_state(
        &self,
        to: &BlockIdentifier,
        conn: &mut AsyncPgConnection,
    ) -> Result<(), StorageError> {
        // To revert all changes of a chain, we need to delete & modify entries
        // from a big number of tables. Reverting state, signifies deleting
        // history. We will not keep any branches in the db only the main branch
        // will be kept.
        let block = orm::Block::by_id(to, conn).await?;

        // All entities and version updates are connected to the block via a
        // cascade delete, this ensures that the state is reverted by simply
        // deleting the correct blocks, which then triggers cascading deletes on
        // child entries.
        diesel::delete(
            schema::block::table
                .filter(schema::block::number.gt(block.number))
                .filter(schema::block::chain_id.eq(block.chain_id)),
        )
        .execute(conn)
        .await?;

        // Any versioned table's rows, which have `valid_to` set to "> block.ts"
        // need, to be updated to be valid again (thus, valid_to = NULL).
        diesel::update(
            schema::contract_storage::table.filter(schema::contract_storage::valid_to.gt(block.ts)),
        )
        .set(schema::contract_storage::valid_to.eq(Option::<NaiveDateTime>::None))
        .execute(conn)
        .await?;

        diesel::update(
            schema::account_balance::table.filter(schema::account_balance::valid_to.gt(block.ts)),
        )
        .set(schema::account_balance::valid_to.eq(Option::<NaiveDateTime>::None))
        .execute(conn)
        .await?;

        diesel::update(
            schema::contract_code::table.filter(schema::contract_code::valid_to.gt(block.ts)),
        )
        .set(schema::contract_code::valid_to.eq(Option::<NaiveDateTime>::None))
        .execute(conn)
        .await?;

        diesel::update(
            schema::protocol_state::table.filter(schema::protocol_state::valid_to.gt(block.ts)),
        )
        .set(schema::protocol_state::valid_to.eq(Option::<NaiveDateTime>::None))
        .execute(conn)
        .await?;

        diesel::update(
            schema::protocol_calls_contract::table
                .filter(schema::protocol_calls_contract::valid_to.gt(block.ts)),
        )
        .set(schema::protocol_calls_contract::valid_to.eq(Option::<NaiveDateTime>::None))
        .execute(conn)
        .await?;

        diesel::update(schema::account::table.filter(schema::account::deleted_at.gt(block.ts)))
            .set(schema::account::deleted_at.eq(Option::<NaiveDateTime>::None))
            .execute(conn)
            .await?;

        diesel::update(
            schema::protocol_component::table
                .filter(schema::protocol_component::deleted_at.gt(block.ts)),
        )
        .set(schema::protocol_component::deleted_at.eq(Option::<NaiveDateTime>::None))
        .execute(conn)
        .await?;

        Ok(())
    }
}

/// Parses an evm address hash from the db
///
/// The db id is required to provide additional error context in case the
/// parsing fails.
fn parse_id_h160(v: &[u8]) -> Result<H160, StorageError> {
    if v.len() != 20 {
        return Err(StorageError::DecodeError(format!(
            "Invalid contract address found: {}",
            hex::encode(v)
        )))
    }
    Ok(H160::from_slice(v))
}

fn construct_account_to_contract_store(
    slot_values: impl Iterator<Item = (i64, Vec<u8>, Option<Vec<u8>>)>,
    addresses: HashMap<i64, Vec<u8>>,
) -> Result<AccountToContractStore, StorageError> {
    let mut result: AccountToContractStore = HashMap::with_capacity(addresses.len());
    for (cid, raw_key, raw_val) in slot_values.into_iter() {
        // note this can theoretically happen (only if there is some really
        // bad database inconsistency) because the call above simply filters
        // for account ids, but won't error or give any inidication of a
        // missing contract id.
        let account_address = addresses.get(&cid).ok_or_else(|| {
            StorageError::DecodeError(format!("Failed to find contract address for id {}", cid))
        })?;

        match result.entry(account_address.clone()) {
            Entry::Occupied(mut e) => {
                e.get_mut().insert(raw_key, raw_val);
            }
            Entry::Vacant(e) => {
                let mut contract_storage = HashMap::new();
                contract_storage.insert(raw_key, raw_val);
                e.insert(contract_storage);
            }
        }
    }
    Ok(result)
}

/// Parses a tuple of U256 representing an slot entry
///
/// In case the value is None it will assume a value of zero.
fn parse_u256_slot_entry(
    raw_key: &[u8],
    raw_val: Option<&[u8]>,
) -> Result<(U256, U256), StorageError> {
    if raw_key.len() != 32 {
        return Err(StorageError::DecodeError(format!(
            "Invalid byte length for U256 in slot key! Found: 0x{}",
            hex::encode(raw_key)
        )))
    }
    let v = if let Some(val) = raw_val {
        if val.len() != 32 {
            return Err(StorageError::DecodeError(format!(
                "Invalid byte length for U256 in slot value! Found: 0x{}",
                hex::encode(val)
            )))
        }
        U256::from_big_endian(val)
    } else {
        U256::zero()
    };

    let k = U256::from_big_endian(raw_key);
    Ok((k, v))
}

/// Given a version find the corresponding timestamp.
///
/// If the version is a block, it will query the database for that block and
/// return its timestamp.
///
/// ## Note:
/// This can fail if there is no block present in the db. With the current table
/// schema this means, that there were no changes detected at that block, but
/// there might have been on previous or in later blocks.
async fn version_to_ts(
    start_version: &Option<&Version>,
    conn: &mut AsyncPgConnection,
) -> Result<NaiveDateTime, StorageError> {
    if let Some(Version(version, kind)) = start_version {
        if !matches!(kind, VersionKind::Last) {
            return Err(StorageError::Unsupported(format!("Unsupported version kind: {:?}", kind)))
        }
        match version {
            BlockOrTimestamp::Block(BlockIdentifier::Hash(h)) => Ok(orm::Block::by_hash(&h, conn)
                .await
                .map_err(|err| StorageError::from_diesel(err, "Block", &hex::encode(h), None))?
                .ts),
            BlockOrTimestamp::Block(BlockIdentifier::Number((chain, no))) => {
                Ok(orm::Block::by_number(*chain, *no, conn)
                    .await
                    .map_err(|err| {
                        StorageError::from_diesel(err, "Block", &format!("{}", no), None)
                    })?
                    .ts)
            }
            BlockOrTimestamp::Timestamp(ts) => Ok(*ts),
        }
    } else {
        Ok(Utc::now().naive_utc())
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use diesel_async::{AsyncConnection, RunQueryDsl};
    use ethers::types::H256;
    use rstest::rstest;

    use super::*;
    use crate::{
        extractor::evm::{self, Account},
        storage::postgres::db_fixtures,
    };

    type EvmGateway = PostgresGateway<evm::Block, evm::Transaction, evm::Account>;

    async fn setup_db() -> AsyncPgConnection {
        let db_url = std::env::var("DATABASE_URL").unwrap();
        let mut conn = AsyncPgConnection::establish(&db_url)
            .await
            .unwrap();
        conn.begin_test_transaction()
            .await
            .unwrap();
        conn
    }

    #[tokio::test]
    async fn test_get_account() {
        let mut conn = setup_db().await;
        let acc_address = setup_account(&mut conn).await;
        let code_bytes = hex::decode("1234").unwrap();
        let code_hash = H256::from_slice(&ethers::utils::keccak256(&code_bytes));
        let expected = Account::new(
            Chain::Ethereum,
            H160::from_str("6B175474E89094C44Da98b954EedeAC495271d0F").unwrap(),
            "account0".to_owned(),
            HashMap::new(),
            U256::from(100),
            code_bytes,
            code_hash,
            "0x3108322284d0a89a7accb288d1a94384d499504fe7e04441b0706c7628dee7b7"
                .parse()
                .expect("txhash ok"),
            "0x3108322284d0a89a7accb288d1a94384d499504fe7e04441b0706c7628dee7b7"
                .parse()
                .expect("txhash ok"),
            None,
        );

        let gateway = EvmGateway::from_connection(&mut conn).await;
        let id = ContractId::new(Chain::Ethereum, hex::decode(acc_address).unwrap());
        let actual = gateway
            .get_contract(&id, &None, &mut conn)
            .await
            .unwrap();

        assert_eq!(expected, actual);
    }

    #[tokio::test]
    async fn test_get_missing_account() {
        let mut conn = setup_db().await;
        let gateway = EvmGateway::from_connection(&mut conn).await;
        let contract_id = ContractId::new(
            Chain::Ethereum,
            hex::decode("6B175474E89094C44Da98b954EedeAC495271d0F").unwrap(),
        );
        let result = gateway
            .get_contract(&contract_id, &None, &mut conn)
            .await;
        if let Err(StorageError::NotFound(entity, id)) = result {
            assert_eq!(entity, "Account");
            assert_eq!(id, hex::encode(contract_id.address));
        } else {
            panic!("Expected NotFound error");
        }
    }

    #[tokio::test]
    async fn test_add_contract() {
        let mut conn = setup_db().await;
        let chain_id = db_fixtures::insert_chain(&mut conn, "ethereum").await;
        let blk = db_fixtures::insert_blocks(&mut conn, chain_id).await;
        let txn = db_fixtures::insert_txns(
            &mut conn,
            &[
                (
                    blk[0],
                    1i64,
                    "0xbb7e16d797a9e2fbc537e30f91ed3d27a254dd9578aa4c3af3e5f0d3e8130945",
                ),
                (
                    blk[1],
                    1i64,
                    "0x3108322284d0a89a7accb288d1a94384d499504fe7e04441b0706c7628dee7b7",
                ),
            ],
        )
        .await;
        let code = hex::decode("1234").unwrap();
        let code_hash = H256::from_slice(&ethers::utils::keccak256(&code));
        let expected = Account::new(
            Chain::Ethereum,
            H160::from_str("6B175474E89094C44Da98b954EedeAC495271d0F").unwrap(),
            "NewAccount".to_owned(),
            HashMap::new(),
            U256::from(100),
            code,
            code_hash,
            "0x3108322284d0a89a7accb288d1a94384d499504fe7e04441b0706c7628dee7b7"
                .parse()
                .expect("txhash ok"),
            "0x3108322284d0a89a7accb288d1a94384d499504fe7e04441b0706c7628dee7b7"
                .parse()
                .expect("txhash ok"),
            Some(
                H256::from_str(
                    "0x3108322284d0a89a7accb288d1a94384d499504fe7e04441b0706c7628dee7b7",
                )
                .unwrap(),
            ),
        );
        let gateway = EvmGateway::from_connection(&mut conn).await;

        gateway
            .add_contract(&expected, &mut conn)
            .await
            .unwrap();

        let contract_id = ContractId::new(
            Chain::Ethereum,
            hex::decode("6B175474E89094C44Da98b954EedeAC495271d0F").unwrap(),
        );
        let actual = gateway
            .get_contract(&contract_id, &None, &mut conn)
            .await
            .unwrap();
        assert_eq!(expected, actual);

        let orm_account = orm::Account::by_id(&contract_id, &mut conn)
            .await
            .unwrap();
        let (block_ts, _tx_ts) = schema::transaction::table
            .inner_join(schema::block::table)
            .filter(schema::transaction::id.eq(txn[1]))
            .select((schema::block::ts, schema::transaction::inserted_ts))
            .first::<(NaiveDateTime, NaiveDateTime)>(&mut conn)
            .await
            .unwrap();
        assert_eq!(block_ts, orm_account.created_at.unwrap());
    }

    #[tokio::test]
    async fn test_upsert_contract() {}

    type MaybeTS = Option<NaiveDateTime>;

    #[tokio::test]
    async fn test_delete_contract() {
        let mut conn = setup_db().await;
        let address = setup_account(&mut conn).await;
        let deletion_txhash = "36984d97c02a98614086c0f9e9c4e97f7e0911f6f136b3c8a76d37d6d524d1e5";
        let address_bytes = hex::decode(address).expect("address ok");
        let id = ContractId::new(Chain::Ethereum, address_bytes.clone());
        let gw = EvmGateway::from_connection(&mut conn).await;
        let tx = evm::Transaction {
            hash: deletion_txhash
                .parse()
                .expect("txhash ok"),
            ..Default::default()
        };
        let (block_id, block_ts) = schema::block::table
            .select((schema::block::id, schema::block::ts))
            .first::<(i64, NaiveDateTime)>(&mut conn)
            .await
            .expect("blockquery succeeded");
        db_fixtures::insert_txns(&mut conn, &[(block_id, 12, deletion_txhash)]).await;

        gw.delete_contract(&id, &tx, &mut conn)
            .await
            .unwrap();

        let res = schema::account::table
            .inner_join(schema::account_balance::table)
            .inner_join(schema::contract_code::table)
            .filter(schema::account::address.eq(address_bytes))
            .select((
                schema::account::deleted_at,
                schema::account_balance::valid_to,
                schema::contract_code::valid_to,
            ))
            .first::<(MaybeTS, MaybeTS, MaybeTS)>(&mut conn)
            .await
            .expect("retrieval query ok");
        assert_eq!(res, (Some(block_ts), Some(block_ts), Some(block_ts)));
    }

    fn bytes32(v: u8) -> Vec<u8> {
        let mut arr = [0; 32];
        arr[31] = v;
        arr.to_vec()
    }

    #[rstest]
    #[case::latest(
        None,
        None,
        [(
            hex::decode("73bce791c239c8010cd3c857d96580037ccdd0ee")
                .unwrap(),
            vec![
                (bytes32(1u8), Some(bytes32(255u8))),
                (bytes32(0u8), Some(bytes32(128u8))),
            ]
            .into_iter()
            .collect(),
        ),
        (
            hex::decode("6b175474e89094c44da98b954eedeac495271d0f")
                .unwrap(),
            vec![
                (bytes32(1u8), Some(bytes32(3u8))),
                (bytes32(5u8), Some(bytes32(25u8))),
                (bytes32(2u8), Some(bytes32(1u8))),
                (bytes32(6u8), Some(bytes32(30u8))),
                (bytes32(0u8), Some(bytes32(2u8))),
            ]
            .into_iter()
            .collect(),
        )]
        .into_iter()
        .collect())
    ]
    #[case::latest_only_c0(
        None,
        Some(vec![hex::decode("73bce791c239c8010cd3c857d96580037ccdd0ee").unwrap()]), 
        [(
            hex::decode("73bce791c239c8010cd3c857d96580037ccdd0ee")
                .unwrap(),
            vec![
                (bytes32(1u8), Some(bytes32(255u8))),
                (bytes32(0u8), Some(bytes32(128u8))),
            ]
            .into_iter()
            .collect(),
        )]
        .into_iter()
        .collect())
    ]
    #[case::at_block_one(
        Some(Version(BlockOrTimestamp::Block(BlockIdentifier::Number((Chain::Ethereum, 1))), VersionKind::Last)),
        None,
        [(
            hex::decode("6b175474e89094c44da98b954eedeac495271d0f")
                .unwrap(),
            vec![
                (bytes32(1u8), Some(bytes32(5u8))),
                (bytes32(2u8), Some(bytes32(1u8))),
                (bytes32(0u8), Some(bytes32(1u8))),
            ]
            .into_iter()
            .collect(),
        ),
        (
            hex::decode("94a3F312366b8D0a32A00986194053C0ed0CdDb1").unwrap(), 
            vec![
                (bytes32(1u8), Some(bytes32(2u8))),
                (bytes32(2u8), Some(bytes32(4u8)))
            ]
            .into_iter()
            .collect(),
        )]
        .into_iter()
        .collect()
    )]
    #[case::before_block_one(
        Some(Version(BlockOrTimestamp::Timestamp("2019-01-01T00:00:00".parse().unwrap()), VersionKind::Last)),
        None,
        HashMap::new())
    ]
    #[tokio::test]
    async fn test_get_slots(
        #[case] version: Option<Version>,
        #[case] addresses: Option<Vec<Vec<u8>>>,
        #[case] exp: AccountToContractStore,
    ) {
        let mut conn = setup_db().await;
        setup_revert(&mut conn).await;
        let gw = EvmGateway::from_connection(&mut conn).await;

        let addresses_slice = addresses.as_ref().map(|outer| {
            outer
                .iter()
                .map(|inner| inner.as_slice())
                .collect::<Vec<_>>()
        });
        let res = gw
            .get_contract_slots(
                Chain::Ethereum,
                addresses_slice.as_deref(),
                version.as_ref(),
                &mut conn,
            )
            .await
            .unwrap();

        assert_eq!(res, exp);
    }

    #[tokio::test]
    async fn test_upsert_slots() {
        let mut conn = setup_db().await;
        let chain_id = db_fixtures::insert_chain(&mut conn, "ethereum").await;
        let blk = db_fixtures::insert_blocks(&mut conn, chain_id).await;
        let txn = db_fixtures::insert_txns(
            &mut conn,
            &[(blk[0], 1i64, "0xbb7e16d797a9e2fbc537e30f91ed3d27a254dd9578aa4c3af3e5f0d3e8130945")],
        )
        .await;
        db_fixtures::insert_account(
            &mut conn,
            "6B175474E89094C44Da98b954EedeAC495271d0F",
            "Account1",
            chain_id,
            Some(txn[0]),
        )
        .await;
        let slot_data: ContractStore = vec![
            (vec![1u8], Some(vec![10u8])),
            (vec![2u8], Some(vec![20u8])),
            (vec![3u8], Some(vec![30u8])),
        ]
        .into_iter()
        .collect();
        let input_slots = vec![(
            evm::Transaction {
                hash: H256::from_str(
                    "0xbb7e16d797a9e2fbc537e30f91ed3d27a254dd9578aa4c3af3e5f0d3e8130945",
                )
                .expect("hash ok"),
                ..Default::default()
            },
            vec![(
                hex::decode("6B175474E89094C44Da98b954EedeAC495271d0F")
                    .expect("account address ok"),
                slot_data.clone(),
            )]
            .into_iter()
            .collect(),
        )];

        let gw = EvmGateway::from_connection(&mut conn).await;

        gw.upsert_slots(&input_slots, &mut conn)
            .await
            .unwrap();

        // Query the stored slots from the database
        let fetched_slot_data: ContractStore = schema::contract_storage::table
            .select((schema::contract_storage::slot, schema::contract_storage::value))
            .get_results(&mut conn)
            .await
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(slot_data, fetched_slot_data);
    }

    async fn setup_account(conn: &mut AsyncPgConnection) -> String {
        // Adds fixtures: chain, block, transaction, account, account_balance
        let acc_address = "6B175474E89094C44Da98b954EedeAC495271d0F";
        let chain_id = db_fixtures::insert_chain(conn, "ethereum").await;
        let blk = db_fixtures::insert_blocks(conn, chain_id).await;
        let tx_ids = db_fixtures::insert_txns(
            conn,
            &[
                (
                    blk[0],
                    1i64,
                    "0xbb7e16d797a9e2fbc537e30f91ed3d27a254dd9578aa4c3af3e5f0d3e8130945",
                ),
                (
                    blk[1],
                    1i64,
                    "0x3108322284d0a89a7accb288d1a94384d499504fe7e04441b0706c7628dee7b7",
                ),
            ],
        )
        .await;
        // Insert account and balances
        let acc_id =
            db_fixtures::insert_account(conn, acc_address, "account0", chain_id, None).await;
        db_fixtures::insert_account_balances(conn, tx_ids[1], acc_id).await;
        let contract_code = hex::decode("1234").unwrap();
        db_fixtures::insert_contract_code(conn, acc_id, tx_ids[1], contract_code).await;
        acc_address.to_string()
    }

    async fn setup_slots_delta(conn: &mut AsyncPgConnection) {
        let chain_id = db_fixtures::insert_chain(conn, "ethereum").await;
        let blk = db_fixtures::insert_blocks(conn, chain_id).await;
        let txn = db_fixtures::insert_txns(
            conn,
            &[
                (
                    blk[0],
                    1i64,
                    "0xbb7e16d797a9e2fbc537e30f91ed3d27a254dd9578aa4c3af3e5f0d3e8130945",
                ),
                (
                    blk[1],
                    1i64,
                    "0x3108322284d0a89a7accb288d1a94384d499504fe7e04441b0706c7628dee7b7",
                ),
            ],
        )
        .await;
        let c0 = db_fixtures::insert_account(
            conn,
            "6B175474E89094C44Da98b954EedeAC495271d0F",
            "c0",
            chain_id,
            Some(txn[0]),
        )
        .await;
        db_fixtures::insert_slots(
            conn,
            c0,
            txn[0],
            "2020-01-01T00:00:00",
            &[(0, 1), (1, 5), (2, 1)],
        )
        .await;
        db_fixtures::insert_slots(
            conn,
            c0,
            txn[1],
            "2020-01-01T01:00:00",
            &[(0, 2), (1, 3), (5, 25), (6, 30)],
        )
        .await;
    }

    #[tokio::test]
    async fn get_slots_delta_forward() {
        let mut conn = setup_db().await;
        setup_slots_delta(&mut conn).await;
        let gw = EvmGateway::from_connection(&mut conn).await;
        let storage: ContractStore = vec![(0u8, 2u8), (1u8, 3u8), (5u8, 25u8), (6u8, 30u8)]
            .into_iter()
            .map(|(k, v)| if v > 0 { (bytes32(k), Some(bytes32(v))) } else { (bytes32(k), None) })
            .collect();
        let mut exp = HashMap::new();
        let addr = hex::decode("6B175474E89094C44Da98b954EedeAC495271d0F").unwrap();
        exp.insert(addr, storage);

        let res = gw
            .get_slots_delta(
                Chain::Ethereum,
                Some(&BlockOrTimestamp::Timestamp(
                    "2020-01-01T00:00:00"
                        .parse::<NaiveDateTime>()
                        .unwrap(),
                )),
                &BlockOrTimestamp::Timestamp(
                    "2020-01-01T02:00:00"
                        .parse::<NaiveDateTime>()
                        .unwrap(),
                ),
                &mut conn,
            )
            .await
            .unwrap();

        assert_eq!(res, exp);
    }

    #[tokio::test]
    async fn get_slots_delta_backward() {
        let mut conn = setup_db().await;
        setup_slots_delta(&mut conn).await;
        let gw = EvmGateway::from_connection(&mut conn).await;
        let storage: ContractStore = vec![(0u8, 1u8), (1u8, 5u8), (5u8, 0u8), (6u8, 0u8)]
            .into_iter()
            .map(|(k, v)| if v > 0 { (bytes32(k), Some(bytes32(v))) } else { (bytes32(k), None) })
            .collect();
        let mut exp = HashMap::new();
        let addr = hex::decode("6B175474E89094C44Da98b954EedeAC495271d0F").unwrap();
        exp.insert(addr, storage);

        let res = gw
            .get_slots_delta(
                Chain::Ethereum,
                Some(&BlockOrTimestamp::Timestamp(
                    "2020-01-01T02:00:00"
                        .parse::<NaiveDateTime>()
                        .unwrap(),
                )),
                &BlockOrTimestamp::Timestamp(
                    "2020-01-01T00:00:00"
                        .parse::<NaiveDateTime>()
                        .unwrap(),
                ),
                &mut conn,
            )
            .await
            .unwrap();

        assert_eq!(res, exp);
    }

    async fn setup_revert(conn: &mut AsyncPgConnection) {
        let chain_id = db_fixtures::insert_chain(conn, "ethereum").await;
        let blk = db_fixtures::insert_blocks(conn, chain_id).await;
        let txn = db_fixtures::insert_txns(
            conn,
            &[
                (
                    // deploy c0
                    blk[0],
                    1i64,
                    "0xbb7e16d797a9e2fbc537e30f91ed3d27a254dd9578aa4c3af3e5f0d3e8130945",
                ),
                (
                    // change c0 state, deploy c2
                    blk[0],
                    2i64,
                    "0x794f7df7a3fe973f1583fbb92536f9a8def3a89902439289315326c04068de54",
                ),
                (
                    // deploy c1, delete c2
                    blk[1],
                    1i64,
                    "0x3108322284d0a89a7accb288d1a94384d499504fe7e04441b0706c7628dee7b7",
                ),
                (
                    // change c0 and c1 state
                    blk[1],
                    2i64,
                    "0x50449de1973d86f21bfafa7c72011854a7e33a226709dc3e2e4edcca34188388",
                ),
            ],
        )
        .await;
        let c0 = db_fixtures::insert_account(
            conn,
            "6B175474E89094C44Da98b954EedeAC495271d0F",
            "c0",
            chain_id,
            Some(txn[0]),
        )
        .await;
        let c1 = db_fixtures::insert_account(
            conn,
            "73BcE791c239c8010Cd3C857d96580037CCdd0EE",
            "c1",
            chain_id,
            Some(txn[2]),
        )
        .await;
        let c2 = db_fixtures::insert_account(
            conn,
            "94a3F312366b8D0a32A00986194053C0ed0CdDb1",
            "c2",
            chain_id,
            Some(txn[1]),
        )
        .await;
        db_fixtures::insert_slots(
            conn,
            c0,
            txn[1],
            "2020-01-01T00:00:00",
            &[(0, 1), (1, 5), (2, 1)],
        )
        .await;
        db_fixtures::insert_slots(conn, c2, txn[1], "2020-01-01T00:00:00", &[(1, 2), (2, 4)]).await;
        db_fixtures::delete_account(conn, c2, "2020-01-01T01:00:00").await;
        db_fixtures::insert_slots(
            conn,
            c0,
            txn[3],
            "2020-01-01T01:00:00",
            &[(0, 2), (1, 3), (5, 25), (6, 30)],
        )
        .await;
        db_fixtures::insert_slots(conn, c1, txn[3], "2020-01-01T01:00:00", &[(0, 128), (1, 255)])
            .await;
    }

    #[tokio::test]
    async fn test_revert() {
        let mut conn = setup_db().await;
        setup_revert(&mut conn).await;
        let block1_hash =
            H256::from_str("0x88e96d4537bea4d9c05d12549907b32561d3bf31f45aae734cdc119f13406cb6")
                .unwrap()
                .as_bytes()
                .into();
        let c0_address =
            hex::decode("6B175474E89094C44Da98b954EedeAC495271d0F").expect("c0 address valid");
        let exp_slots: HashMap<U256, U256> = vec![
            (U256::from(0), U256::from(1)),
            (U256::from(1), U256::from(5)),
            (U256::from(2), U256::from(1)),
        ]
        .into_iter()
        .collect();
        let gw = EvmGateway::from_connection(&mut conn).await;

        gw.revert_contract_state(&BlockIdentifier::Hash(block1_hash), &mut conn)
            .await
            .unwrap();

        let slots: HashMap<U256, U256> = schema::contract_storage::table
            .inner_join(schema::account::table)
            .filter(schema::account::address.eq(c0_address))
            .select((schema::contract_storage::slot, schema::contract_storage::value))
            .get_results::<(Vec<u8>, Option<Vec<u8>>)>(&mut conn)
            .await
            .unwrap()
            .iter()
            .map(|(k, v)| {
                (
                    U256::from_big_endian(k),
                    v.as_ref()
                        .map(|rv| U256::from_big_endian(rv))
                        .unwrap_or_else(U256::zero),
                )
            })
            .collect();
        assert_eq!(slots, exp_slots);

        let c1 = schema::account::table
            .filter(schema::account::title.eq("c1"))
            .select(schema::account::id)
            .get_results::<i64>(&mut conn)
            .await
            .unwrap();
        assert_eq!(c1.len(), 0);
    }
}
