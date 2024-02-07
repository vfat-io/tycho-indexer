#![allow(unused_variables)]
use async_trait::async_trait;
use chrono::{NaiveDateTime, Utc};
use diesel::prelude::*;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use itertools::Itertools;
use std::{
    cmp::Ordering,
    collections::{BTreeSet, HashMap, HashSet},
};
use tracing::{instrument, warn};

use crate::{
    extractor::evm::{ComponentBalance, ProtocolComponent, ProtocolState, ProtocolStateDelta},
    models::{Chain, ProtocolType},
    storage::{
        postgres::{
            orm,
            orm::{Account, ComponentTVL, NewAccount},
            schema,
            versioning::apply_delta_versioning,
            PostgresGateway,
        },
        Address, Balance, BlockOrTimestamp, ComponentId, ContractDelta, ContractId,
        ProtocolGateway, StorableBlock, StorableComponentBalance, StorableContract,
        StorableProtocolComponent, StorableProtocolState, StorableProtocolStateDelta,
        StorableProtocolType, StorableToken, StorableTransaction, StorageError, StoreVal, TxHash,
        Version,
    },
};
use tycho_types::Bytes;

use super::WithTxHash;

// Private methods
impl<B, TX, A, D, T> PostgresGateway<B, TX, A, D, T>
where
    B: StorableBlock<orm::Block, orm::NewBlock, i64>,
    TX: StorableTransaction<orm::Transaction, orm::NewTransaction, i64>,
    D: ContractDelta + From<A>,
    A: StorableContract<orm::Contract, orm::NewContract, i64>,
    T: StorableToken<orm::Token, orm::NewToken, i64>,
{
    /// # Decoding ProtocolStates from database results.
    ///
    /// This function takes as input the database result for querying protocol states and their
    /// linked component id and transaction hash.
    ///
    /// ## Assumptions:
    /// - It is assumed that the rows in the result are ordered by:
    ///     1. Component ID,
    ///     2. Transaction block, and then
    ///     3. Transaction index.
    ///
    /// The function processes these individual `ProtocolState` entities and combines all entities
    /// with matching component IDs into a single `ProtocolState`. The final output is a list
    /// where each element is a `ProtocolState` representing a unique component.
    ///
    /// ## Returns:
    /// - A Result containing a vector of `ProtocolState`, otherwise, it will return a StorageError.
    fn _decode_protocol_states(
        &self,
        result: Result<Vec<(orm::ProtocolState, ComponentId, StoreVal)>, diesel::result::Error>,
        context: &str,
    ) -> Result<Vec<ProtocolState>, StorageError> {
        match result {
            Ok(data_vec) => {
                // Decode final state deltas. We can assume result is sorted by component_id and
                // transaction index. Therefore we can use slices to iterate over the data in groups
                // of component_id. The last update for each component will have the latest
                // transaction hash (modify_tx).

                let mut protocol_states = Vec::new();

                let mut index = 0;
                while index < data_vec.len() {
                    let component_start = index;
                    let current_component_id = &data_vec[index].1;

                    // Iterate until the component_id changes
                    while index < data_vec.len() && &data_vec[index].1 == current_component_id {
                        index += 1;
                    }

                    let states_slice = &data_vec[component_start..index];
                    let tx_hash = &states_slice
                        .last()
                        .ok_or(StorageError::Unexpected(
                            "Could not get tx_hash from ProtocolState".to_string(),
                        ))?
                        .2; // Last element has the latest transaction

                    let protocol_state = ProtocolState::from_storage(
                        states_slice
                            .iter()
                            .map(|x| x.0.clone())
                            .collect(),
                        current_component_id.clone(),
                        tx_hash,
                    )?;

                    protocol_states.push(protocol_state);
                }
                Ok(protocol_states)
            }

            Err(err) => Err(StorageError::from_diesel(err, "ProtocolStates", context, None)),
        }
    }

    async fn _get_or_create_protocol_system_id(
        &self,
        new: String,
        conn: &mut <PostgresGateway<B, TX, A, D, T> as ProtocolGateway>::DB,
    ) -> Result<i64, StorageError> {
        use super::schema::protocol_system::dsl::*;

        let existing_entry = protocol_system
            .filter(name.eq(new.to_string().clone()))
            .first::<orm::ProtocolSystem>(conn)
            .await;

        if let Ok(entry) = existing_entry {
            return Ok(entry.id);
        } else {
            let new_entry = orm::NewProtocolSystem { name: new.to_string() };

            let inserted_protocol_system = diesel::insert_into(protocol_system)
                .values(&new_entry)
                .get_result::<orm::ProtocolSystem>(conn)
                .await
                .map_err(|err| {
                    StorageError::from_diesel(err, "ProtocolSystem", &new.to_string(), None)
                })?;
            Ok(inserted_protocol_system.id)
        }
    }
}

#[async_trait]
impl<B, TX, A, D, T> ProtocolGateway for PostgresGateway<B, TX, A, D, T>
where
    B: StorableBlock<orm::Block, orm::NewBlock, i64>,
    TX: StorableTransaction<orm::Transaction, orm::NewTransaction, i64>,
    D: ContractDelta + From<A>,
    A: StorableContract<orm::Contract, orm::NewContract, i64>,
    T: StorableToken<orm::Token, orm::NewToken, i64>,
{
    type DB = AsyncPgConnection;
    type Token = T;
    type ProtocolState = ProtocolState;
    type ProtocolStateDelta = ProtocolStateDelta;
    type ProtocolType = ProtocolType;
    type ProtocolComponent = ProtocolComponent;
    type ComponentBalance = ComponentBalance;

    async fn get_protocol_components(
        &self,
        chain: &Chain,
        system: Option<String>,
        ids: Option<&[&str]>,
        min_tvl: Option<f64>,
        conn: &mut Self::DB,
    ) -> Result<Vec<ProtocolComponent>, StorageError> {
        use super::schema::{protocol_component::dsl::*, transaction::dsl::*};
        let chain_id_value = self.get_chain_id(chain);

        let mut query = protocol_component
            .inner_join(transaction.on(creation_tx.eq(schema::transaction::id)))
            .left_join(schema::component_tvl::table)
            .select((orm::ProtocolComponent::as_select(), hash))
            .into_boxed();

        match (system, ids) {
            (Some(ps), None) => {
                let protocol_system = self.get_protocol_system_id(&ps);
                query = query.filter(
                    chain_id
                        .eq(chain_id_value)
                        .and(protocol_system_id.eq(protocol_system)),
                );
            }
            (None, Some(external_ids)) => {
                query = query.filter(
                    chain_id
                        .eq(chain_id_value)
                        .and(external_id.eq_any(external_ids)),
                );
            }
            (Some(ps), Some(external_ids)) => {
                let protocol_system = self.get_protocol_system_id(&ps);
                query = query.filter(
                    chain_id.eq(chain_id_value).and(
                        external_id
                            .eq_any(external_ids)
                            .and(protocol_system_id.eq(protocol_system)),
                    ),
                );
            }
            (_, _) => {
                query = query.filter(chain_id.eq(chain_id_value));
            }
        }

        if let Some(thr) = min_tvl {
            query = query.filter(schema::component_tvl::tvl.gt(thr));
        }

        let orm_protocol_components = query
            .load::<(orm::ProtocolComponent, TxHash)>(conn)
            .await?;

        let protocol_component_ids = orm_protocol_components
            .iter()
            .map(|(pc, _)| pc.id)
            .collect::<Vec<i64>>();

        let protocol_component_tokens: Vec<(i64, Address)> =
            schema::protocol_component_holds_token::table
                .inner_join(schema::token::table)
                .inner_join(
                    schema::account::table.on(schema::token::account_id.eq(schema::account::id)),
                )
                .select((
                    schema::protocol_component_holds_token::protocol_component_id,
                    schema::account::address,
                ))
                .filter(
                    schema::protocol_component_holds_token::protocol_component_id
                        .eq_any(protocol_component_ids.clone()),
                )
                .load::<(i64, Address)>(conn)
                .await?;

        let protocol_component_contracts: Vec<(i64, Address)> =
            schema::protocol_component_holds_contract::table
                .inner_join(schema::contract_code::table)
                .inner_join(
                    schema::account::table
                        .on(schema::contract_code::account_id.eq(schema::account::id)),
                )
                .select((
                    schema::protocol_component_holds_contract::protocol_component_id,
                    schema::account::address,
                ))
                .filter(
                    schema::protocol_component_holds_contract::protocol_component_id
                        .eq_any(protocol_component_ids),
                )
                .load::<(i64, Address)>(conn)
                .await?;

        fn map_addresses_to_protocol_component(
            protocol_component_to_address: Vec<(i64, Address)>,
        ) -> HashMap<i64, Vec<Address>> {
            protocol_component_to_address
                .into_iter()
                .fold(HashMap::new(), |mut acc, (key, address)| {
                    acc.entry(key)
                        .or_default()
                        .push(address);
                    acc
                })
        }
        let protocol_component_tokens =
            map_addresses_to_protocol_component(protocol_component_tokens);
        let protocol_component_contracts =
            map_addresses_to_protocol_component(protocol_component_contracts);

        orm_protocol_components
            .into_iter()
            .map(|(pc, tx_hash)| {
                let ps = self.get_protocol_system(&pc.protocol_system_id);
                let tokens_by_pc: &Vec<Address> = protocol_component_tokens
                    .get(&pc.id)
                    .expect("Could not find Tokens for Protocol Component."); // We expect all protocol components to have tokens.
                let contracts_by_pc: &Vec<Address> = protocol_component_contracts
                    .get(&pc.id)
                    .expect("Could not find Contracts for Protocol Component."); // We expect all protocol components to have contracts.

                ProtocolComponent::from_storage(
                    pc.clone(),
                    tokens_by_pc,
                    contracts_by_pc,
                    chain.to_owned(),
                    &ps,
                    tx_hash.into(),
                )
            })
            .collect::<Result<Vec<ProtocolComponent>, StorageError>>()
    }

    async fn add_protocol_components(
        &self,
        new: &[&Self::ProtocolComponent],
        conn: &mut Self::DB,
    ) -> Result<(), StorageError> {
        use super::schema::{
            account::dsl::*, protocol_component::dsl::*, protocol_component_holds_contract::dsl::*,
            protocol_component_holds_token::dsl::*, token::dsl::*,
        };
        let mut values: Vec<orm::NewProtocolComponent> = Vec::with_capacity(new.len());
        let tx_hashes: Vec<TxHash> = new
            .iter()
            .map(|pc| pc.creation_tx.into())
            .collect();
        let tx_hash_id_mapping: HashMap<TxHash, i64> =
            orm::Transaction::ids_by_hash(&tx_hashes, conn).await?;
        let pt_id = orm::ProtocolType::id_by_name(&new[0].protocol_type_name, conn)
            .await
            .map_err(|err| {
                StorageError::from_diesel(err, "ProtocolType", &new[0].protocol_type_name, None)
            })?;
        for pc in new {
            let txh = tx_hash_id_mapping
                .get::<TxHash>(&pc.creation_tx.into())
                .ok_or(StorageError::DecodeError("TxHash not found".to_string()))?;

            let new_pc = pc.to_storage(
                self.get_chain_id(&pc.chain),
                self.get_protocol_system_id(&pc.protocol_system.to_string()),
                pt_id,
                txh.to_owned(),
                pc.created_at,
            )?;
            values.push(new_pc);
        }

        let inserted_protocol_components: Vec<(i64, String, i64, i64)> =
            diesel::insert_into(protocol_component)
                .values(&values)
                .on_conflict((
                    schema::protocol_component::chain_id,
                    protocol_system_id,
                    external_id,
                ))
                .do_nothing()
                .returning((
                    schema::protocol_component::id,
                    schema::protocol_component::external_id,
                    schema::protocol_component::protocol_system_id,
                    schema::protocol_component::chain_id,
                ))
                .get_results(conn)
                .await
                .map_err(|err| {
                    StorageError::from_diesel(err, "ProtocolComponent", "Batch insert", None)
                })?;

        let mut protocol_db_id_map = HashMap::new();
        for (pc_id, ex_id, ps_id, chain_id_db) in inserted_protocol_components {
            protocol_db_id_map.insert(
                (ex_id, self.get_protocol_system(&ps_id), self.get_chain(&chain_id_db)),
                pc_id,
            );
        }

        let filtered_new_protocol_components: Vec<&&Self::ProtocolComponent> = new
            .iter()
            .filter(|component| {
                let key =
                    (component.id.clone(), component.protocol_system.clone(), component.chain);

                protocol_db_id_map.get(&key).is_some()
            })
            .collect();

        // establish component-token junction
        let token_addresses: HashSet<Address> = filtered_new_protocol_components
            .iter()
            .flat_map(|pc| pc.get_byte_token_addresses())
            .collect();

        let pc_tokens_map = filtered_new_protocol_components
            .iter()
            .flat_map(|pc| {
                let pc_id = protocol_db_id_map
                    .get(&(pc.id.clone(), pc.protocol_system.clone(), pc.chain))
                    .expect("Could not find Protocol Component."); //Because we just inserted the protocol systems, there should not be any missing.
                                                                   // However, trying to handle this via Results is needlessly difficult, because you
                                                                   // can not use flat_map on a Result.

                pc.get_byte_token_addresses()
                    .into_iter()
                    .map(move |add| (*pc_id, add))
                    .collect::<Vec<(i64, Address)>>()
            })
            .collect::<Vec<(i64, Address)>>();

        let token_add_by_id: HashMap<Address, i64> = token
            .inner_join(account)
            .select((schema::account::address, schema::token::id))
            .filter(schema::account::address.eq_any(token_addresses))
            .into_boxed()
            .load::<(Address, i64)>(conn)
            .await
            .map_err(|err| StorageError::from_diesel(err, "Token", "Several Chains", None))?
            .into_iter()
            .collect();

        let protocol_component_token_junction: Result<
            Vec<orm::NewProtocolComponentHoldsToken>,
            StorageError,
        > = pc_tokens_map
            .iter()
            .map(|(pc_id, t_address)| {
                let t_id = token_add_by_id
                    .get(t_address)
                    .ok_or(StorageError::NotFound("Token id".to_string(), t_address.to_string()))?;
                Ok(orm::NewProtocolComponentHoldsToken {
                    protocol_component_id: *pc_id,
                    token_id: *t_id,
                })
            })
            .collect();

        diesel::insert_into(protocol_component_holds_token)
            .values(&protocol_component_token_junction?)
            .execute(conn)
            .await?;

        // establish component-contract junction
        let contract_addresses: HashSet<Address> = new
            .iter()
            .flat_map(|pc| pc.get_byte_contract_addresses())
            .collect();

        let pc_contract_map = new
            .iter()
            .flat_map(|pc| {
                let pc_id = protocol_db_id_map
                    .get(&(pc.id.clone(), pc.protocol_system.clone(), pc.chain))
                    .expect("Could not find Protocol Component."); //Because we just inserted the protocol systems, there should not be any missing.
                                                                   // However, trying to handel this via Results is needlessly difficult, because you
                                                                   // can not use flat_map on a Result.

                pc.get_byte_contract_addresses()
                    .into_iter()
                    .map(move |add| (*pc_id, add))
                    .collect::<Vec<(i64, Address)>>()
            })
            .collect::<Vec<(i64, Address)>>();

        let contract_add_by_id: HashMap<Address, i64> = schema::contract_code::table
            .inner_join(account)
            .select((schema::account::address, schema::contract_code::id))
            .filter(schema::account::address.eq_any(contract_addresses))
            .into_boxed()
            .load::<(Address, i64)>(conn)
            .await
            .map_err(|err| StorageError::from_diesel(err, "Contract", "Several Chains", None))?
            .into_iter()
            .collect();

        let protocol_component_contract_junction: Result<
            Vec<orm::NewProtocolComponentHoldsContract>,
            StorageError,
        > = pc_contract_map
            .iter()
            .map(|(pc_id, t_address)| {
                let t_id = contract_add_by_id
                    .get(t_address)
                    .ok_or(StorageError::NotFound("".to_string(), "".to_string()))?;
                Ok(orm::NewProtocolComponentHoldsContract {
                    protocol_component_id: *pc_id,
                    contract_code_id: *t_id,
                })
            })
            .collect();

        diesel::insert_into(protocol_component_holds_contract)
            .values(&protocol_component_contract_junction?)
            .execute(conn)
            .await?;

        Ok(())
    }

    async fn delete_protocol_components(
        &self,
        to_delete: &[&Self::ProtocolComponent],
        block_ts: NaiveDateTime,
        conn: &mut Self::DB,
    ) -> Result<(), StorageError> {
        use super::schema::protocol_component::dsl::*;

        let ids_to_delete: Vec<String> = to_delete
            .iter()
            .map(|c| c.id.to_string())
            .collect();

        diesel::update(protocol_component.filter(external_id.eq_any(ids_to_delete)))
            .set(deleted_at.eq(block_ts))
            .execute(conn)
            .await?;
        Ok(())
    }
    async fn add_protocol_types(
        &self,
        new_protocol_types: &[Self::ProtocolType],
        conn: &mut Self::DB,
    ) -> Result<(), StorageError> {
        use super::schema::protocol_type::dsl::*;
        let values: Vec<orm::NewProtocolType> = new_protocol_types
            .iter()
            .map(|new_protocol_type| new_protocol_type.to_storage())
            .collect();

        diesel::insert_into(protocol_type)
            .values(&values)
            .on_conflict(name)
            .do_nothing()
            .execute(conn)
            .await
            .map_err(|err| StorageError::from_diesel(err, "ProtocolType", "Batch insert", None))?;

        Ok(())
    }

    // Gets all protocol states from the db filtered by chain, component ids and/or protocol system.
    // The filters are applied in the following order: component ids, protocol system, chain. If
    // component ids are provided, the protocol system filter is ignored. The chain filter is
    // always applied.
    async fn get_protocol_states(
        &self,
        chain: &Chain,
        at: Option<Version>,
        system: Option<String>,
        ids: Option<&[&str]>,
        conn: &mut Self::DB,
    ) -> Result<Vec<Self::ProtocolState>, StorageError> {
        let chain_db_id = self.get_chain_id(chain);
        let version_ts = match &at {
            Some(version) => Some(version.to_ts(conn).await?),
            None => None,
        };

        match (ids, system) {
            (Some(ids), Some(_)) => {
                warn!("Both protocol IDs and system were provided. System will be ignored.");
                self._decode_protocol_states(
                    orm::ProtocolState::by_id(ids, chain_db_id, version_ts, conn).await,
                    ids.join(",").as_str(),
                )
            }
            (Some(ids), _) => self._decode_protocol_states(
                orm::ProtocolState::by_id(ids, chain_db_id, version_ts, conn).await,
                ids.join(",").as_str(),
            ),
            (_, Some(system)) => self._decode_protocol_states(
                orm::ProtocolState::by_protocol_system(
                    system.clone(),
                    chain_db_id,
                    version_ts,
                    conn,
                )
                .await,
                system.to_string().as_str(),
            ),
            _ => self._decode_protocol_states(
                orm::ProtocolState::by_chain(chain_db_id, version_ts, conn).await,
                chain.to_string().as_str(),
            ),
        }
    }

    async fn update_protocol_states(
        &self,
        chain: &Chain,
        new: &[(TxHash, &ProtocolStateDelta)],
        conn: &mut Self::DB,
    ) -> Result<(), StorageError> {
        let chain_db_id = self.get_chain_id(chain);
        let new = new
            .iter()
            .map(|(tx, delta)| WithTxHash { entity: delta, tx: Some(tx.to_owned()) })
            .collect::<Vec<_>>();

        let txns: HashMap<Bytes, (i64, i64, NaiveDateTime)> = orm::Transaction::ids_and_ts_by_hash(
            new.iter()
                .filter_map(|u| u.tx.as_ref())
                .collect::<Vec<&TxHash>>()
                .as_slice(),
            conn,
        )
        .await?
        .into_iter()
        .map(|(id, hash, index, ts)| (hash, (id, index, ts)))
        .collect();

        let components: HashMap<String, i64> = orm::ProtocolComponent::ids_by_external_ids(
            new.iter()
                .map(|state| state.component_id.as_str())
                .collect::<Vec<&str>>()
                .as_slice(),
            chain_db_id,
            conn,
        )
        .await?
        .into_iter()
        .map(|(id, external_id)| (external_id, id))
        .collect();

        let mut state_data: Vec<(orm::NewProtocolState, i64)> = Vec::new();

        for state in new {
            let tx = state
                .tx
                .as_ref()
                .ok_or(StorageError::Unexpected(
                    "Could not reference tx in ProtocolStateDelta object".to_string(),
                ))?;
            let tx_db = txns
                .get(tx)
                .ok_or(StorageError::NotFound("Tx id".to_string(), tx.to_string()))?;

            let component_db_id = *components
                .get(&state.component_id)
                .ok_or(StorageError::NotFound(
                    "Component id".to_string(),
                    state.component_id.to_string(),
                ))?;

            let mut new_states: Vec<(orm::NewProtocolState, i64)> =
                ProtocolStateDelta::to_storage(state.entity, component_db_id, tx_db.0, tx_db.2)
                    .into_iter()
                    .map(|state| (state, tx_db.1))
                    .collect();

            // invalidated db entities for deleted attributes
            for attr in &state.deleted_attributes {
                // PERF: slow but required due to diesel restrictions
                diesel::update(schema::protocol_state::table)
                    .filter(schema::protocol_state::protocol_component_id.eq(component_db_id))
                    .filter(schema::protocol_state::attribute_name.eq(attr))
                    .filter(schema::protocol_state::valid_to.is_null())
                    .set(schema::protocol_state::valid_to.eq(tx_db.2))
                    .execute(conn)
                    .await?;
            }

            state_data.append(&mut new_states);
        }

        // Sort state_data by protocol_component_id, attribute_name, and transaction index
        state_data.sort_by(|a, b| {
            let order =
                a.0.protocol_component_id
                    .cmp(&b.0.protocol_component_id);
            if order == Ordering::Equal {
                let sub_order =
                    a.0.attribute_name
                        .cmp(&b.0.attribute_name);

                if sub_order == Ordering::Equal {
                    // Sort by block ts and tx_index as well
                    a.1.cmp(&b.1)
                } else {
                    sub_order
                }
            } else {
                order
            }
        });

        // Invalidate older states within the new state data
        let mut i = 0;
        while i + 1 < state_data.len() {
            let next_state = &state_data[i + 1].0.clone();
            let (current_state, _) = &mut state_data[i];

            // Check if next_state has same protocol_component_id and attribute_name
            if current_state.protocol_component_id == next_state.protocol_component_id &&
                current_state.attribute_name == next_state.attribute_name
            {
                // Invalidate the current state
                current_state.valid_to = Some(next_state.valid_from);
            }

            i += 1;
        }

        let state_data: Vec<orm::NewProtocolState> = state_data
            .into_iter()
            .map(|(state, _index)| state)
            .collect();

        // TODO: invalidate newly outdated protocol states already in the db (ENG-2682)

        // insert the prepared protocol state deltas
        if !state_data.is_empty() {
            diesel::insert_into(schema::protocol_state::table)
                .values(&state_data)
                .execute(conn)
                .await?;
        }
        Ok(())
    }

    async fn get_tokens(
        &self,
        chain: Chain,
        addresses: Option<&[&Address]>,
        conn: &mut Self::DB,
    ) -> Result<Vec<Self::Token>, StorageError> {
        use super::schema::{account::dsl::*, token::dsl::*};

        let mut query = token
            .inner_join(account)
            .select((token::all_columns(), schema::account::chain_id, schema::account::address))
            .into_boxed();

        if let Some(addrs) = addresses {
            query = query.filter(schema::account::address.eq_any(addrs));
        }

        let results = query
            .order(schema::token::symbol.asc())
            .load::<(orm::Token, i64, Address)>(conn)
            .await
            .map_err(|err| StorageError::from_diesel(err, "Token", &chain.to_string(), None))?;

        let tokens: Result<Vec<Self::Token>, StorageError> = results
            .into_iter()
            .map(|(orm_token, chain_id_, address_)| {
                let chain = self.get_chain(&chain_id_);
                let contract_id = ContractId::new(chain, address_);

                Self::Token::from_storage(orm_token, contract_id)
                    .map_err(|err| StorageError::DecodeError(err.to_string()))
            })
            .collect();
        tokens
    }

    async fn add_tokens(
        &self,
        tokens: &[&Self::Token],
        conn: &mut Self::DB,
    ) -> Result<(), StorageError> {
        let titles: Vec<String> = tokens
            .iter()
            .map(|token| format!("{:?}_{}", token.chain(), token.symbol()))
            .collect();

        let addresses: Vec<_> = tokens
            .iter()
            .map(|token| token.address().as_bytes().to_vec())
            .collect();

        let new_accounts: Vec<NewAccount> = tokens
            .iter()
            .zip(titles.iter())
            .zip(addresses.iter())
            .map(|((token, title), address)| {
                let chain_id = self.get_chain_id(&token.chain());
                NewAccount {
                    title,
                    address,
                    chain_id,
                    creation_tx: None,
                    created_at: None,
                    deleted_at: None,
                }
            })
            .collect();

        diesel::insert_into(schema::account::table)
            .values(&new_accounts)
            .on_conflict((schema::account::address, schema::account::chain_id))
            .do_nothing()
            .execute(conn)
            .await
            .map_err(|err| StorageError::from_diesel(err, "Account", "batch", None))?;

        let accounts: Vec<Account> = schema::account::table
            .filter(schema::account::address.eq_any(addresses))
            .select(Account::as_select())
            .get_results::<Account>(conn)
            .await
            .map_err(|err| StorageError::from_diesel(err, "Account", "retrieve", None))?;

        let account_map: HashMap<(Vec<u8>, i64), i64> = accounts
            .iter()
            .map(|account| ((account.address.clone().to_vec(), account.chain_id), account.id))
            .collect();

        let new_tokens: Vec<orm::NewToken> = tokens
            .iter()
            .map(|token| {
                let token_chain_id = self.get_chain_id(&token.chain());
                let account_key = (token.address().as_ref().to_vec(), token_chain_id);

                let account_id = *account_map
                    .get(&account_key)
                    .expect("Account ID not found");

                token.to_storage(account_id)
            })
            .collect();

        diesel::insert_into(schema::token::table)
            .values(&new_tokens)
            // .on_conflict(..).do_nothing() is necessary to ignore updating duplicated entries
            .on_conflict(schema::token::account_id)
            .do_nothing()
            .execute(conn)
            .await
            .map_err(|err| StorageError::from_diesel(err, "Token", "batch", None))?;

        Ok(())
    }

    async fn add_component_balances(
        &self,
        component_balances: &[&Self::ComponentBalance],
        chain: &Chain,
        block_ts: NaiveDateTime,
        conn: &mut Self::DB,
    ) -> Result<(), StorageError> {
        use super::schema::{account::dsl::*, token::dsl::*};

        let chain_db_id = self.get_chain_id(chain);
        let mut new_component_balances = Vec::new();
        let token_addresses: Vec<Address> = component_balances
            .iter()
            .map(|component_balance| component_balance.token())
            .collect();
        let token_ids: HashMap<Address, i64> = token
            .inner_join(account)
            .select((schema::account::address, schema::token::id))
            .filter(schema::account::address.eq_any(&token_addresses))
            .load::<(Address, i64)>(conn)
            .await?
            .into_iter()
            .collect();

        let modify_txs = component_balances
            .iter()
            .map(|component_balance| component_balance.modify_tx())
            .collect::<Vec<TxHash>>();
        let transaction_ids: HashMap<TxHash, i64> =
            orm::Transaction::ids_by_hash(&modify_txs, conn).await?;

        let external_ids: Vec<&str> = component_balances
            .iter()
            .map(|component_balance| component_balance.component_id.as_str())
            .collect();

        let protocol_component_ids: HashMap<String, i64> =
            orm::ProtocolComponent::ids_by_external_ids(&external_ids, chain_db_id, conn)
                .await?
                .into_iter()
                .map(|(component_id, external_id)| (external_id, component_id))
                .collect();

        for component_balance in component_balances.iter() {
            let token_id = token_ids[&component_balance.token()];
            let transaction_id = transaction_ids[&component_balance.modify_tx()];
            let protocol_component_id = protocol_component_ids[&component_balance
                .component_id
                .to_string()];

            let new_component_balance = component_balance.to_storage(
                token_id,
                transaction_id,
                protocol_component_id,
                block_ts,
            );
            new_component_balances.push(new_component_balance);
        }

        if !component_balances.is_empty() {
            apply_delta_versioning::<_, orm::ComponentBalance>(&mut new_component_balances, conn)
                .await?;
            diesel::insert_into(schema::component_balance::table)
                .values(&new_component_balances)
                .execute(conn)
                .await
                .map_err(|err| StorageError::from_diesel(err, "ComponentBalance", "batch", None))?;
        }
        Ok(())
    }

    #[instrument(skip(self, conn))]
    async fn get_balance_deltas(
        &self,
        chain: &Chain,
        start_version: Option<&BlockOrTimestamp>,
        target_version: &BlockOrTimestamp,
        conn: &mut Self::DB,
    ) -> Result<Vec<ComponentBalance>, StorageError> {
        use schema::component_balance::dsl::*;
        let chain_id = self.get_chain_id(chain);

        let start_ts = match start_version {
            Some(version) => version.to_ts(conn).await?,
            None => Utc::now().naive_utc(),
        };
        let target_ts = target_version.to_ts(conn).await?;

        let res = if start_ts <= target_ts {
            // Going forward
            //                  ]     changes to update   ]
            // -----------------|--------------------------|
            //                start                     target
            // We query for balance updates between start and target version.
            component_balance
                .inner_join(schema::protocol_component::table)
                .inner_join(schema::transaction::table)
                .inner_join(schema::token::table.inner_join(schema::account::table))
                .filter(
                    schema::protocol_component::chain_id
                        .eq(chain_id)
                        .and(valid_from.gt(start_ts))
                        .and(valid_from.le(target_ts))
                        .and(
                            valid_to
                                .gt(target_ts)
                                .or(valid_to.is_null()),
                        ),
                )
                .order_by((
                    protocol_component_id,
                    token_id,
                    valid_from.desc(),
                    schema::transaction::index.desc(),
                ))
                .distinct_on((protocol_component_id, token_id))
                .select((
                    schema::protocol_component::external_id,
                    schema::account::address,
                    new_balance,
                    balance_float,
                    schema::transaction::hash,
                ))
                .get_results::<(String, Address, Balance, f64, TxHash)>(conn)
                .await?
                .into_iter()
                .map(|(external_id, address, balance, bal_f64, tx)| ComponentBalance {
                    component_id: external_id,
                    token: address.into(),
                    balance,
                    balance_float: bal_f64,
                    modify_tx: tx.into(),
                })
                .collect()
        } else {
            // Going backwards
            //                  ]     changes to revert    ]
            // -----------------|--------------------------|
            //                target                     start
            // We query for the previous values of all (protocol_component, token) pairs updated
            // between start and target version.
            component_balance
                .inner_join(schema::protocol_component::table)
                .inner_join(schema::transaction::table)
                .inner_join(schema::token::table.inner_join(schema::account::table))
                .filter(
                    schema::protocol_component::chain_id
                        .eq(chain_id)
                        .and(valid_from.ge(target_ts))
                        .and(valid_from.lt(start_ts))
                        .and(
                            valid_to
                                .gt(target_ts)
                                .or(valid_to.is_null()),
                        ),
                )
                .order_by((
                    protocol_component_id,
                    token_id,
                    valid_from.asc(),
                    schema::transaction::index.asc(),
                ))
                .distinct_on((protocol_component_id, token_id))
                .select((
                    schema::protocol_component::external_id,
                    schema::account::address,
                    previous_value,
                    schema::transaction::hash,
                ))
                .get_results::<(String, Address, Balance, TxHash)>(conn)
                .await?
                .into_iter()
                .map(|(external_id, address, balance, tx)| ComponentBalance {
                    component_id: external_id,
                    token: address.into(),
                    balance,
                    balance_float: f64::NAN,
                    modify_tx: tx.into(),
                })
                .collect()
        };
        Ok(res)
    }

    async fn get_balances(
        &self,
        chain: &Chain,
        ids: Option<&[&str]>,
        at: Option<&BlockOrTimestamp>,
        conn: &mut Self::DB,
    ) -> Result<HashMap<String, HashMap<Bytes, f64>>, StorageError> {
        let version_ts = match &at {
            Some(version) => Some(version.to_ts(conn).await?),
            None => None,
        };

        let mut q = schema::component_balance::table
            .inner_join(schema::protocol_component::table)
            .inner_join(schema::token::table.inner_join(schema::account::table))
            .select((
                schema::protocol_component::external_id,
                schema::account::address,
                schema::component_balance::balance_float,
            ))
            .filter(
                schema::component_balance::valid_to
                    .gt(version_ts) // if version_ts is None, diesel equates this expression to "False"
                    .or(schema::component_balance::valid_to.is_null()),
            )
            .into_boxed();

        if let Some(external_ids) = ids {
            q = q.filter(schema::protocol_component::external_id.eq_any(external_ids))
        }

        let balances: HashMap<_, _> = q
            .get_results::<(String, Bytes, f64)>(conn)
            .await?
            .into_iter()
            .group_by(|e| e.0.clone())
            .into_iter()
            .map(|(cid, group)| {
                (
                    cid,
                    group
                        .map(|(_, addr, bal)| (addr, bal))
                        .collect::<HashMap<_, _>>(),
                )
            })
            .collect();

        Ok(balances)
    }

    async fn get_protocol_states_delta(
        &self,
        chain: &Chain,
        start_version: Option<&BlockOrTimestamp>,
        end_version: &BlockOrTimestamp,
        conn: &mut Self::DB,
    ) -> Result<Vec<ProtocolStateDelta>, StorageError> {
        let start_ts = match start_version {
            Some(version) => version.to_ts(conn).await?,
            None => Utc::now().naive_utc(),
        };
        let end_ts = end_version.to_ts(conn).await?;

        if start_ts <= end_ts {
            // Going forward
            //                  ]     changes to update   ]
            // -----------------|--------------------------|
            //                start                     target
            // We query for state updates between start and target version. We also query for
            // deleted states between start and target version. We then merge the two
            // sets of results.

            let chain_db_id = self.get_chain_id(chain);

            // fetch updated component attributes
            let state_updates =
                orm::ProtocolState::forward_deltas_by_chain(chain_db_id, start_ts, end_ts, conn)
                    .await
                    .map_err(|err| {
                        StorageError::from_diesel(
                            err,
                            "ProtocolStates",
                            chain.to_string().as_str(),
                            None,
                        )
                    })?;

            // fetch deleted component attributes
            let deleted_attrs = orm::ProtocolState::deleted_attributes_by_chain(
                chain_db_id,
                start_ts,
                end_ts,
                conn,
            )
            .await
            .map_err(|err| {
                StorageError::from_diesel(err, "ProtocolStates", chain.to_string().as_str(), None)
            })?;

            // Decode final state deltas. We can assume both the deleted_attrs and state_updates
            // are sorted by component_id and transaction index. Therefore we can use slices to
            // iterate over the data in groups of component_id. To do this we first need to collect
            // an ordered set of the component ids, then we can loop through deleted_attrs and
            // state_updates in parallel, creating a slice for each component_id.

            // Get sets of component_ids from state_updates and deleted_attrs
            let state_updates_ids: BTreeSet<_> = state_updates
                .iter()
                .map(|item| &item.1)
                .collect();
            let deleted_attrs_ids: BTreeSet<_> = deleted_attrs
                .iter()
                .map(|item| &item.0)
                .collect();
            // Union of two sets gives us a sorted set of all unique component_ids
            let mut all_component_ids = state_updates_ids.clone();
            all_component_ids.append(&mut deleted_attrs_ids.clone());

            let mut protocol_states_delta = Vec::new();

            // index trackers to iterate over the state updates and deleted attributes in parallel
            let (mut updates_index, mut deletes_index) = (0, 0);

            for current_component_id in all_component_ids {
                let component_start = updates_index;

                // Iterate over states until the component_id no longer matches the current
                // component id
                while updates_index < state_updates.len() &&
                    &state_updates[updates_index].1 == current_component_id
                {
                    updates_index += 1;
                }

                let deleted_start = deletes_index;
                // Iterate over deleted attributes until the component_id no longer matches the
                // current component id
                while deletes_index < deleted_attrs.len() &&
                    &deleted_attrs[deletes_index].0 == current_component_id
                {
                    deletes_index += 1;
                }

                let states_slice = &state_updates[component_start..updates_index];
                let deleted_slice = &deleted_attrs[deleted_start..deletes_index];

                let state_delta = ProtocolStateDelta::from_storage(
                    states_slice
                        .iter()
                        .map(|x| x.0.clone())
                        .collect(),
                    current_component_id.clone(),
                    deleted_slice
                        .iter()
                        .map(|x| x.1.clone())
                        .collect::<Vec<String>>(),
                )?;

                protocol_states_delta.push(state_delta);
            }
            Ok(protocol_states_delta)
        } else {
            // Going backwards
            //                  ]     changes to revert    ]
            // -----------------|--------------------------|
            //                target                     start
            // We query for the previous values of all component attributes updated between
            // start and target version.

            let chain_db_id = self.get_chain_id(chain);

            // fetch reverse attribute changes
            let result =
                orm::ProtocolState::reverse_delta_by_chain(chain_db_id, start_ts, end_ts, conn)
                    .await
                    .map_err(|err| {
                        StorageError::from_diesel(
                            err,
                            "ProtocolStates",
                            chain.to_string().as_str(),
                            None,
                        )
                    })?;

            // Decode final state deltas. We can assume result is sorted by component_id and
            // transaction index. Therefore we can use slices to iterate over the data in groups of
            // component_id.

            let mut deltas = Vec::new();

            let mut index = 0;
            while index < result.len() {
                let component_start = index;
                let current_component_id = &result[index].0;

                // Iterate until the component_id changes
                while index < result.len() && &result[index].0 == current_component_id {
                    index += 1;
                }

                let states_slice = &result[component_start..index];

                // sort through state updates and deletions
                let mut updates = HashMap::new();
                let mut deleted = HashSet::new();
                for (_, attribute, prev_value) in states_slice {
                    if let Some(value) = prev_value {
                        // if prev_value is not null, then the attribute was updated and
                        // must be reverted via a reversed update
                        updates.insert(attribute.clone(), value.clone());
                    } else {
                        // if prev_value is null, then the attribute was created and must be
                        // deleted on revert
                        deleted.insert(attribute.clone());
                    }
                }
                let state_delta = ProtocolStateDelta {
                    component_id: current_component_id.clone(),
                    updated_attributes: updates,
                    deleted_attributes: deleted,
                };

                deltas.push(state_delta);
            }

            Ok(deltas)
        }
    }

    async fn get_token_prices(
        &self,
        chain: Chain,
        conn: &mut Self::DB,
    ) -> Result<HashMap<Bytes, f64>, StorageError> {
        use schema::token_price::dsl::*;
        let chain_id = self.get_chain_id(&chain);
        Ok(token_price
            .inner_join(schema::token::table.inner_join(schema::account::table))
            .select((schema::account::address, price))
            .filter(schema::account::chain_id.eq(chain_id))
            .get_results::<(Bytes, f64)>(conn)
            .await
            .map_err(|err| StorageError::from_diesel(err, "TokenPrice", &chain.to_string(), None))?
            .into_iter()
            .collect::<HashMap<_, _>>())
    }

    async fn upsert_component_tvl(
        &self,
        chain: Chain,
        tvl_values: &HashMap<String, f64>,
        conn: &mut Self::DB,
    ) -> Result<(), StorageError> {
        use schema::protocol_component::dsl::*;

        let external_db_id_map: HashMap<String, i64> = protocol_component
            .select((external_id, id))
            .filter(external_id.eq_any(tvl_values.keys()))
            .get_results::<(String, i64)>(conn)
            .await?
            .into_iter()
            .collect::<HashMap<_, _>>();

        let upsert_map: HashMap<_, _> = tvl_values
            .iter()
            .filter_map(|(component_id, v)| {
                if let Some(db_id) = external_db_id_map.get(component_id) {
                    Some((*db_id, *v))
                } else {
                    warn!(?component_id, "Tried to upsert tvl for unknown compoent!");
                    None
                }
            })
            .collect();
        ComponentTVL::upsert_many(&upsert_map)
            .execute(conn)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        extractor::evm::{self, ERC20Token},
        storage::{BlockIdentifier, ChangeType},
    };
    use chrono::{NaiveDateTime, Utc};
    use diesel_async::AsyncConnection;
    use ethers::{prelude::H160, types::U256};
    use rstest::rstest;
    use serde_json::json;

    use crate::{
        models,
        models::{FinancialType, ImplementationType},
        storage::postgres::{db_fixtures, orm, schema, PostgresGateway},
    };
    use ethers::prelude::H256;
    use std::{collections::HashMap, str::FromStr};
    use tycho_types::Bytes;

    type EVMGateway = PostgresGateway<
        evm::Block,
        evm::Transaction,
        evm::Account,
        evm::AccountUpdate,
        evm::ERC20Token,
    >;

    const WETH: &str = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2";
    const USDC: &str = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48";
    const USDT: &str = "0xdAC17F958D2ee523a2206206994597C13D831ec7";
    const LUSD: &str = "0x5f98805A4E8be255a32880FDeC7F6728C6568bA0";
    const DAI: &str = "0x6B175474E89094C44Da98b954EedeAC495271d0F";

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

    /// This sets up the data needed to test the gateway. The setup is structured such that each
    /// protocol state's historical changes are kept together this makes it easy to reason about
    /// that change an account should have at each version Please note that if you change
    /// something here, also update the state fixtures right below, which contain protocol states
    /// at each version.
    async fn setup_data(conn: &mut AsyncPgConnection) -> Vec<String> {
        let chain_id = db_fixtures::insert_chain(conn, "ethereum").await;
        let chain_id_sn = db_fixtures::insert_chain(conn, "starknet").await;
        let blk = db_fixtures::insert_blocks(conn, chain_id).await;
        let tx_hashes = [
            "0xbb7e16d797a9e2fbc537e30f91ed3d27a254dd9578aa4c3af3e5f0d3e8130945".to_string(),
            "0x794f7df7a3fe973f1583fbb92536f9a8def3a89902439289315326c04068de54".to_string(),
            "0x3108322284d0a89a7accb288d1a94384d499504fe7e04441b0706c7628dee7b7".to_string(),
            "0x50449de1973d86f21bfafa7c72011854a7e33a226709dc3e2e4edcca34188388".to_string(),
        ];

        let txn = db_fixtures::insert_txns(
            conn,
            &[
                (blk[0], 1i64, &tx_hashes[0]),
                (blk[0], 2i64, &tx_hashes[1]),
                // ----- Block 01 LAST
                (blk[1], 1i64, &tx_hashes[2]),
                (blk[1], 2i64, &tx_hashes[3]),
                // ----- Block 02 LAST
            ],
        )
        .await;

        let protocol_system_id_ambient =
            db_fixtures::insert_protocol_system(conn, "ambient".to_owned()).await;
        let protocol_system_id_zz =
            db_fixtures::insert_protocol_system(conn, "zigzag".to_owned()).await;

        let protocol_type_id = db_fixtures::insert_protocol_type(
            conn,
            "Pool",
            Some(orm::FinancialType::Swap),
            None,
            Some(orm::ImplementationType::Custom),
        )
        .await;

        // insert tokens
        let (account_id_weth, weth_id) =
            db_fixtures::insert_token(conn, chain_id, WETH.trim_start_matches("0x"), "WETH", 18)
                .await;
        let (account_id_usdc, usdc_id) =
            db_fixtures::insert_token(conn, chain_id, USDC.trim_start_matches("0x"), "USDC", 6)
                .await;
        let (account_id_dai, dai_id) =
            db_fixtures::insert_token(conn, chain_id, DAI.trim_start_matches("0x"), "DAI", 18)
                .await;
        let (account_id_lusd, lusd_id) =
            db_fixtures::insert_token(conn, chain_id, LUSD.trim_start_matches("0x"), "LUSD", 18)
                .await;

        // insert token prices
        db_fixtures::insert_token_prices(&[(weth_id, 1.0), (usdc_id, 0.005)], conn).await;

        let contract_code_id = db_fixtures::insert_contract_code(
            conn,
            account_id_weth,
            txn[0],
            Bytes::from_str("C0C0C0").unwrap(),
        )
        .await;

        // components and their balances
        let protocol_component_id = db_fixtures::insert_protocol_component(
            conn,
            "state1",
            chain_id,
            protocol_system_id_ambient,
            protocol_type_id,
            txn[0],
            Some(vec![weth_id, usdc_id]),
            Some(vec![contract_code_id]),
        )
        .await;
        db_fixtures::insert_component_balance(
            conn,
            Bytes::from(U256::exp10(18)),
            1e18,
            weth_id,
            txn[0],
            protocol_component_id,
        )
        .await;
        db_fixtures::insert_component_balance(
            conn,
            Bytes::from(U256::from(2000) * U256::exp10(6)),
            2000.0 * 1e6,
            usdc_id,
            txn[0],
            protocol_component_id,
        )
        .await;
        let protocol_component_id2 = db_fixtures::insert_protocol_component(
            conn,
            "state3",
            chain_id,
            protocol_system_id_ambient,
            protocol_type_id,
            txn[0],
            Some(vec![weth_id, dai_id]),
            Some(vec![contract_code_id]),
        )
        .await;
        db_fixtures::insert_component_balance(
            conn,
            Bytes::from(U256::exp10(18)),
            1e18,
            weth_id,
            txn[0],
            protocol_component_id2,
        )
        .await;
        db_fixtures::insert_component_balance(
            conn,
            Bytes::from(U256::from(2000) * U256::exp10(18)),
            2000.0 * 1e18,
            dai_id,
            txn[0],
            protocol_component_id2,
        )
        .await;
        let protocol_component_id3 = db_fixtures::insert_protocol_component(
            conn,
            "state2",
            chain_id_sn,
            protocol_system_id_zz,
            protocol_type_id,
            txn[1],
            Some(vec![lusd_id, usdc_id]),
            Some(vec![contract_code_id]),
        )
        .await;
        db_fixtures::insert_component_balance(
            conn,
            Bytes::from(U256::from(2000) * U256::exp10(18)),
            1e18,
            lusd_id,
            txn[0],
            protocol_component_id3,
        )
        .await;
        db_fixtures::insert_component_balance(
            conn,
            Bytes::from(U256::from(2000) * U256::exp10(6)),
            2000.0 * 1e6,
            usdc_id,
            txn[0],
            protocol_component_id3,
        )
        .await;

        // protocol state for state1-reserve1
        db_fixtures::insert_protocol_state(
            conn,
            protocol_component_id,
            txn[0],
            "reserve1".to_owned(),
            Bytes::from(U256::from(1100)),
            None,
            Some(txn[2]),
        )
        .await;

        // protocol state for state1-reserve2
        db_fixtures::insert_protocol_state(
            conn,
            protocol_component_id,
            txn[0],
            "reserve2".to_owned(),
            Bytes::from(U256::from(500)),
            None,
            None,
        )
        .await;

        // protocol state update for state1-reserve1
        db_fixtures::insert_protocol_state(
            conn,
            protocol_component_id,
            txn[3],
            "reserve1".to_owned(),
            Bytes::from(U256::from(1000)),
            Some(Bytes::from(U256::from(1100))),
            None,
        )
        .await;

        tx_hashes.to_vec()
    }

    fn protocol_state() -> ProtocolState {
        let attributes: HashMap<String, Bytes> = vec![
            ("reserve1".to_owned(), Bytes::from(U256::from(1000))),
            ("reserve2".to_owned(), Bytes::from(U256::from(500))),
        ]
        .into_iter()
        .collect();
        ProtocolState::new(
            "state1".to_owned(),
            attributes,
            "0x50449de1973d86f21bfafa7c72011854a7e33a226709dc3e2e4edcca34188388"
                .parse()
                .unwrap(),
        )
    }

    #[rstest]
    #[case::by_chain(None, None)]
    #[case::by_system(Some("ambient".to_string()), None)]
    #[case::by_ids(None, Some(vec ! ["state1"]))]
    #[tokio::test]

    async fn test_get_protocol_states(
        #[case] system: Option<String>,
        #[case] ids: Option<Vec<&str>>,
    ) {
        let mut conn = setup_db().await;
        setup_data(&mut conn).await;

        let expected = vec![protocol_state()];

        let gateway = EVMGateway::from_connection(&mut conn).await;

        let result = gateway
            .get_protocol_states(&Chain::Ethereum, None, system, ids.as_deref(), &mut conn)
            .await
            .unwrap();

        assert_eq!(result, expected)
    }

    #[tokio::test]

    async fn test_get_protocol_states_at() {
        let mut conn = setup_db().await;
        setup_data(&mut conn).await;

        let gateway = EVMGateway::from_connection(&mut conn).await;

        let mut protocol_state = protocol_state();
        let attributes: HashMap<String, Bytes> = vec![
            ("reserve1".to_owned(), Bytes::from(U256::from(1100))),
            ("reserve2".to_owned(), Bytes::from(U256::from(500))),
        ]
        .into_iter()
        .collect();
        protocol_state.attributes = attributes;
        protocol_state.modify_tx =
            "0xbb7e16d797a9e2fbc537e30f91ed3d27a254dd9578aa4c3af3e5f0d3e8130945"
                .parse()
                .unwrap();
        let expected = vec![protocol_state];

        let result = gateway
            .get_protocol_states(
                &Chain::Ethereum,
                Some(Version::from_block_number(Chain::Ethereum, 1)),
                None,
                None,
                &mut conn,
            )
            .await
            .unwrap();

        assert_eq!(result, expected)
    }

    fn protocol_state_delta() -> ProtocolStateDelta {
        let attributes: HashMap<String, Bytes> =
            vec![("reserve1".to_owned(), Bytes::from(U256::from(1000)))]
                .into_iter()
                .collect();
        ProtocolStateDelta::new("state3".to_owned(), attributes)
    }

    #[tokio::test]

    async fn test_update_protocol_states() {
        let mut conn = setup_db().await;
        setup_data(&mut conn).await;

        let gateway = EVMGateway::from_connection(&mut conn).await;
        let chain = Chain::Ethereum;

        // set up deletable attribute state
        let protocol_component_id = schema::protocol_component::table
            .filter(schema::protocol_component::external_id.eq("state2"))
            .select(schema::protocol_component::id)
            .first::<i64>(&mut conn)
            .await
            .expect("Failed to fetch protocol component id");
        let txn_id = schema::transaction::table
            .filter(
                schema::transaction::hash.eq(H256::from_str(
                    "0xbb7e16d797a9e2fbc537e30f91ed3d27a254dd9578aa4c3af3e5f0d3e8130945",
                )
                .expect("valid txhash")
                .as_bytes()
                .to_owned()),
            )
            .select(schema::transaction::id)
            .first::<i64>(&mut conn)
            .await
            .expect("Failed to fetch transaction id");
        db_fixtures::insert_protocol_state(
            &mut conn,
            protocol_component_id,
            txn_id,
            "deletable".to_owned(),
            Bytes::from(U256::from(1000)),
            None,
            None,
        )
        .await;

        // update
        let mut new_state1 = protocol_state_delta();
        let attributes1: HashMap<String, Bytes> = vec![
            ("reserve1".to_owned(), Bytes::from(U256::from(700))),
            ("reserve2".to_owned(), Bytes::from(U256::from(700))),
        ]
        .into_iter()
        .collect();
        new_state1.updated_attributes = attributes1.clone();
        new_state1.deleted_attributes = vec!["deletable".to_owned()]
            .into_iter()
            .collect();
        let tx_1: H256 = "0x3108322284d0a89a7accb288d1a94384d499504fe7e04441b0706c7628dee7b7"
            .parse()
            .unwrap();

        // newer update
        let mut new_state2 = protocol_state_delta();
        let attributes2: HashMap<String, Bytes> = vec![
            ("reserve1".to_owned(), Bytes::from(U256::from(800))),
            ("reserve2".to_owned(), Bytes::from(U256::from(800))),
        ]
        .into_iter()
        .collect();
        new_state2.updated_attributes = attributes2.clone();
        let tx_2: H256 = "0x50449de1973d86f21bfafa7c72011854a7e33a226709dc3e2e4edcca34188388"
            .parse()
            .unwrap();

        // update the protocol state
        gateway
            .update_protocol_states(
                &chain,
                &[(tx_1.into(), &new_state1), (tx_2.into(), &new_state2)],
                &mut conn,
            )
            .await
            .expect("Failed to update protocol states");

        // check the correct state is considered the valid one
        let db_states = gateway
            .get_protocol_states(
                &chain,
                None,
                None,
                Some(&[new_state1.component_id.as_str()]),
                &mut conn,
            )
            .await
            .expect("Failed ");
        let mut expected_state = protocol_state();
        expected_state.attributes = attributes2;
        expected_state.component_id = new_state1.component_id.clone();
        assert_eq!(db_states[0], expected_state);

        // fetch the older state from the db and check it's valid_to is set correctly
        let tx_hash1: Bytes = tx_1.as_bytes().into();
        let older_state = schema::protocol_state::table
            .inner_join(schema::protocol_component::table)
            .inner_join(schema::transaction::table)
            .filter(schema::transaction::hash.eq(tx_hash1))
            .filter(schema::protocol_component::external_id.eq(new_state1.component_id.as_str()))
            .select(orm::ProtocolState::as_select())
            .first::<orm::ProtocolState>(&mut conn)
            .await
            .expect("Failed to fetch protocol state");
        assert_eq!(older_state.attribute_value, Bytes::from(U256::from(700)));
        // fetch the newer state from the db to compare the valid_from
        let tx_hash2: Bytes = tx_2.as_bytes().into();
        let newer_state = schema::protocol_state::table
            .inner_join(schema::protocol_component::table)
            .inner_join(schema::transaction::table)
            .filter(schema::transaction::hash.eq(tx_hash2))
            .filter(schema::protocol_component::external_id.eq(new_state1.component_id.as_str()))
            .select(orm::ProtocolState::as_select())
            .first::<orm::ProtocolState>(&mut conn)
            .await
            .expect("Failed to fetch protocol state");
        assert_eq!(older_state.valid_to, Some(newer_state.valid_from));
    }

    #[tokio::test]
    async fn test_get_balance_deltas() {
        let mut conn = setup_db().await;
        setup_data(&mut conn).await;
        let protocol_external_id = String::from("state1");
        // set up changed balances
        let protocol_component_id = schema::protocol_component::table
            .filter(schema::protocol_component::external_id.eq(protocol_external_id.clone()))
            .select(schema::protocol_component::id)
            .first::<i64>(&mut conn)
            .await
            .expect("Failed to fetch protocol component id");
        let (token_id, account_id) = schema::token::table
            .filter(schema::token::symbol.eq("WETH"))
            .select((schema::token::id, schema::token::account_id))
            .first::<(i64, i64)>(&mut conn)
            .await
            .expect("Failed to fetch token id and acccount id");
        let token_address = schema::account::table
            .filter(schema::account::id.eq(account_id))
            .select(schema::account::address)
            .first::<Address>(&mut conn)
            .await
            .expect("Failed to fetch token address");

        let from_tx_hash =
            H256::from_str("0x794f7df7a3fe973f1583fbb92536f9a8def3a89902439289315326c04068de54")
                .expect("valid txhash");

        let from_txn_id = schema::transaction::table
            .filter(schema::transaction::hash.eq(from_tx_hash.clone().as_bytes()))
            .select(schema::transaction::id)
            .first::<i64>(&mut conn)
            .await
            .expect("Failed to fetch transaction id");

        let to_tx_hash =
            H256::from_str("0x50449de1973d86f21bfafa7c72011854a7e33a226709dc3e2e4edcca34188388")
                .expect("valid txhash");

        let to_txn_id = schema::transaction::table
            .filter(schema::transaction::hash.eq(to_tx_hash.clone().as_bytes()))
            .select(schema::transaction::id)
            .first::<i64>(&mut conn)
            .await
            .expect("Failed to fetch transaction id");

        db_fixtures::insert_component_balance(
            &mut conn,
            Balance::from(U256::from(1000)),
            Balance::from(U256::from(0)),
            1000.0,
            token_id,
            from_txn_id,
            protocol_component_id,
        )
        .await;
        db_fixtures::insert_component_balance(
            &mut conn,
            Balance::from(U256::from(2000)),
            Balance::from(U256::from(1000)),
            2000.0,
            token_id,
            to_txn_id,
            protocol_component_id,
        )
        .await;

        let gateway = EVMGateway::from_connection(&mut conn).await;

        let expected_forward_deltas: Vec<ComponentBalance> = vec![ComponentBalance {
            component_id: protocol_external_id.clone(),
            token: token_address.clone().into(),
            balance: Balance::from(U256::from(2000)),
            balance_float: 2000.0,
            modify_tx: to_tx_hash,
        }];

        // test forward case
        let result = gateway
            .get_balance_deltas(
                &Chain::Ethereum,
                Some(&BlockOrTimestamp::Block(BlockIdentifier::Number((Chain::Ethereum, 1)))),
                &BlockOrTimestamp::Block(BlockIdentifier::Number((Chain::Ethereum, 2))),
                &mut conn,
            )
            .await
            .unwrap();
        assert_eq!(result, expected_forward_deltas);

        let expected_txh: H256 = "bb7e16d797a9e2fbc537e30f91ed3d27a254dd9578aa4c3af3e5f0d3e8130945"
            .parse()
            .unwrap();
        let expected_backward_deltas: Vec<ComponentBalance> = vec![
            ComponentBalance {
                token: DAI
                    .trim_start_matches("0x")
                    .parse()
                    .unwrap(),
                new_balance: Bytes::from(
                    "0x00000000000000000000000000000000000000000000006c6b935b8bbd400000",
                ),
                balance_float: 0.0,
                modify_tx: expected_txh.clone(),
                component_id: "state3".to_owned(),
            },
            ComponentBalance {
                token: USDC
                    .trim_start_matches("0x")
                    .parse()
                    .unwrap(),
                new_balance: Bytes::from(
                    "0x0000000000000000000000000000000000000000000000000000000077359400",
                ),
                balance_float: 0.0,
                modify_tx: expected_txh.clone(),
                component_id: "state1".to_owned(),
            },
            ComponentBalance {
                token: WETH
                    .trim_start_matches("0x")
                    .parse()
                    .unwrap(),
                new_balance: Bytes::from(
                    "0x0000000000000000000000000000000000000000000000000de0b6b3a7640000",
                ),
                balance_float: 0.0,
                modify_tx: expected_txh.clone(),
                component_id: "state1".to_owned(),
            },
            ComponentBalance {
                token: WETH
                    .trim_start_matches("0x")
                    .parse()
                    .unwrap(),
                new_balance: Bytes::from(
                    "0x0000000000000000000000000000000000000000000000000de0b6b3a7640000",
                ),
                balance_float: 0.0,
                modify_tx: expected_txh.clone(),
                component_id: "state3".to_owned(),
            },
        ];

        // test backward case
        let mut result = gateway
            .get_balance_deltas(
                &Chain::Ethereum,
                Some(&BlockOrTimestamp::Block(BlockIdentifier::Number((Chain::Ethereum, 2)))),
                &BlockOrTimestamp::Block(BlockIdentifier::Number((Chain::Ethereum, 1))),
                &mut conn,
            )
            .await
            .unwrap();
        result.sort_unstable_by_key(|e| (e.token, e.component_id.clone()));
        // fix NaN comparison
        result.iter_mut().for_each(|r| {
            assert!(r.balance_float.is_nan());
            r.balance_float = 0.0;
        });
        assert_eq!(result, expected_backward_deltas);
    }

    #[tokio::test]
    async fn test_get_protocol_states_delta_forward() {
        let mut conn = setup_db().await;
        setup_data(&mut conn).await;

        // set up deleted attribute state
        let protocol_component_id = schema::protocol_component::table
            .filter(schema::protocol_component::external_id.eq("state1"))
            .select(schema::protocol_component::id)
            .first::<i64>(&mut conn)
            .await
            .expect("Failed to fetch protocol component id");
        let from_txn_id = schema::transaction::table
            .filter(
                schema::transaction::hash.eq(H256::from_str(
                    "0x794f7df7a3fe973f1583fbb92536f9a8def3a89902439289315326c04068de54",
                )
                .expect("valid txhash")
                .as_bytes()
                .to_owned()),
            )
            .select(schema::transaction::id)
            .first::<i64>(&mut conn)
            .await
            .expect("Failed to fetch transaction id");
        let to_txn_id = schema::transaction::table
            .filter(
                schema::transaction::hash.eq(H256::from_str(
                    "0x50449de1973d86f21bfafa7c72011854a7e33a226709dc3e2e4edcca34188388",
                )
                .expect("valid txhash")
                .as_bytes()
                .to_owned()),
            )
            .select(schema::transaction::id)
            .first::<i64>(&mut conn)
            .await
            .expect("Failed to fetch transaction id");
        db_fixtures::insert_protocol_state(
            &mut conn,
            protocol_component_id,
            from_txn_id,
            "deleted".to_owned(),
            Bytes::from(U256::from(1000)),
            None,
            Some(to_txn_id),
        )
        .await;

        // set up deleted attribute different state (one that isn't also updated)
        let protocol_component_id2 = schema::protocol_component::table
            .filter(schema::protocol_component::external_id.eq("state3"))
            .select(schema::protocol_component::id)
            .first::<i64>(&mut conn)
            .await
            .expect("Failed to fetch protocol component id");
        db_fixtures::insert_protocol_state(
            &mut conn,
            protocol_component_id2,
            from_txn_id,
            "deleted2".to_owned(),
            Bytes::from(U256::from(100)),
            None,
            Some(to_txn_id),
        )
        .await;

        let gateway = EVMGateway::from_connection(&mut conn).await;

        // expected result
        let mut state_delta = protocol_state_delta();
        state_delta.component_id = "state1".to_owned();
        state_delta.deleted_attributes = vec!["deleted".to_owned()]
            .into_iter()
            .collect();
        let other_state_delta = ProtocolStateDelta {
            component_id: "state3".to_owned(),
            updated_attributes: HashMap::new(),
            deleted_attributes: vec!["deleted2".to_owned()]
                .into_iter()
                .collect(),
        };
        let expected = vec![state_delta, other_state_delta];

        // test
        let result = gateway
            .get_protocol_states_delta(
                &Chain::Ethereum,
                Some(&BlockOrTimestamp::Block(BlockIdentifier::Number((Chain::Ethereum, 1)))),
                &BlockOrTimestamp::Block(BlockIdentifier::Number((Chain::Ethereum, 2))),
                &mut conn,
            )
            .await
            .unwrap();

        // asserts
        assert_eq!(result, expected)
    }

    #[tokio::test]
    async fn test_get_protocol_states_delta_backward() {
        let mut conn = setup_db().await;
        setup_data(&mut conn).await;

        // set up newly added attribute state (to be deleted on revert)
        let protocol_component_id = schema::protocol_component::table
            .filter(schema::protocol_component::external_id.eq("state1"))
            .select(schema::protocol_component::id)
            .first::<i64>(&mut conn)
            .await
            .expect("Failed to fetch protocol component id");
        let txn_id = schema::transaction::table
            .filter(
                schema::transaction::hash.eq(H256::from_str(
                    "0x3108322284d0a89a7accb288d1a94384d499504fe7e04441b0706c7628dee7b7",
                )
                .expect("valid txhash")
                .as_bytes()
                .to_owned()),
            )
            .select(schema::transaction::id)
            .first::<i64>(&mut conn)
            .await
            .expect("Failed to fetch transaction id");
        db_fixtures::insert_protocol_state(
            &mut conn,
            protocol_component_id,
            txn_id,
            "to_delete".to_owned(),
            Bytes::from(U256::from(1000)),
            None,
            None,
        )
        .await;

        // set up deleted attribute state (to be created on revert)
        let from_txn_id = schema::transaction::table
            .filter(
                schema::transaction::hash.eq(H256::from_str(
                    "0x794f7df7a3fe973f1583fbb92536f9a8def3a89902439289315326c04068de54",
                )
                .expect("valid txhash")
                .as_bytes()
                .to_owned()),
            )
            .select(schema::transaction::id)
            .first::<i64>(&mut conn)
            .await
            .expect("Failed to fetch transaction id");
        let to_txn_id = schema::transaction::table
            .filter(
                schema::transaction::hash.eq(H256::from_str(
                    "0x50449de1973d86f21bfafa7c72011854a7e33a226709dc3e2e4edcca34188388",
                )
                .expect("valid txhash")
                .as_bytes()
                .to_owned()),
            )
            .select(schema::transaction::id)
            .first::<i64>(&mut conn)
            .await
            .expect("Failed to fetch transaction id");
        db_fixtures::insert_protocol_state(
            &mut conn,
            protocol_component_id,
            from_txn_id,
            "deleted".to_owned(),
            Bytes::from(U256::from(1000)),
            None,
            Some(to_txn_id),
        )
        .await;

        let gateway = EVMGateway::from_connection(&mut conn).await;

        // expected result
        let attributes: HashMap<String, Bytes> = vec![
            ("reserve1".to_owned(), Bytes::from(U256::from(1100))),
            ("deleted".to_owned(), Bytes::from(U256::from(1000))),
        ]
        .into_iter()
        .collect();
        let state_delta = ProtocolStateDelta {
            component_id: "state1".to_owned(),
            updated_attributes: attributes,
            deleted_attributes: vec!["to_delete".to_owned()]
                .into_iter()
                .collect(),
        };
        let expected = vec![state_delta];

        // test
        let result = gateway
            .get_protocol_states_delta(
                &Chain::Ethereum,
                Some(&BlockOrTimestamp::Block(BlockIdentifier::Number((Chain::Ethereum, 2)))),
                &BlockOrTimestamp::Block(BlockIdentifier::Number((Chain::Ethereum, 1))),
                &mut conn,
            )
            .await
            .unwrap();

        // asserts
        assert_eq!(result, expected)
    }

    #[tokio::test]
    async fn test_get_or_create_protocol_system_id() {
        let mut conn = setup_db().await;
        let gw = EVMGateway::from_connection(&mut conn).await;

        let first_id = gw
            ._get_or_create_protocol_system_id("ambient".to_string(), &mut conn)
            .await
            .unwrap();

        let second_id = gw
            ._get_or_create_protocol_system_id("ambient".to_string(), &mut conn)
            .await
            .unwrap();
        assert!(first_id > 0);
        assert_eq!(first_id, second_id);
    }

    #[tokio::test]
    async fn test_add_protocol_type() {
        let mut conn = setup_db().await;
        let gw = EVMGateway::from_connection(&mut conn).await;

        let protocol_type = models::ProtocolType {
            name: "Protocol".to_string(),
            financial_type: FinancialType::Debt,
            attribute_schema: Some(json!({"attribute": "schema"})),
            implementation: ImplementationType::Custom,
        };

        gw.add_protocol_types(&[protocol_type], &mut conn)
            .await
            .unwrap();

        let inserted_data = schema::protocol_type::table
            .filter(schema::protocol_type::name.eq("Protocol"))
            .select(schema::protocol_type::all_columns)
            .first::<orm::ProtocolType>(&mut conn)
            .await
            .unwrap();

        assert_eq!(inserted_data.name, "Protocol".to_string());
        assert_eq!(inserted_data.financial_type, orm::FinancialType::Debt);
        assert_eq!(inserted_data.attribute_schema, Some(json!({"attribute": "schema"})));
        assert_eq!(inserted_data.implementation, orm::ImplementationType::Custom);
    }

    #[tokio::test]

    async fn test_get_tokens() {
        let mut conn = setup_db().await;
        setup_data(&mut conn).await;
        let gw = EVMGateway::from_connection(&mut conn).await;

        // get all tokens (no address filter)
        let tokens = gw
            .get_tokens(Chain::Ethereum, None, &mut conn)
            .await
            .unwrap();
        assert_eq!(tokens.len(), 4);

        // get weth and usdc
        let tokens = gw
            .get_tokens(Chain::Ethereum, Some(&[&WETH.into(), &USDC.into()]), &mut conn)
            .await
            .unwrap();
        assert_eq!(tokens.len(), 2);

        // get weth
        let tokens = gw
            .get_tokens(Chain::Ethereum, Some(&[&WETH.into()]), &mut conn)
            .await
            .unwrap();
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].symbol, "WETH".to_string());
        assert_eq!(tokens[0].decimals, 18);
    }

    #[tokio::test]

    async fn test_add_tokens() {
        let mut conn = setup_db().await;
        setup_data(&mut conn).await;
        let gw = EVMGateway::from_connection(&mut conn).await;

        // Insert one new token (USDT) and an existing token (WETH)
        let weth_symbol = "WETH".to_string();
        let old_token = db_fixtures::get_token_by_symbol(&mut conn, weth_symbol.clone()).await;
        let old_account = &orm::Account::by_address(
            &Bytes::from_str(WETH.trim_start_matches("0x")).expect("address ok"),
            &mut conn,
        )
        .await
        .unwrap()[0];

        let usdt_symbol = "USDT".to_string();
        let tokens = [
            &ERC20Token {
                address: H160::from_str(USDT).unwrap(),
                symbol: usdt_symbol.clone(),
                decimals: 6,
                tax: 0,
                gas: vec![Some(64), None],
                chain: Chain::Ethereum,
            },
            &ERC20Token {
                address: H160::from_str(WETH).unwrap(),
                symbol: weth_symbol.clone(),
                decimals: 18,
                tax: 0,
                gas: vec![Some(100), None],
                chain: Chain::Ethereum,
            },
        ];

        gw.add_tokens(&tokens, &mut conn)
            .await
            .unwrap();

        let inserted_token = db_fixtures::get_token_by_symbol(&mut conn, usdt_symbol.clone()).await;
        assert_eq!(inserted_token.symbol, usdt_symbol);
        assert_eq!(inserted_token.decimals, 6);
        let inserted_account = &orm::Account::by_address(
            &Bytes::from_str(USDT.trim_start_matches("0x")).expect("address ok"),
            &mut conn,
        )
        .await
        .unwrap()[0];
        assert_eq!(inserted_account.id, inserted_token.account_id);
        assert_eq!(inserted_account.title, "Ethereum_USDT".to_string());

        // make sure nothing changed on WETH (ids included)
        let new_token = db_fixtures::get_token_by_symbol(&mut conn, weth_symbol.clone()).await;
        assert_eq!(new_token, old_token);
        let new_account = &orm::Account::by_address(
            &Bytes::from_str(WETH.trim_start_matches("0x")).expect("address ok"),
            &mut conn,
        )
        .await
        .unwrap()[0];
        assert_eq!(new_account, old_account);
        assert!(inserted_account.id > new_account.id);
    }
    #[tokio::test]
    async fn test_add_component_balances() {
        let mut conn = setup_db().await;
        setup_data(&mut conn).await;
        let gw = EVMGateway::from_connection(&mut conn).await;
        let tx_hash =
            H256::from_str("0xbb7e16d797a9e2fbc537e30f91ed3d27a254dd9578aa4c3af3e5f0d3e8130945")
                .unwrap();
        let component_external_id = "state2".to_owned();
        let base_token = H160::from_str(WETH.trim_start_matches("0x")).unwrap();
        // Test the case where a previous balance doesn't exist
        let component_balance = ComponentBalance {
            token: base_token,
            new_balance: Bytes::from(&[12u8]),
            balance_float: 12.0,
            modify_tx: tx_hash,
            component_id: component_external_id,
        };
        let block_ts = NaiveDateTime::from_timestamp_opt(1000, 0).unwrap();

        gw.add_component_balances(&[&component_balance], &Chain::Starknet, block_ts, &mut conn)
            .await
            .unwrap();

        let inserted_data = schema::component_balance::table
            .select(orm::ComponentBalance::as_select())
            .filter(schema::component_balance::new_balance.eq(Bytes::from(&[12u8])))
            .first::<orm::ComponentBalance>(&mut conn)
            .await
            .expect("retrieving inserted balance failed!");

        assert!(inserted_data.is_ok());
        let inserted_data: orm::ComponentBalance = inserted_data.unwrap();

        assert_eq!(inserted_data.new_balance, Balance::from(U256::from(12)));
        assert_eq!(inserted_data.previous_value, Balance::from(U256::from(0)),);

        let referenced_token = schema::token::table
            .filter(schema::token::id.eq(inserted_data.token_id))
            .select(orm::Token::as_select())
            .first::<orm::Token>(&mut conn)
            .await
            .expect("failed to get associated token");
        assert_eq!(referenced_token.symbol, "WETH");

        let referenced_component = schema::protocol_component::table
            .filter(schema::protocol_component::id.eq(inserted_data.protocol_component_id))
            .select(orm::ProtocolComponent::as_select())
            .first::<orm::ProtocolComponent>(&mut conn)
            .await
            .expect("failed to get associated component");
        assert_eq!(referenced_component.external_id, "state2");

        // Test the case where there was a previous balance
        let new_tx_hash =
            H256::from_str("0x3108322284d0a89a7accb288d1a94384d499504fe7e04441b0706c7628dee7b7")
                .unwrap();
        let updated_component_balance = ComponentBalance {
            token: base_token,
            balance: Balance::from(U256::from(2000)),
            balance_float: 2000.0,
            modify_tx: new_tx_hash,
            component_id: protocol_component_id.clone(),
        };

        let updated_component_balances = vec![&updated_component_balance];
        let new_block_ts = NaiveDateTime::from_timestamp_opt(2000, 0).unwrap();

        gw.add_component_balances(
            &updated_component_balances,
            &Chain::Starknet,
            new_block_ts,
            &mut conn,
        )
        .await
        .unwrap();

        // Obtain newest inserted value
        let new_inserted_data = schema::component_balance::table
            .select(orm::ComponentBalance::as_select())
            .order_by(schema::component_balance::id.desc())
            .first::<orm::ComponentBalance>(&mut conn)
            .await;

        assert!(new_inserted_data.is_ok());
        let new_inserted_data: orm::ComponentBalance = new_inserted_data.unwrap();

        assert_eq!(new_inserted_data.new_balance, Balance::from(U256::from(2000)));
        assert_eq!(new_inserted_data.previous_value, Balance::from(U256::from(1000)));
    }

    #[tokio::test]
    async fn test_add_protocol_components() {
        let mut conn = setup_db().await;
        setup_data(&mut conn).await;
        let gw = EVMGateway::from_connection(&mut conn).await;
        let protocol_type_name_1 = String::from("Test_Type_1");
        let protocol_type_id_1 =
            db_fixtures::insert_protocol_type(&mut conn, &protocol_type_name_1, None, None, None)
                .await;
        db_fixtures::insert_protocol_type(&mut conn, "Test_Type_2", None, None, None).await;
        let protocol_system = "ambient".to_string();
        let chain = Chain::Ethereum;
        let original_component = ProtocolComponent {
            id: "test_contract_id".to_string(),
            protocol_system,
            protocol_type_name: protocol_type_name_1,
            chain,
            tokens: vec![H160::from_str(WETH).unwrap()],
            contract_ids: vec![H160::from_str(WETH).unwrap()],
            static_attributes: HashMap::new(),
            change: ChangeType::Creation,
            creation_tx: H256::from_str(
                "0xbb7e16d797a9e2fbc537e30f91ed3d27a254dd9578aa4c3af3e5f0d3e8130945",
            )
            .unwrap(),
            created_at: Default::default(),
        };

        let result = gw
            .add_protocol_components(&[&original_component.clone()], &mut conn)
            .await;

        assert!(result.is_ok());

        let inserted_data = schema::protocol_component::table
            .filter(schema::protocol_component::external_id.eq("test_contract_id".to_string()))
            .select(orm::ProtocolComponent::as_select())
            .first::<orm::ProtocolComponent>(&mut conn)
            .await;

        assert!(inserted_data.is_ok());
        let inserted_data: orm::ProtocolComponent = inserted_data.unwrap();
        assert_eq!(inserted_data.protocol_type_id, protocol_type_id_1);
        assert_eq!(
            gw.get_protocol_system_id(
                &original_component
                    .protocol_system
                    .to_string()
            ),
            inserted_data.protocol_system_id
        );
        assert_eq!(gw.get_chain_id(&original_component.chain), inserted_data.chain_id);
        assert_eq!(original_component.id, inserted_data.external_id);

        // assert junction table
        let component_token_junction = schema::protocol_component_holds_token::table
            .select((
                schema::protocol_component_holds_token::protocol_component_id,
                schema::protocol_component_holds_token::token_id,
            ))
            .filter(
                schema::protocol_component_holds_token::protocol_component_id.eq(inserted_data.id),
            )
            .first::<(i64, i64)>(&mut conn)
            .await
            .unwrap();

        assert_eq!(component_token_junction.0, inserted_data.id);

        let token = schema::token::table
            .select(schema::token::all_columns)
            .filter(schema::token::id.eq(component_token_junction.1))
            .load::<orm::Token>(&mut conn)
            .await;

        assert!(token.is_ok());

        // assert component-contract junction table
        let component_contract_junction = schema::protocol_component_holds_contract::table
            .select((
                schema::protocol_component_holds_contract::protocol_component_id,
                schema::protocol_component_holds_contract::contract_code_id,
            ))
            .filter(
                schema::protocol_component_holds_contract::protocol_component_id
                    .eq(inserted_data.id),
            )
            .first::<(i64, i64)>(&mut conn)
            .await
            .unwrap();

        assert_eq!(component_contract_junction.0, inserted_data.id);

        let contract = schema::contract_code::table
            .select(schema::contract_code::all_columns)
            .filter(schema::contract_code::id.eq(component_contract_junction.1))
            .load::<orm::ContractCode>(&mut conn)
            .await;

        assert!(contract.is_ok())
    }

    fn create_test_protocol_component(id: &str) -> ProtocolComponent {
        ProtocolComponent {
            id: id.to_string(),
            protocol_system: "ambient".to_string(),
            protocol_type_name: "type_id_1".to_string(),
            chain: Chain::Ethereum,
            tokens: vec![],
            contract_ids: vec![],
            static_attributes: HashMap::new(),
            change: ChangeType::Creation,
            creation_tx: H256::from_low_u64_be(
                0x0000000000000000000000000000000000000000000000000000000011121314,
            ),
            created_at: NaiveDateTime::from_timestamp_opt(1000, 0).unwrap(),
        }
    }

    #[tokio::test]

    async fn test_delete_protocol_components() {
        let mut conn = setup_db().await;
        setup_data(&mut conn).await;
        let gw = EVMGateway::from_connection(&mut conn).await;

        let test_components = vec![
            create_test_protocol_component("state1"),
            create_test_protocol_component("state2"),
        ];

        let res = gw
            .delete_protocol_components(
                &test_components
                    .iter()
                    .collect::<Vec<_>>(),
                Utc::now().naive_utc(),
                &mut conn,
            )
            .await;

        assert!(res.is_ok());
        let pc_ids: Vec<String> = test_components
            .iter()
            .map(|test_pc| test_pc.id.to_string())
            .collect();

        let updated_timestamps = schema::protocol_component::table
            .filter(schema::protocol_component::external_id.eq_any(pc_ids))
            .select(schema::protocol_component::deleted_at)
            .load::<Option<NaiveDateTime>>(&mut conn)
            .await
            .unwrap();

        assert_eq!(updated_timestamps.len(), 2);
        updated_timestamps
            .into_iter()
            .for_each(|ts| assert!(ts.is_some(), "Found None in updated_ts"));
    }
    #[rstest]
    #[case::get_one(Some("zigzag".to_string()))]
    #[case::get_none(Some("ambient".to_string()))]
    #[tokio::test]

    async fn test_get_protocol_components_with_system_only(#[case] system: Option<String>) {
        let mut conn = setup_db().await;
        let tx_hashes = setup_data(&mut conn).await;
        let gw = EVMGateway::from_connection(&mut conn).await;

        let chain = Chain::Starknet;

        let result = gw
            .get_protocol_components(&chain, system.clone(), None, None, &mut conn)
            .await;

        assert!(result.is_ok());

        match system.unwrap().as_str() {
            "zigzag" => {
                let components = result.unwrap();
                assert_eq!(components.len(), 1);

                let pc = &components[0];
                assert_eq!(pc.id, "state2".to_string());
                assert_eq!(pc.protocol_system, "zigzag");
                assert_eq!(pc.chain, Chain::Starknet);
                assert_eq!(pc.creation_tx, H256::from_str(tx_hashes.get(1).unwrap()).unwrap());
            }
            "ambient" => {
                let components = result.unwrap();
                assert_eq!(components.len(), 0)
            }
            _ => {}
        }
    }

    #[rstest]
    #[case::get_one("state1".to_string())]
    #[case::get_none("state2".to_string())]
    #[tokio::test]

    async fn test_get_protocol_components_with_external_id_only(#[case] external_id: String) {
        let mut conn = setup_db().await;
        let tx_hashes = setup_data(&mut conn).await;
        let gw = EVMGateway::from_connection(&mut conn).await;

        let temp_ids_array = [external_id.as_str()];
        let ids = Some(temp_ids_array.as_slice());
        let chain = Chain::Ethereum;

        let result = gw
            .get_protocol_components(&chain, None, ids, None, &mut conn)
            .await;

        match external_id.as_str() {
            "state1" => {
                let components = result.unwrap();
                assert_eq!(components.len(), 1);

                let pc = &components[0];
                assert_eq!(pc.id, external_id.to_string());
                assert_eq!(pc.protocol_system, "ambient");
                assert_eq!(pc.chain, Chain::Ethereum);
                assert_eq!(pc.creation_tx, H256::from_str(&tx_hashes[0].to_string()).unwrap());
            }
            "state2" => {
                let components = result.unwrap();
                assert_eq!(components.len(), 0)
            }
            _ => {}
        }
    }

    #[tokio::test]
    async fn test_get_protocol_components_with_system_and_ids() {
        let mut conn = setup_db().await;
        let tx_hashes = setup_data(&mut conn).await;
        let gw = EVMGateway::from_connection(&mut conn).await;

        let system = "ambient".to_string();
        let ids = Some(["state1", "state2"].as_slice());
        let chain = Chain::Ethereum;
        let result = gw
            .get_protocol_components(&chain, Some(system), ids, None, &mut conn)
            .await;

        let components = result.unwrap();
        assert_eq!(components.len(), 1);

        let pc = &components[0];
        assert_eq!(pc.id, "state1".to_string());
        assert_eq!(pc.protocol_system, "ambient");
        assert_eq!(pc.chain, Chain::Ethereum);
        assert_eq!(pc.creation_tx, H256::from_str(&tx_hashes[0].to_string()).unwrap());
    }

    #[rstest]
    #[case::get_one(Chain::Ethereum, 0)]
    #[case::get_none(Chain::Starknet, 1)]
    #[tokio::test]
    async fn test_get_protocol_components_with_chain_filter(#[case] chain: Chain, #[case] i: i64) {
        let mut conn = setup_db().await;
        let tx_hashes = setup_data(&mut conn).await;
        let gw = EVMGateway::from_connection(&mut conn).await;

        let mut components = gw
            .get_protocol_components(&chain, None, None, None, &mut conn)
            .await
            .expect("failed retrieving components");
        components.sort_by(|a, b| a.id.cmp(&b.id));

        // there are two eth and one stark component.
        assert!(!components.is_empty());
        let pc = &components[0];
        assert_eq!(pc.id, format!("state{}", i + 1).to_string());
        assert_eq!(pc.chain, chain);
        let i_usize: usize = i as usize;
        assert_eq!(pc.creation_tx, H256::from_str(&tx_hashes[i_usize].to_string()).unwrap());
        match chain {
            Chain::Ethereum => {
                assert_eq!(
                    pc.tokens,
                    vec![H160::from_str(WETH).unwrap(), H160::from_str(USDC).unwrap()]
                )
            }
            Chain::Starknet => {
                assert_eq!(
                    pc.tokens,
                    vec![H160::from_str(USDC).unwrap(), H160::from_str(LUSD).unwrap()]
                )
            }
            _ => panic!("Unexpected chain returned"),
        };
        assert!(
            pc.contract_ids
                .contains(&H160::from_str(WETH).unwrap()),
            "ProtocolComponent is missing WETH contract. Check the tests' data setup"
        );
    }

    #[tokio::test]
    async fn test_get_token_prices() {
        let mut conn = setup_db().await;
        let _ = setup_data(&mut conn).await;
        let gw = EVMGateway::from_connection(&mut conn).await;
        let exp = [(Bytes::from(WETH), 1.0), (Bytes::from(USDC), 0.005)]
            .into_iter()
            .collect::<HashMap<_, _>>();

        let prices = gw
            .get_token_prices(Chain::Ethereum, &mut conn)
            .await
            .expect("retrieving token prices failed!");

        assert_eq!(prices, exp);
    }
}
