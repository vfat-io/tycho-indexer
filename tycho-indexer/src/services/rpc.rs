//! This module contains Tycho RPC implementation

use crate::{
    extractor::evm,
    models::Chain,
    storage::{
        self, Address, BlockIdentifier, BlockOrTimestamp, ContractStateGateway, StorageError,
    },
};
use tycho_types::{dto::ProtocolComponentRequestParameters, Bytes};

use actix_web::{web, HttpResponse};
use diesel_async::{
    pooled_connection::deadpool::{self, Pool},
    AsyncPgConnection,
};
use std::{collections::HashSet, sync::Arc};
use thiserror::Error;
use tracing::{debug, error, info, instrument};

use crate::storage::ProtocolGateway;
use tycho_types::{
    dto,
    dto::{ResponseToken, StateRequestParameters},
};

use super::EvmPostgresGateway;

#[derive(Error, Debug)]
pub enum RpcError {
    #[error("Failed to parse JSON: {0}")]
    Parse(String),

    #[error("Failed to get storage: {0}")]
    Storage(#[from] StorageError),

    #[error("Failed to get database connection: {0}")]
    Connection(#[from] deadpool::PoolError),
}

impl From<evm::Account> for dto::ResponseAccount {
    fn from(value: evm::Account) -> Self {
        dto::ResponseAccount::new(
            value.chain.into(),
            value.address.into(),
            value.title.clone(),
            value
                .slots
                .into_iter()
                .map(|(k, v)| (Bytes::from(k), Bytes::from(v)))
                .collect(),
            Bytes::from(value.balance),
            value.code,
            Bytes::from(value.code_hash),
            Bytes::from(value.balance_modify_tx),
            Bytes::from(value.code_modify_tx),
            value.creation_tx.map(Bytes::from),
        )
    }
}

impl From<evm::ProtocolState> for dto::ResponseProtocolState {
    fn from(protocol_state: evm::ProtocolState) -> Self {
        Self {
            component_id: protocol_state.component_id,
            attributes: protocol_state.attributes,
            modify_tx: protocol_state.modify_tx.into(),
        }
    }
}

impl From<evm::ProtocolStateDelta> for dto::ProtocolStateDelta {
    fn from(protocol_state: evm::ProtocolStateDelta) -> Self {
        Self {
            component_id: protocol_state.component_id,
            updated_attributes: protocol_state.updated_attributes,
            deleted_attributes: protocol_state.deleted_attributes,
        }
    }
}

impl From<evm::AccountUpdate> for dto::AccountUpdate {
    fn from(account_update: evm::AccountUpdate) -> Self {
        Self {
            address: account_update.address.into(),
            chain: account_update.chain.into(),
            slots: account_update
                .slots
                .into_iter()
                .map(|(k, v)| (Bytes::from(k), Bytes::from(v)))
                .collect(),
            balance: account_update.balance.map(Bytes::from),
            code: account_update.code,
            change: account_update.change.into(),
        }
    }
}

impl From<storage::ChangeType> for dto::ChangeType {
    fn from(value: storage::ChangeType) -> Self {
        match value {
            storage::ChangeType::Update => dto::ChangeType::Update,
            storage::ChangeType::Creation => dto::ChangeType::Creation,
            storage::ChangeType::Deletion => dto::ChangeType::Deletion,
        }
    }
}

impl From<dto::Chain> for Chain {
    fn from(value: dto::Chain) -> Self {
        match value {
            dto::Chain::Ethereum => Chain::Ethereum,
            dto::Chain::Starknet => Chain::Starknet,
            dto::Chain::ZkSync => Chain::ZkSync,
        }
    }
}

impl From<Chain> for dto::Chain {
    fn from(value: Chain) -> Self {
        match value {
            Chain::Ethereum => dto::Chain::Ethereum,
            Chain::Starknet => dto::Chain::Starknet,
            Chain::ZkSync => dto::Chain::ZkSync,
        }
    }
}

impl From<evm::ERC20Token> for ResponseToken {
    fn from(token: evm::ERC20Token) -> Self {
        Self {
            address: token.address.into(),
            symbol: token.symbol,
            decimals: token.decimals,
            tax: token.tax,
            chain: token.chain.into(),
            gas: token.gas,
        }
    }
}

impl From<evm::ProtocolComponent> for dto::ProtocolComponent {
    fn from(protocol_component: evm::ProtocolComponent) -> Self {
        Self {
            chain: protocol_component.chain.into(),
            id: protocol_component.id,
            protocol_system: protocol_component.protocol_system,
            protocol_type_name: protocol_component.protocol_type_name,
            tokens: protocol_component
                .tokens
                .into_iter()
                .map(|token| {
                    let bytes = token.as_bytes().to_vec();
                    Bytes::from(bytes)
                })
                .collect(),
            contract_ids: protocol_component
                .contract_ids
                .into_iter()
                .map(|h| {
                    let bytes = h.as_bytes().to_vec();
                    Bytes::from(bytes)
                })
                .collect(),
            static_attributes: protocol_component.static_attributes,
            creation_tx: protocol_component.creation_tx.into(),
            created_at: protocol_component.created_at,
            change: protocol_component.change.into(),
        }
    }
}

impl TryFrom<&dto::VersionParam> for BlockOrTimestamp {
    type Error = RpcError;

    fn try_from(version: &dto::VersionParam) -> Result<Self, Self::Error> {
        match (&version.timestamp, &version.block) {
            (_, Some(block)) => {
                // If a full block is provided, we prioritize hash over number and chain
                let block_identifier = match (&block.hash, &block.chain, &block.number) {
                    (Some(hash), _, _) => BlockIdentifier::Hash(hash.clone()),
                    (_, Some(chain), Some(number)) => {
                        BlockIdentifier::Number((Chain::from(*chain), *number))
                    }
                    _ => return Err(RpcError::Parse("Insufficient block information".to_owned())),
                };
                Ok(BlockOrTimestamp::Block(block_identifier))
            }
            (Some(timestamp), None) => Ok(BlockOrTimestamp::Timestamp(*timestamp)),
            (None, None) => {
                Err(RpcError::Parse("Missing timestamp or block identifier".to_owned()))
            }
        }
    }
}

pub struct RpcHandler {
    db_gateway: Arc<EvmPostgresGateway>,
    db_connection_pool: Pool<AsyncPgConnection>,
}

impl RpcHandler {
    pub fn new(
        db_gateway: Arc<EvmPostgresGateway>,
        db_connection_pool: Pool<AsyncPgConnection>,
    ) -> Self {
        Self { db_gateway, db_connection_pool }
    }

    #[instrument(skip(self, chain, request, params))]
    async fn get_contract_state(
        &self,
        chain: &Chain,
        request: &dto::StateRequestBody,
        params: &dto::StateRequestParameters,
    ) -> Result<dto::StateRequestResponse, RpcError> {
        let mut conn = self.db_connection_pool.get().await?;

        info!(?chain, ?request, ?params, "Getting contract state.");
        self.get_contract_state_inner(chain, request, params, &mut conn)
            .await
    }

    async fn get_contract_state_inner(
        &self,
        chain: &Chain,
        request: &dto::StateRequestBody,
        params: &dto::StateRequestParameters,
        db_connection: &mut AsyncPgConnection,
    ) -> Result<dto::StateRequestResponse, RpcError> {
        #![allow(unused_variables)]
        //TODO: handle when no contract is specified with filters
        let at = BlockOrTimestamp::try_from(&request.version)?;

        let version = storage::Version(at, storage::VersionKind::Last);

        // Get the contract IDs from the request
        let contract_ids = request.contract_ids.clone();
        let addresses: Option<Vec<Address>> = contract_ids.map(|ids| {
            ids.into_iter()
                .map(|id| Address::from(id.address))
                .collect::<Vec<Address>>()
        });
        debug!(?addresses, "Getting contract states.");
        let addresses = addresses.as_deref();

        // Get the contract states from the database
        // TODO support additional tvl_gt and intertia_min_gt filters
        match self
            .db_gateway
            .get_contracts(chain, addresses, Some(&version), true, db_connection)
            .await
        {
            Ok(accounts) => Ok(dto::StateRequestResponse::new(
                accounts
                    .into_iter()
                    .map(dto::ResponseAccount::from)
                    .collect(),
            )),
            Err(err) => {
                error!(error = %err, "Error while getting contract states.");
                Err(err.into())
            }
        }
    }

    #[instrument(skip(self, chain, request, params))]
    async fn get_contract_delta(
        &self,
        chain: &Chain,
        request: &dto::ContractDeltaRequestBody,
        params: &dto::StateRequestParameters,
    ) -> Result<dto::ContractDeltaRequestResponse, RpcError> {
        let mut conn = self.db_connection_pool.get().await?;

        info!(?request, ?params, "Getting contract delta.");
        self.get_contract_delta_inner(chain, request, params, &mut conn)
            .await
    }

    async fn get_contract_delta_inner(
        &self,
        chain: &Chain,
        request: &dto::ContractDeltaRequestBody,
        params: &dto::StateRequestParameters,
        db_connection: &mut AsyncPgConnection,
    ) -> Result<dto::ContractDeltaRequestResponse, RpcError> {
        #![allow(unused_variables)]
        //TODO: handle when no contract is specified with filters
        let start = BlockOrTimestamp::try_from(&request.start)?;
        let end = BlockOrTimestamp::try_from(&request.end)?;

        // Get the contract IDs from the request
        let contract_ids = request.contract_ids.clone();
        let addresses: Option<HashSet<Address>> = contract_ids.map(|ids| {
            ids.into_iter()
                .map(|id| id.address)
                .collect::<HashSet<Address>>()
        });
        debug!(?addresses, "Getting contract states.");
        let addresses: Option<&HashSet<Address>> = addresses.as_ref();

        // Get the contract deltas from the database
        match self
            .db_gateway
            .get_accounts_delta(chain, Some(&start), &end, db_connection)
            .await
        {
            Ok(mut accounts) => {
                // Filter by contract addresses if specified in the request
                // PERF: This is not efficient, we should filter in the query
                if let Some(contract_addrs) = addresses {
                    accounts.retain(|acc| {
                        let address = Address::from(acc.address);
                        contract_addrs.contains(&address)
                    });
                }
                Ok(dto::ContractDeltaRequestResponse::new(
                    accounts
                        .into_iter()
                        .map(dto::AccountUpdate::from)
                        .collect(),
                ))
            }
            Err(err) => {
                error!(error = %err, "Error while getting contract delta.");
                Err(err.into())
            }
        }
    }

    #[instrument(skip(self, request, params))]
    async fn get_protocol_state(
        &self,
        chain: &Chain,
        request: &dto::ProtocolStateRequestBody,
        params: &dto::StateRequestParameters,
    ) -> Result<dto::ProtocolStateRequestResponse, RpcError> {
        let mut conn = self.db_connection_pool.get().await?;

        info!(?request, ?params, "Getting protocol state.");
        self.get_protocol_state_inner(chain, request, params, &mut conn)
            .await
    }

    async fn get_protocol_state_inner(
        &self,
        chain: &Chain,
        request: &dto::ProtocolStateRequestBody,
        params: &dto::StateRequestParameters,
        db_connection: &mut AsyncPgConnection,
    ) -> Result<dto::ProtocolStateRequestResponse, RpcError> {
        #![allow(unused_variables)]
        //TODO: handle when no id is specified with filters
        let at = BlockOrTimestamp::try_from(&request.version)?;

        let version = storage::Version(at, storage::VersionKind::Last);

        // Get the protocol IDs from the request
        let protocol_ids: Option<Vec<dto::ProtocolId>> = request.protocol_ids.clone();
        let ids: Option<Vec<&str>> = protocol_ids.as_ref().map(|ids| {
            ids.iter()
                .map(|id| id.id.as_str())
                .collect::<Vec<&str>>()
        });
        debug!(?ids, "Getting protocol states.");
        let ids = ids.as_deref();

        // Get the protocol states from the database
        match self
            .db_gateway
            .get_protocol_states(
                chain,
                Some(version),
                request.protocol_system.clone(),
                ids,
                db_connection,
            )
            .await
        {
            Ok(accounts) => Ok(dto::ProtocolStateRequestResponse::new(
                accounts
                    .into_iter()
                    .map(dto::ResponseProtocolState::from)
                    .collect(),
            )),
            Err(err) => {
                error!(error = %err, "Error while getting protocol states.");
                Err(err.into())
            }
        }
    }

    async fn get_tokens(
        &self,
        chain: &Chain,
        request: &dto::TokensRequestBody,
    ) -> Result<dto::TokensRequestResponse, RpcError> {
        let mut conn = self.db_connection_pool.get().await?;

        info!(?chain, ?request, "Getting tokens.");
        self.get_tokens_inner(chain, request, &mut conn)
            .await
    }

    async fn get_tokens_inner(
        &self,
        chain: &Chain,
        request: &dto::TokensRequestBody,
        db_connection: &mut AsyncPgConnection,
    ) -> Result<dto::TokensRequestResponse, RpcError> {
        let address_refs: Option<Vec<&Address>> = request
            .token_addresses
            .as_ref()
            .map(|vec| vec.iter().collect());
        let addresses_slice = address_refs.as_deref();
        debug!(?addresses_slice, "Getting tokens.");

        match self
            .db_gateway
            .get_tokens(*chain, addresses_slice, db_connection)
            .await
        {
            Ok(tokens) => Ok(dto::TokensRequestResponse::new(
                tokens
                    .into_iter()
                    .map(dto::ResponseToken::from)
                    .collect(),
            )),
            Err(err) => {
                error!(error = %err, "Error while getting tokens.");
                Err(err.into())
            }
        }
    }

    async fn get_protocol_components(
        &self,
        chain: &Chain,
        request: &dto::ProtocolComponentsRequestBody,
        params: &dto::ProtocolComponentRequestParameters,
    ) -> Result<dto::ProtocolComponentRequestResponse, RpcError> {
        let mut conn = self.db_connection_pool.get().await?;

        info!(?chain, ?request, "Getting tokens.");
        self.get_protocol_components_inner(chain, request, params, &mut conn)
            .await
    }

    async fn get_protocol_components_inner(
        &self,
        chain: &Chain,
        request: &dto::ProtocolComponentsRequestBody,
        params: &dto::ProtocolComponentRequestParameters,
        db_connection: &mut AsyncPgConnection,
    ) -> Result<dto::ProtocolComponentRequestResponse, RpcError> {
        #![allow(unused_variables)]
        let system = request.protocol_system.clone();
        let ids_strs: Option<Vec<&str>> = request
            .component_ids
            .as_ref()
            .map(|vec| vec.iter().map(AsRef::as_ref).collect());

        let ids_slice = ids_strs.as_deref();
        match self
            .db_gateway
            .get_protocol_components(chain, system, ids_slice, db_connection)
            .await
        {
            Ok(components) => Ok(dto::ProtocolComponentRequestResponse::new(
                components
                    .into_iter()
                    .map(dto::ProtocolComponent::from)
                    .collect(),
            )),
            Err(err) => {
                error!(error = %err, "Error while getting protocol components.");
                Err(err.into())
            }
        }
    }

    #[instrument(skip(self, chain, request, params))]
    async fn get_protocol_delta(
        &self,
        chain: &Chain,
        request: &dto::ProtocolDeltaRequestBody,
        params: &dto::StateRequestParameters,
    ) -> Result<dto::ProtocolDeltaRequestResponse, RpcError> {
        let mut conn = self.db_connection_pool.get().await?;

        info!(?request, ?params, "Getting protocol delta.");
        self.get_protocol_delta_inner(chain, request, params, &mut conn)
            .await
    }

    async fn get_protocol_delta_inner(
        &self,
        chain: &Chain,
        request: &dto::ProtocolDeltaRequestBody,
        params: &dto::StateRequestParameters,
        db_connection: &mut AsyncPgConnection,
    ) -> Result<dto::ProtocolDeltaRequestResponse, RpcError> {
        #![allow(unused_variables)]
        //TODO: handle when no contract is specified with filters
        let start = BlockOrTimestamp::try_from(&request.start)?;
        let end = BlockOrTimestamp::try_from(&request.end)?;

        // Get the components IDs from the request
        let ids = request.component_ids.clone();
        let component_ids: Option<HashSet<String>> = ids.map(|ids| {
            ids.into_iter()
                .collect::<HashSet<String>>()
        });
        debug!(?component_ids, "Getting protocol states delta.");
        let component_ids: Option<&HashSet<String>> = component_ids.as_ref();

        // Get the contract deltas from the database
        match self
            .db_gateway
            .get_protocol_states_delta(chain, Some(&start), &end, db_connection)
            .await
        {
            Ok(mut components) => {
                // Filter by contract addresses if specified in the request
                // PERF: This is not efficient, we should filter in the query
                if let Some(component_ids) = component_ids {
                    components.retain(|acc| {
                        let id: String = acc.component_id.clone();
                        component_ids.contains(&id)
                    });
                }
                Ok(dto::ProtocolDeltaRequestResponse::new(
                    components
                        .into_iter()
                        .map(dto::ProtocolStateDelta::from)
                        .collect(),
                ))
            }
            Err(err) => {
                error!(error = %err, "Error while getting protocol state delta.");
                Err(err.into())
            }
        }
    }
}

#[utoipa::path(
    post,
    path = "/v1/{execution_env}/contract_state",
    responses(
        (status = 200, description = "OK", body = StateRequestResponse),
    ),
    request_body = StateRequestBody,
    params(
        ("execution_env" = Chain, description = "Execution environment"),
        StateRequestParameters
    ),
)]
pub async fn contract_state(
    execution_env: web::Path<Chain>,
    query: web::Query<dto::StateRequestParameters>,
    body: web::Json<dto::StateRequestBody>,
    handler: web::Data<RpcHandler>,
) -> HttpResponse {
    // Call the handler to get the state
    let response = handler
        .into_inner()
        .get_contract_state(&execution_env, &body, &query)
        .await;

    match response {
        Ok(state) => HttpResponse::Ok().json(state),
        Err(err) => {
            error!(error = %err, ?body, ?query, "Error while getting contract state.");
            HttpResponse::InternalServerError().finish()
        }
    }
}

#[utoipa::path(
    post,
    path = "/v1/{execution_env}/contract_delta",
    responses(
        (status = 200, description = "OK", body = ContractDeltaRequestResponse),
    ),
    request_body = ContractDeltaRequestBody,
    params(
        ("execution_env" = Chain, description = "Execution environment"),
        StateRequestParameters
    ),
)]
pub async fn contract_delta(
    execution_env: web::Path<Chain>,
    params: web::Query<dto::StateRequestParameters>,
    body: web::Json<dto::ContractDeltaRequestBody>,
    handler: web::Data<RpcHandler>,
) -> HttpResponse {
    // Call the handler to get the state delta
    let response = handler
        .into_inner()
        .get_contract_delta(&execution_env, &body, &params)
        .await;

    match response {
        Ok(state) => HttpResponse::Ok().json(state),
        Err(err) => {
            error!(error = %err, ?body, "Error while getting contract state delta.");
            HttpResponse::InternalServerError().finish()
        }
    }
}

#[utoipa::path(
    post,
    path = "/v1/{execution_env}/tokens",
    responses(
        (status = 200, description = "OK", body = TokensRequestResponse),
    ),
    request_body = TokensRequestBody,
    params(
        ("execution_env" = Chain, description = "Execution environment"),
    ),
)]
pub async fn tokens(
    execution_env: web::Path<Chain>,
    body: web::Json<dto::TokensRequestBody>,
    handler: web::Data<RpcHandler>,
) -> HttpResponse {
    // Call the handler to get tokens
    let response = handler
        .into_inner()
        .get_tokens(&execution_env, &body)
        .await;

    match response {
        Ok(state) => HttpResponse::Ok().json(state),
        Err(err) => {
            error!(error = %err, ?body, "Error while getting tokens.");
            HttpResponse::InternalServerError().finish()
        }
    }
}

#[utoipa::path(
    post,
    path = "/v1/{execution_env}/protocol_components",
    responses(
        (status = 200, description = "OK", body = ProtocolComponentRequestResponse),
    ),
    request_body = ProtocolComponentsRequestBody,
    params(
        ("execution_env" = Chain, description = "Execution environment"),
        ProtocolComponentRequestParameters
    ),
)]
pub async fn protocol_components(
    execution_env: web::Path<Chain>,
    body: web::Json<dto::ProtocolComponentsRequestBody>,
    params: web::Query<dto::ProtocolComponentRequestParameters>,
    handler: web::Data<RpcHandler>,
) -> HttpResponse {
    // Call the handler to get tokens
    let response = handler
        .into_inner()
        .get_protocol_components(&execution_env, &body, &params)
        .await;

    match response {
        Ok(state) => HttpResponse::Ok().json(state),
        Err(err) => {
            error!(error = %err, ?body, "Error while getting tokens.");
            HttpResponse::InternalServerError().finish()
        }
    }
}

#[utoipa::path(
    post,
    path = "/v1/{execution_env}/protocol_state",
    responses(
        (status = 200, description = "OK", body = ProtocolStateRequestResponse),
    ),
    request_body = ProtocolStateRequestBody,
    params(
        ("execution_env" = Chain, description = "Execution environment"),
        StateRequestParameters
    ),
)]
pub async fn protocol_state(
    execution_env: web::Path<Chain>,
    body: web::Json<dto::ProtocolStateRequestBody>,
    params: web::Query<dto::StateRequestParameters>,
    handler: web::Data<RpcHandler>,
) -> HttpResponse {
    // Call the handler to get protocol states
    let response = handler
        .into_inner()
        .get_protocol_state(&execution_env, &body, &params)
        .await;

    match response {
        Ok(state) => HttpResponse::Ok().json(state),
        Err(err) => {
            error!(error = %err, ?body, "Error while getting protocol states.");
            HttpResponse::InternalServerError().finish()
        }
    }
}

#[utoipa::path(
    post,
    path = "/v1/{execution_env}/protocol_delta",
    responses(
        (status = 200, description = "OK", body = ProtocolDeltaRequestResponse),
    ),
    request_body = ProtocolDeltaRequestBody,
    params(
        ("execution_env" = Chain, description = "Execution environment"),
        StateRequestParameters
    ),
)]
pub async fn protocol_delta(
    execution_env: web::Path<Chain>,
    body: web::Json<dto::ProtocolDeltaRequestBody>,
    params: web::Query<dto::StateRequestParameters>,
    handler: web::Data<RpcHandler>,
) -> HttpResponse {
    // Call the handler to get protocol deltas
    let response = handler
        .into_inner()
        .get_protocol_delta(&execution_env, &body, &params)
        .await;

    match response {
        Ok(state) => HttpResponse::Ok().json(state),
        Err(err) => {
            error!(error = %err, ?body, "Error while getting protocol deltas.");
            HttpResponse::InternalServerError().finish()
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::postgres::{self, db_fixtures, schema};
    use actix_web::test;
    use chrono::Utc;
    use diesel::{prelude::*, ExpressionMethods};
    use diesel_async::AsyncConnection;
    use ethers::types::{H160, U256};
    use tycho_types::Bytes;

    use std::{collections::HashMap, str::FromStr, sync::Arc};

    use self::storage::postgres::orm;

    use crate::{
        extractor::evm::{self},
        storage::BlockIdentifier,
    };

    use ethers::prelude::H256;

    use diesel_async::{AsyncPgConnection, RunQueryDsl};

    use crate::{models::Chain, storage::BlockOrTimestamp};

    use super::*;

    const WETH: &str = "C02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2";
    const USDC: &str = "A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48";
    const USDT: &str = "dAC17F958D2ee523a2206206994597C13D831ec7";
    const DAI: &str = "6B175474E89094C44Da98b954EedeAC495271d0F";

    #[test]
    async fn test_validate_version_priority() {
        let json_str = r#"
    {
        "version": {
            "timestamp": "2069-01-01T04:20:00",
            "block": {
                "hash": "0x24101f9cb26cd09425b52da10e8c2f56ede94089a8bbe0f31f1cda5f4daa52c4",
                "parentHash": "0x8d75152454e60413efe758cc424bfd339897062d7e658f302765eb7b50971815",
                "number": 213,
                "chain": "ethereum"
            }
        }
    }
    "#;

        let body: dto::StateRequestBody = serde_json::from_str(json_str).unwrap();

        let version = BlockOrTimestamp::try_from(&body.version).unwrap();
        assert_eq!(
            version,
            BlockOrTimestamp::Block(BlockIdentifier::Hash(
                Bytes::from_str("24101f9cb26cd09425b52da10e8c2f56ede94089a8bbe0f31f1cda5f4daa52c4")
                    .unwrap()
            ))
        );
    }

    #[test]
    async fn test_validate_version_with_block_number() {
        let json_str = r#"
    {
        "version": {
            "block": {
                "number": 213,
                "chain": "ethereum"
            }
        }
    }
    "#;

        let body: dto::StateRequestBody =
            serde_json::from_str(json_str).expect("serde parsing error");

        let version = BlockOrTimestamp::try_from(&body.version).expect("nor block nor timestamp");
        assert_eq!(
            version,
            BlockOrTimestamp::Block(BlockIdentifier::Number((Chain::Ethereum, 213)))
        );
    }

    #[test]
    async fn test_parse_state_request_no_version_specified() {
        let json_str = r#"
    {
        "contractIds": [
            {
                "address": "0xb4eccE46b8D4e4abFd03C9B806276A6735C9c092",
                "chain": "ethereum"
            }
        ]
    }
    "#;

        let result: dto::StateRequestBody = serde_json::from_str(json_str).unwrap();

        let contract0 = "b4eccE46b8D4e4abFd03C9B806276A6735C9c092".into();

        let expected = dto::StateRequestBody {
            contract_ids: Some(vec![dto::ContractId::new(dto::Chain::Ethereum, contract0)]),
            version: dto::VersionParam { timestamp: Some(Utc::now().naive_utc()), block: None },
        };

        let time_difference = expected
            .version
            .timestamp
            .unwrap()
            .timestamp_millis() -
            result
                .version
                .timestamp
                .unwrap()
                .timestamp_millis();

        // Allowing a small time delta (1 second)
        assert!(time_difference <= 1000);
        assert_eq!(result.contract_ids, expected.contract_ids);
        assert_eq!(result.version.block, expected.version.block);
    }

    pub async fn setup_contract_data(conn: &mut AsyncPgConnection) -> String {
        // Adds fixtures: chain, block, transaction, account, account_balance
        let acc_address = "6B175474E89094C44Da98b954EedeAC495271d0F";

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
                // ----- Block 01 LAST
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
                // ----- Block 02 LAST
            ],
        )
        .await;

        // Account C0
        let c0 = db_fixtures::insert_account(conn, acc_address, "account0", chain_id, Some(txn[0]))
            .await;
        db_fixtures::insert_account_balance(conn, 0, txn[0], Some("2020-01-01T00:00:00"), c0).await;
        db_fixtures::insert_contract_code(conn, c0, txn[0], Bytes::from_str("C0C0C0").unwrap())
            .await;
        db_fixtures::insert_account_balance(conn, 100, txn[1], Some("2020-01-01T01:00:00"), c0)
            .await;
        // Slot 2 is never modified again
        db_fixtures::insert_slots(conn, c0, txn[1], "2020-01-01T00:00:00", None, &[(2, 1, None)])
            .await;
        // First version for slots 0 and 1.
        db_fixtures::insert_slots(
            conn,
            c0,
            txn[1],
            "2020-01-01T00:00:00",
            Some("2020-01-01T01:00:00"),
            &[(0, 1, None), (1, 5, None)],
        )
        .await;
        db_fixtures::insert_account_balance(conn, 101, txn[3], None, c0).await;
        // Second and final version for 0 and 1, new slots 5 and 6
        db_fixtures::insert_slots(
            conn,
            c0,
            txn[3],
            "2020-01-01T01:00:00",
            None,
            &[(0, 2, Some(1)), (1, 3, Some(5)), (5, 25, None), (6, 30, None)],
        )
        .await;

        // Account C1
        let c1 = db_fixtures::insert_account(
            conn,
            "73BcE791c239c8010Cd3C857d96580037CCdd0EE",
            "c1",
            chain_id,
            Some(txn[2]),
        )
        .await;
        db_fixtures::insert_account_balance(conn, 50, txn[2], None, c1).await;
        db_fixtures::insert_contract_code(conn, c1, txn[2], Bytes::from_str("C1C1C1").unwrap())
            .await;
        db_fixtures::insert_slots(
            conn,
            c1,
            txn[3],
            "2020-01-01T01:00:00",
            None,
            &[(0, 128, None), (1, 255, None)],
        )
        .await;

        // Account C2
        let c2 = db_fixtures::insert_account(
            conn,
            "94a3F312366b8D0a32A00986194053C0ed0CdDb1",
            "c2",
            chain_id,
            Some(txn[1]),
        )
        .await;
        db_fixtures::insert_account_balance(conn, 25, txn[1], None, c2).await;
        db_fixtures::insert_contract_code(conn, c2, txn[1], Bytes::from_str("C2C2C2").unwrap())
            .await;
        db_fixtures::insert_slots(
            conn,
            c2,
            txn[1],
            "2020-01-01T00:00:00",
            None,
            &[(1, 2, None), (2, 4, None)],
        )
        .await;
        db_fixtures::delete_account(conn, c2, "2020-01-01T01:00:00").await;
        acc_address.to_string()
    }

    #[tokio::test]
    async fn test_get_state() {
        let db_url = std::env::var("DATABASE_URL").unwrap();
        let pool = postgres::connect(&db_url)
            .await
            .unwrap();
        let cloned_pool = pool.clone();
        let mut conn = cloned_pool.get().await.unwrap();
        conn.begin_test_transaction()
            .await
            .unwrap();
        let acc_address = setup_contract_data(&mut conn).await;

        let db_gateway = Arc::new(EvmPostgresGateway::from_connection(&mut conn).await);
        let req_handler = RpcHandler::new(db_gateway, pool);

        let expected = evm::Account {
            chain: Chain::Ethereum,
            address: "0x6b175474e89094c44da98b954eedeac495271d0f"
                .parse()
                .unwrap(),
            title: "account0".to_owned(),
            slots: evm_slots([(6, 30), (5, 25), (1, 3), (2, 1), (0, 2)]),
            balance: U256::from(101),
            code: Bytes::from_str("C0C0C0").unwrap(),
            code_hash: "0x106781541fd1c596ade97569d584baf47e3347d3ac67ce7757d633202061bdc4"
                .parse()
                .unwrap(),
            balance_modify_tx: "0x50449de1973d86f21bfafa7c72011854a7e33a226709dc3e2e4edcca34188388"
                .parse()
                .unwrap(),
            code_modify_tx: "0xbb7e16d797a9e2fbc537e30f91ed3d27a254dd9578aa4c3af3e5f0d3e8130945"
                .parse()
                .unwrap(),
            creation_tx: Some(
                "0xbb7e16d797a9e2fbc537e30f91ed3d27a254dd9578aa4c3af3e5f0d3e8130945"
                    .parse()
                    .unwrap(),
            ),
        };

        let request = dto::StateRequestBody {
            contract_ids: Some(vec![dto::ContractId::new(
                dto::Chain::Ethereum,
                acc_address.parse::<Bytes>().unwrap(),
            )]),
            version: dto::VersionParam { timestamp: Some(Utc::now().naive_utc()), block: None },
        };

        let state = req_handler
            .get_contract_state_inner(
                &Chain::Ethereum,
                &request,
                &dto::StateRequestParameters::default(),
                &mut conn,
            )
            .await
            .unwrap();

        assert_eq!(state.accounts.len(), 1);
        assert_eq!(state.accounts[0], expected.into());
    }

    #[test]
    async fn test_msg() {
        // Define the contract address and endpoint
        let endpoint = "http://127.0.0.1:4242/v1/ethereum/contract_state";

        // Create the request body using the dto::StateRequestBody struct
        let request_body = dto::StateRequestBody {
            contract_ids: Some(vec![dto::ContractId::new(
                dto::Chain::Ethereum,
                Bytes::from_str("b4eccE46b8D4e4abFd03C9B806276A6735C9c092").unwrap(),
            )]),
            version: dto::VersionParam::default(),
        };

        // Serialize the request body to JSON
        let json_data = serde_json::to_string(&request_body).expect("Failed to serialize to JSON");

        // Print the curl command
        println!(
            "curl -X POST -H \"Content-Type: application/json\" -d '{}' {}",
            json_data, endpoint
        );
    }

    pub async fn setup_tokens(conn: &mut AsyncPgConnection) {
        // Adds WETH, USDC and DAI to the DB
        let chain_id = db_fixtures::insert_chain(conn, "ethereum").await;
        db_fixtures::insert_token(conn, chain_id, WETH, "WETH", 18).await;
        db_fixtures::insert_token(conn, chain_id, USDC, "USDC", 6).await;
        db_fixtures::insert_token(conn, chain_id, DAI, "DAI", 18).await;
    }
    #[tokio::test]
    async fn test_get_tokens() {
        let db_url = std::env::var("DATABASE_URL").unwrap();
        let pool = postgres::connect(&db_url)
            .await
            .unwrap();
        let cloned_pool = pool.clone();
        let mut conn = cloned_pool.get().await.unwrap();
        conn.begin_test_transaction()
            .await
            .unwrap();
        setup_tokens(&mut conn).await;

        let db_gateway = Arc::new(EvmPostgresGateway::from_connection(&mut conn).await);
        let req_handler = RpcHandler::new(db_gateway, pool);

        // request for 2 tokens that are in the DB (WETH and USDC)
        let request = dto::TokensRequestBody {
            token_addresses: Some(vec![
                USDC.parse::<Bytes>().unwrap(),
                WETH.parse::<Bytes>().unwrap(),
            ]),
        };

        let tokens = req_handler
            .get_tokens_inner(&Chain::Ethereum, &request, &mut conn)
            .await
            .unwrap();

        assert_eq!(tokens.tokens.len(), 2);
        assert_eq!(tokens.tokens[0].symbol, "USDC");
        assert_eq!(tokens.tokens[1].symbol, "WETH");

        // request for 1 token that is not in the DB (USDT)
        let request =
            dto::TokensRequestBody { token_addresses: Some(vec![USDT.parse::<Bytes>().unwrap()]) };

        let tokens = req_handler
            .get_tokens_inner(&Chain::Ethereum, &request, &mut conn)
            .await
            .unwrap();

        assert_eq!(tokens.tokens.len(), 0);

        // request without any address filter -> should return all tokens
        let request = dto::TokensRequestBody { token_addresses: None };

        let tokens = req_handler
            .get_tokens_inner(&Chain::Ethereum, &request, &mut conn)
            .await
            .unwrap();

        assert_eq!(tokens.tokens.len(), 3);
    }

    pub async fn setup_components(conn: &mut AsyncPgConnection) {
        let chain_id = db_fixtures::insert_chain(conn, "ethereum").await;
        let blk = db_fixtures::insert_blocks(conn, chain_id).await;
        let tx_ids = db_fixtures::insert_txns(
            conn,
            &[(blk[0], 1i64, "0xbb7e16d797a9e2fbc537e30f91ed3d27a254dd9578aa4c3af3e5f0d3e8130945")],
        )
        .await;
        let protocol_system_id =
            db_fixtures::insert_protocol_system(conn, "ambient".to_owned()).await;
        db_fixtures::insert_protocol_system(conn, "curve".to_owned()).await;

        let protocol_type_id =
            db_fixtures::insert_protocol_type(conn, "Pool", None, None, None).await;
        let (_, weth_id) = db_fixtures::insert_token(conn, chain_id, WETH, "WETH", 18).await;
        let (_, usdc_id) = db_fixtures::insert_token(conn, chain_id, USDC, "USDC", 6).await;

        let pool_account = db_fixtures::insert_account(
            conn,
            "aaaaaaaaa24eeeb8d57d431224f73832bc34f688",
            "ambient_pool",
            chain_id,
            Some(tx_ids[0]),
        )
        .await;

        let contract_code_id = db_fixtures::insert_contract_code(
            conn,
            pool_account,
            tx_ids[0],
            Bytes::from_str("C0C0C0").unwrap(),
        )
        .await;

        db_fixtures::insert_protocol_component(
            conn,
            "ambient_USDC_ETH",
            chain_id,
            protocol_system_id,
            protocol_type_id,
            tx_ids[0],
            Option::from(vec![usdc_id, weth_id]),
            Option::from(vec![contract_code_id]),
        )
        .await;
    }

    async fn setup_protocol_data(conn: &mut AsyncPgConnection) -> Vec<String> {
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
        let (_account_id_usdc, _usdc_id) =
            db_fixtures::insert_token(conn, chain_id, USDC.trim_start_matches("0x"), "USDC", 6)
                .await;

        let contract_code_id = db_fixtures::insert_contract_code(
            conn,
            account_id_weth,
            txn[0],
            Bytes::from_str("C0C0C0").unwrap(),
        )
        .await;

        let protocol_component_id = db_fixtures::insert_protocol_component(
            conn,
            "state1",
            chain_id,
            protocol_system_id_ambient,
            protocol_type_id,
            txn[0],
            Some(vec![weth_id]),
            Some(vec![contract_code_id]),
        )
        .await;
        let _protocol_component_id2 = db_fixtures::insert_protocol_component(
            conn,
            "state3",
            chain_id,
            protocol_system_id_ambient,
            protocol_type_id,
            txn[0],
            Some(vec![weth_id]),
            Some(vec![contract_code_id]),
        )
        .await;
        db_fixtures::insert_protocol_component(
            conn,
            "state2",
            chain_id_sn,
            protocol_system_id_zz,
            protocol_type_id,
            txn[1],
            Some(vec![weth_id]),
            Some(vec![contract_code_id]),
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

    fn evm_attributes<'a>(
        data: impl IntoIterator<Item = (&'a str, i32)>,
    ) -> HashMap<String, Bytes> {
        data.into_iter()
            .map(|(s, v)| (s.to_owned(), Bytes::from(U256::from(v))))
            .collect()
    }

    #[tokio::test]
    async fn test_get_protocol_state() {
        let db_url = std::env::var("DATABASE_URL").unwrap();
        let pool = postgres::connect(&db_url)
            .await
            .unwrap();
        let mut conn = pool.get().await.unwrap();
        conn.begin_test_transaction()
            .await
            .unwrap();
        setup_protocol_data(&mut conn).await;

        let db_gateway = Arc::new(EvmPostgresGateway::from_connection(&mut conn).await);
        let req_handler = RpcHandler::new(db_gateway, pool);

        let expected = evm::ProtocolState {
            component_id: "state1".to_owned(),
            attributes: evm_attributes([("reserve1", 1000), ("reserve2", 500)]),
            modify_tx: "0x50449de1973d86f21bfafa7c72011854a7e33a226709dc3e2e4edcca34188388"
                .parse()
                .unwrap(),
        };

        let request = dto::ProtocolStateRequestBody {
            protocol_ids: Some(vec![dto::ProtocolId {
                id: "state1".to_owned(),
                chain: dto::Chain::Ethereum,
            }]),
            protocol_system: None,
            version: dto::VersionParam { timestamp: Some(Utc::now().naive_utc()), block: None },
        };

        let state = req_handler
            .get_protocol_state_inner(
                &Chain::Ethereum,
                &request,
                &StateRequestParameters::default(),
                &mut conn,
            )
            .await
            .unwrap();

        assert_eq!(state.accounts.len(), 1);
        assert_eq!(state.accounts[0], expected.into());
    }

    #[tokio::test]
    async fn test_get_protocol_components() {
        let db_url = std::env::var("DATABASE_URL").unwrap();
        let pool = postgres::connect(&db_url)
            .await
            .unwrap();
        let cloned_pool = pool.clone();
        let mut conn = cloned_pool.get().await.unwrap();
        conn.begin_test_transaction()
            .await
            .unwrap();
        setup_components(&mut conn).await;

        let db_gateway = Arc::new(EvmPostgresGateway::from_connection(&mut conn).await);
        let req_handler = RpcHandler::new(db_gateway, pool);

        // request for ambient protocol components - there is one
        let request = dto::ProtocolComponentsRequestBody {
            protocol_system: Option::from("ambient".to_string()),
            component_ids: None,
        };
        let params = dto::ProtocolComponentRequestParameters::default();

        let components = req_handler
            .get_protocol_components_inner(&Chain::Ethereum, &request, &params, &mut conn)
            .await
            .unwrap();

        assert_eq!(components.protocol_components.len(), 1);

        // request for curve protocol components - there are none
        let request = dto::ProtocolComponentsRequestBody {
            protocol_system: Option::from("curve".to_string()),
            component_ids: None,
        };

        let components = req_handler
            .get_protocol_components_inner(&Chain::Ethereum, &request, &params, &mut conn)
            .await
            .unwrap();

        assert_eq!(components.protocol_components.len(), 0);
    }

    fn evm_slots(data: impl IntoIterator<Item = (i32, i32)>) -> HashMap<U256, U256> {
        data.into_iter()
            .map(|(s, v)| (U256::from(s), U256::from(v)))
            .collect()
    }

    #[tokio::test]
    async fn test_get_contract_delta() {
        // Setup
        let db_url = std::env::var("DATABASE_URL").unwrap();
        let pool = postgres::connect(&db_url)
            .await
            .unwrap();
        let mut conn = pool.get().await.unwrap();
        conn.begin_test_transaction()
            .await
            .unwrap();
        let acc_address = setup_contract_data(&mut conn).await;

        let db_gateway = Arc::new(EvmPostgresGateway::from_connection(&mut conn).await);
        let req_handler = RpcHandler::new(db_gateway, pool);

        let expected = evm::AccountUpdate::new(
            H160::from_str("6B175474E89094C44Da98b954EedeAC495271d0F").unwrap(),
            Chain::Ethereum,
            evm_slots([(6, 30), (5, 25), (1, 3), (0, 2)]),
            Some(U256::from(101)),
            None,
            storage::ChangeType::Update,
        );

        let request = dto::ContractDeltaRequestBody {
            contract_ids: Some(vec![dto::ContractId::new(
                Chain::Ethereum.into(),
                acc_address.parse().unwrap(),
            )]),
            start: dto::VersionParam {
                timestamp: None,
                block: Some(dto::BlockParam {
                    hash: Some(
                        "0x88e96d4537bea4d9c05d12549907b32561d3bf31f45aae734cdc119f13406cb6"
                            .parse()
                            .unwrap(),
                    ),
                    chain: None,
                    number: None,
                }),
            },
            end: dto::VersionParam {
                timestamp: None,
                block: Some(dto::BlockParam {
                    hash: Some(
                        "0xb495a1d7e6663152ae92708da4843337b958146015a2802f4193a410044698c9"
                            .parse()
                            .unwrap(),
                    ),
                    chain: None,
                    number: None,
                }),
            },
        };

        let delta = req_handler
            .get_contract_delta_inner(
                &Chain::Ethereum,
                &request,
                &StateRequestParameters::default(),
                &mut conn,
            )
            .await
            .unwrap();

        assert_eq!(delta.accounts.len(), 1);
        assert_eq!(delta.accounts[0], expected.into());
    }

    #[tokio::test]
    async fn test_get_protocol_delta() {
        // Setup
        let db_url = std::env::var("DATABASE_URL").unwrap();
        let pool = postgres::connect(&db_url)
            .await
            .unwrap();
        let mut conn = pool.get().await.unwrap();
        conn.begin_test_transaction()
            .await
            .unwrap();

        setup_protocol_data(&mut conn).await;

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

        let db_gateway = Arc::new(EvmPostgresGateway::from_connection(&mut conn).await);
        let req_handler = RpcHandler::new(db_gateway, pool);

        let expected = dto::ProtocolDeltaRequestResponse {
            protocols: vec![dto::ProtocolStateDelta {
                component_id: "state3".to_owned(),
                updated_attributes: HashMap::new(),
                deleted_attributes: vec!["deleted2".to_owned()]
                    .into_iter()
                    .collect(),
            }],
        };

        let request = dto::ProtocolDeltaRequestBody {
            component_ids: Some(vec!["state3".to_owned()]), // Filter to only "state3"
            start: dto::VersionParam {
                timestamp: None,
                block: Some(dto::BlockParam {
                    hash: None,
                    chain: Some(dto::Chain::Ethereum),
                    number: Some(1),
                }),
            },
            end: dto::VersionParam {
                timestamp: None,
                block: Some(dto::BlockParam {
                    hash: None,
                    chain: Some(dto::Chain::Ethereum),
                    number: Some(2),
                }),
            },
        };

        let delta = req_handler
            .get_protocol_delta_inner(
                &Chain::Ethereum,
                &request,
                &StateRequestParameters::default(),
                &mut conn,
            )
            .await
            .unwrap();

        assert_eq!(delta.protocols.len(), 1);
        assert_eq!(delta, expected);
    }
}
