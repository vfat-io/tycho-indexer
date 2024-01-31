//! This module contains Tycho RPC implementation

use crate::{
    extractor::evm,
    models::Chain,
    storage::{
        self, Address, BlockIdentifier, BlockOrTimestamp, ContractStateGateway, StorageError,
    },
};
use tycho_types::Bytes;

use actix_web::{web, HttpResponse};
use diesel_async::{
    pooled_connection::deadpool::{self, Pool},
    AsyncPgConnection,
};
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, error, info, instrument};

use crate::storage::ProtocolGateway;
use tycho_types::{
    dto,
    dto::{ResponseToken, StateRequestParameters},
};

use super::EvmPostgresGateway;

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

#[derive(Error, Debug)]
pub enum RpcError {
    #[error("Failed to parse JSON: {0}")]
    Parse(String),

    #[error("Failed to get storage: {0}")]
    Storage(#[from] StorageError),

    #[error("Failed to get database connection: {0}")]
    Connection(#[from] deadpool::PoolError),
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

    #[instrument(skip(self, request, params))]
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

#[cfg(test)]
mod tests {
    use crate::storage::{
        postgres::{self, db_fixtures},
        Code,
    };
    use actix_web::test;
    use chrono::Utc;
    use diesel_async::AsyncConnection;
    use ethers::types::{H160, H256, U256};
    use tycho_types::Bytes;

    use std::{collections::HashMap, str::FromStr, sync::Arc};

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
            .timestamp_millis()
            - result
                .version
                .timestamp
                .unwrap()
                .timestamp_millis();

        // Allowing a small time delta (1 second)
        assert!(time_difference <= 1000);
        assert_eq!(result.contract_ids, expected.contract_ids);
        assert_eq!(result.version.block, expected.version.block);
    }

    pub async fn setup_account(conn: &mut AsyncPgConnection) -> String {
        // Adds fixtures: chain, block, transaction, account, account_balance
        let acc_address = "6B175474E89094C44Da98b954EedeAC495271d0F";
        let chain_id = db_fixtures::insert_chain(conn, "ethereum").await;
        let blk = db_fixtures::insert_blocks(conn, chain_id).await;
        let tid = db_fixtures::insert_txns(
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
            db_fixtures::insert_account(conn, acc_address, "account0", chain_id, Some(tid[0]))
                .await;

        db_fixtures::insert_account_balance(conn, 100, tid[0], None, acc_id).await;
        let contract_code = Code::from("1234");
        db_fixtures::insert_contract_code(conn, acc_id, tid[0], contract_code).await;
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
        let acc_address = setup_account(&mut conn).await;

        let db_gateway = Arc::new(EvmPostgresGateway::from_connection(&mut conn).await);
        let req_handler = RpcHandler::new(db_gateway, pool);

        let code = Code::from("1234");
        let code_hash = H256::from_slice(&ethers::utils::keccak256(&code));
        let expected = evm::Account::new(
            Chain::Ethereum,
            H160::from_str("6B175474E89094C44Da98b954EedeAC495271d0F").unwrap(),
            "account0".to_owned(),
            HashMap::new(),
            U256::from(100),
            code,
            code_hash,
            "0xbb7e16d797a9e2fbc537e30f91ed3d27a254dd9578aa4c3af3e5f0d3e8130945"
                .parse()
                .unwrap(),
            "0xbb7e16d797a9e2fbc537e30f91ed3d27a254dd9578aa4c3af3e5f0d3e8130945"
                .parse()
                .unwrap(),
            Some(
                "0xbb7e16d797a9e2fbc537e30f91ed3d27a254dd9578aa4c3af3e5f0d3e8130945"
                    .parse()
                    .unwrap(),
            ),
        );

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
}
