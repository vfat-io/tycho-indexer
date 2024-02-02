//! # Tycho RPC Client
//!
//! The objective of this module is to provide swift and simplified access to the Remote Procedure
//! Call (RPC) endpoints of Tycho. These endpoints are chiefly responsible for facilitating data
//! queries, especially querying snapshots of data.
//!
//! Currently we provide only a HTTP implementation.
use hyper::{client::HttpConnector, Body, Client, Request, Uri};
use std::string::ToString;
use thiserror::Error;
use tracing::{debug, error, info, instrument, warn};

use async_trait::async_trait;

use tycho_types::dto::{Chain, StateRequestBody, StateRequestParameters, StateRequestResponse};

use crate::TYCHO_SERVER_VERSION;

#[derive(Error, Debug)]
pub enum RPCError {
    /// The passed tycho url failed to parse.
    #[error("Failed to parse URI: {0}. Error: {1}")]
    UriParsing(String, String),
    /// The request data is not correctly formed.
    #[error("Failed to format request: {0}")]
    FormatRequest(String),
    /// Errors forwarded from the HTTP protocol.
    #[error("Unexpected HTTP client error: {0}")]
    HttpClient(String),
    /// The response from the server could not be parsed correctly.
    #[error("Failed to parse response: {0}")]
    ParseResponse(String),
}

#[async_trait]
pub trait RPCClient {
    /// Retrieves a snapshot of contract state.
    async fn get_contract_state(
        &self,
        chain: Chain,
        filters: &StateRequestParameters,
        request: &StateRequestBody,
    ) -> Result<StateRequestResponse, RPCError>;
}

#[derive(Debug, Clone)]
pub struct HttpRPCClient {
    http_client: Client<HttpConnector>,
    uri: Uri,
}

impl HttpRPCClient {
    pub fn new(base_uri: &str) -> Result<Self, RPCError> {
        let uri = base_uri
            .parse::<Uri>()
            .map_err(|e| RPCError::UriParsing(base_uri.to_string(), e.to_string()))?;

        Ok(Self { http_client: Client::new(), uri })
    }
}

#[async_trait]
impl RPCClient for HttpRPCClient {
    #[instrument(skip(self, filters, request))]
    async fn get_contract_state(
        &self,
        chain: Chain,
        filters: &StateRequestParameters,
        request: &StateRequestBody,
    ) -> Result<StateRequestResponse, RPCError> {
        // Check if contract ids are specified
        if request.contract_ids.is_none() ||
            request
                .contract_ids
                .as_ref()
                .unwrap()
                .is_empty()
        {
            warn!("No contract ids specified in request.");
        }

        let uri = format!(
            "{}/{}/{}/contract_state{}",
            self.uri
                .to_string()
                .trim_end_matches('/'),
            TYCHO_SERVER_VERSION,
            chain,
            filters.to_query_string()
        );
        dbg!(&uri);
        debug!(%uri, "Sending contract_state request to Tycho server");
        let body =
            serde_json::to_string(&request).map_err(|e| RPCError::FormatRequest(e.to_string()))?;

        let header = hyper::header::HeaderValue::from_str("application/json")
            .map_err(|e| RPCError::FormatRequest(e.to_string()))?;

        let req = Request::post(uri)
            .header(hyper::header::CONTENT_TYPE, header)
            .body(Body::from(body))
            .map_err(|e| RPCError::FormatRequest(e.to_string()))?;
        debug!(?req, "Sending request to Tycho server");

        let response = self
            .http_client
            .request(req)
            .await
            .map_err(|e| RPCError::HttpClient(e.to_string()))?;
        debug!(?response, "Received response from Tycho server");

        let body = hyper::body::to_bytes(response.into_body())
            .await
            .map_err(|e| RPCError::ParseResponse(e.to_string()))?;
        let accounts: StateRequestResponse =
            serde_json::from_slice(&body).map_err(|e| RPCError::ParseResponse(e.to_string()))?;
        info!(?accounts, "Received contract_state response from Tycho server");

        Ok(accounts)
    }
}

#[cfg(test)]
mod tests {
    use tycho_types::Bytes;

    use super::*;

    use mockito::Server;

    use std::collections::HashMap;

    #[tokio::test]
    async fn test_get_contract_state() {
        let mut server = Server::new_async().await;
        let server_resp = r#"
        {
            "accounts": [
                {
                    "chain": "ethereum",
                    "address": "0x0000000000000000000000000000000000000000",
                    "title": "",
                    "slots": {},
                    "balance": "0x01f4",
                    "code": "0x00",
                    "code_hash": "0x5c06b7c5b3d910fd33bc2229846f9ddaf91d584d9b196e16636901ac3a77077e",
                    "balance_modify_tx": "0x0000000000000000000000000000000000000000000000000000000000000000",
                    "code_modify_tx": "0x0000000000000000000000000000000000000000000000000000000000000000",
                    "creation_tx": null
                }
            ]
        }
        "#;
        // test that the response is deserialized correctly
        serde_json::from_str::<StateRequestResponse>(server_resp).expect("deserialize");

        let mocked_server = server
            .mock("POST", "/v1/ethereum/contract_state")
            .expect(1)
            .with_body(server_resp)
            .create_async()
            .await;

        let client = HttpRPCClient::new(server.url().as_str()).expect("create client");

        let response = client
            .get_contract_state(Chain::Ethereum, &Default::default(), &Default::default())
            .await
            .expect("get state");
        let accounts = response.accounts;

        mocked_server.assert();
        assert_eq!(accounts.len(), 1);
        assert_eq!(accounts[0].slots, HashMap::new());
        assert_eq!(accounts[0].balance, Bytes::from(500u16.to_be_bytes()));
        assert_eq!(accounts[0].code, [0].to_vec());
        assert_eq!(
            accounts[0].code_hash,
            hex::decode("5c06b7c5b3d910fd33bc2229846f9ddaf91d584d9b196e16636901ac3a77077e")
                .unwrap()
        );
    }
}
