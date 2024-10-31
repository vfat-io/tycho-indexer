use std::{collections::HashMap, env, time::Duration};
use thiserror::Error;
use tokio::sync::mpsc::Receiver;
use tracing::info;

use tycho_core::dto::{Chain, ExtractorIdentity};

use crate::{
    deltas::DeltasClient,
    feed::{
        component_tracker::ComponentFilter, synchronizer::ProtocolStateSynchronizer,
        BlockSynchronizer, FeedMessage,
    },
    HttpRPCClient, WsDeltasClient,
};

#[derive(Error, Debug)]
pub enum StreamError {
    #[error("Error during stream set up: {0}")]
    SetUpError(String),

    #[error("WebSocket client connection error: {0}")]
    WebSocketConnectionError(String),

    #[error("BlockSynchronizer error: {0}")]
    BlockSynchronizerError(String),
}

pub struct TychoStreamBuilder {
    tycho_url: String,
    chain: Chain,
    exchanges: HashMap<String, ComponentFilter>,
    block_time: u64,
    timeout: u64,
    no_state: bool,
    auth_key: Option<String>,
    no_tls: bool,
}

impl TychoStreamBuilder {
    /// Creates a new `TychoStreamBuilder` with the given Tycho URL and blockchain network.
    /// Initializes the builder with default values for block time and timeout based on the chain.
    pub fn new(tycho_url: &str, chain: Chain) -> Self {
        let (block_time, timeout) = Self::default_timing(&chain);
        Self {
            tycho_url: tycho_url.to_string(),
            chain,
            exchanges: HashMap::new(),
            block_time,
            timeout,
            no_state: false,
            auth_key: None,
            no_tls: true,
        }
    }

    /// Returns the default block time and timeout values for the given blockchain network.
    fn default_timing(chain: &Chain) -> (u64, u64) {
        match chain {
            Chain::Ethereum => (12, 1),
            Chain::Starknet => (30, 5),
            Chain::ZkSync => (1, 2),
            Chain::Arbitrum => (1, 0), // Typically closer to 0.25s
        }
    }

    /// Adds an exchange and its corresponding filter to the Tycho client.
    pub fn exchange(mut self, name: &str, filter: ComponentFilter) -> Self {
        self.exchanges
            .insert(name.to_string(), filter);
        self
    }

    /// Sets the block time for the Tycho client.
    pub fn block_time(mut self, block_time: u64) -> Self {
        self.block_time = block_time;
        self
    }

    /// Sets the timeout duration for network operations.
    pub fn timeout(mut self, timeout: u64) -> Self {
        self.timeout = timeout;
        self
    }

    /// Configures the client to exclude state updates from the stream.
    pub fn no_state(mut self, no_state: bool) -> Self {
        self.no_state = no_state;
        self
    }

    /// Sets the API key for authenticating with the Tycho server.
    ///
    /// Optionally you can set the TYCHO_AUTH_TOKEN env var instead. Make sure to set no_tsl
    /// to false if you do this.
    pub fn auth_key(mut self, auth_key: Option<String>) -> Self {
        self.auth_key = auth_key;
        self.no_tls = false;
        self
    }

    /// Disables TLS/SSL for the connection, using `http` and `ws` protocols.
    pub fn no_tls(mut self, no_tls: bool) -> Self {
        self.no_tls = no_tls;
        self
    }

    /// Builds and starts the Tycho client, connecting to the Tycho server and
    /// setting up the synchronization of exchange components.
    pub async fn build(self) -> Result<Receiver<FeedMessage>, StreamError> {
        if self.exchanges.is_empty() {
            return Err(StreamError::SetUpError(
                "At least one exchange must be registered.".to_string(),
            ));
        }

        // If no auth_key is set and no_tls is false, try to read from the TYCHO_AUTH_TOKEN
        // environment variable
        let auth_key = if self.auth_key.is_none() && !self.no_tls {
            match env::var("TYCHO_AUTH_TOKEN") {
                Ok(token) => Some(token),
                Err(_) => return Err(StreamError::SetUpError("Authentication key is required when `no_tls` is false. Set the TYCHO_AUTH_TOKEN environment variable or use the `auth_key` method.".to_string())),
            }
        } else {
            self.auth_key
        };

        // Determine the URLs based on the TLS setting
        let (tycho_ws_url, tycho_rpc_url) = if self.no_tls {
            info!("Using non-secure connection: ws:// and http://");
            let tycho_ws_url = format!("ws://{}", self.tycho_url);
            let tycho_rpc_url = format!("http://{}", self.tycho_url);
            (tycho_ws_url, tycho_rpc_url)
        } else {
            info!("Using secure connection: wss:// and https://");
            let tycho_ws_url = format!("wss://{}", self.tycho_url);
            let tycho_rpc_url = format!("https://{}", self.tycho_url);
            (tycho_ws_url, tycho_rpc_url)
        };

        // Initialize the WebSocket client
        let ws_client = WsDeltasClient::new(&tycho_ws_url, auth_key.as_deref()).unwrap();
        let ws_jh = ws_client
            .connect()
            .await
            .map_err(|e| StreamError::WebSocketConnectionError(e.to_string()))?;

        // Create and configure the BlockSynchronizer
        let mut block_sync = BlockSynchronizer::new(
            Duration::from_secs(self.block_time),
            Duration::from_secs(self.timeout),
        );

        // Register each exchange with the BlockSynchronizer
        for (name, filter) in self.exchanges {
            info!("Registering exchange: {}", name);
            let id = ExtractorIdentity { chain: self.chain, name: name.clone() };
            let sync = ProtocolStateSynchronizer::new(
                id.clone(),
                true,
                filter,
                3,
                !self.no_state,
                HttpRPCClient::new(&tycho_rpc_url, auth_key.as_deref()).unwrap(),
                ws_client.clone(),
            );
            block_sync = block_sync.register_synchronizer(id, sync);
        }

        // Start the BlockSynchronizer and monitor for disconnections
        let (sync_jh, rx) = block_sync
            .run()
            .await
            .map_err(|e| StreamError::BlockSynchronizerError(e.to_string()))?;

        // Monitor WebSocket and BlockSynchronizer futures
        tokio::spawn(async move {
            tokio::select! {
                res = ws_jh => {
                    let _ = res.map_err(|e| StreamError::WebSocketConnectionError(e.to_string()));
                }
                res = sync_jh => {
                    res.map_err(|e| StreamError::BlockSynchronizerError(e.to_string())).unwrap();
                }
            }
        });

        Ok(rx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_no_exchanges() {
        let receiver = TychoStreamBuilder::new("localhost:4242", Chain::Ethereum)
            .auth_key(Some("my_api_key".into()))
            .build()
            .await;
        assert!(receiver.is_err(), "Client should fail to build when no exchanges are registered.");
    }
}
