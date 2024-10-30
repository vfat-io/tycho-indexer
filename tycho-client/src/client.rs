use std::{collections::HashMap, time::Duration};
use tokio::sync::mpsc::Receiver;
use tracing::info;
use tracing_subscriber;

use tycho_core::dto::{Chain, ExtractorIdentity};

use crate::{
    deltas::DeltasClient,
    feed::{
        component_tracker::ComponentFilter, synchronizer::ProtocolStateSynchronizer,
        BlockSynchronizer, FeedMessage,
    },
    HttpRPCClient, WsDeltasClient,
};

/// A builder for configuring and initializing a Tycho client.
/// The `TychoClientBuilder` allows you to set up various options for connecting
/// to a Tycho server and managing the synchronization of exchange components.
///
/// # Fields
/// - `tycho_url`: The base URL for the Tycho server (e.g., `"localhost:4242"`).
/// - `chain`: The blockchain network (e.g., `Chain::Ethereum`).
/// - `exchanges`: A map of exchange names to their respective `ComponentFilter` objects.
/// - `block_time`: The expected block time interval, in seconds, for the blockchain.
/// - `timeout`: The timeout duration, in seconds, for network operations.
/// - `no_state`: If `true`, excludes state updates from the stream.
/// - `auth_key`: An optional API key for authenticating with the Tycho server.
/// - `no_tls`: If `true`, disables TLS/SSL for the connection, using `http` and `ws` instead.
pub struct TychoClientBuilder {
    tycho_url: String,
    chain: Chain,
    exchanges: HashMap<String, ComponentFilter>,
    block_time: u64,
    timeout: u64,
    no_state: bool,
    auth_key: Option<String>,
    no_tls: bool,
}

impl TychoClientBuilder {
    /// Creates a new `TychoClientBuilder` with the given Tycho URL and blockchain network.
    /// Initializes the builder with default values for block time and timeout based on the chain.
    ///
    /// # Parameters
    /// - `tycho_url`: The base URL for the Tycho server (e.g., `"localhost:4242"`).
    /// - `chain`: The blockchain network (e.g., `Chain::Ethereum`).
    ///
    /// # Returns
    /// A new instance of `TychoClientBuilder`.
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
    ///
    /// # Parameters
    /// - `chain`: The blockchain network to get the default values for.
    ///
    /// # Returns
    /// A tuple containing the default block time and timeout in seconds.
    fn default_timing(chain: &Chain) -> (u64, u64) {
        match chain {
            Chain::Ethereum => (12, 1),
            Chain::Starknet => (30, 5),
            Chain::ZkSync => (1, 2),
            Chain::Arbitrum => (1, 0), // Typically closer to 0.25s
        }
    }

    /// Adds an exchange and its corresponding filter to the Tycho client.
    ///
    /// # Parameters
    /// - `name`: The name of the exchange (e.g., `"uniswap_v2"`).
    /// - `filter`: The `ComponentFilter` for filtering exchange components.
    ///
    /// # Returns
    /// The updated `TychoClientBuilder` instance.
    pub fn exchange(mut self, name: &str, filter: ComponentFilter) -> Self {
        self.exchanges
            .insert(name.to_string(), filter);
        self
    }

    /// Sets the block time for the Tycho client.
    ///
    /// # Parameters
    /// - `block_time`: The block time in seconds.
    ///
    /// # Returns
    /// The updated `TychoClientBuilder` instance.
    pub fn block_time(mut self, block_time: u64) -> Self {
        self.block_time = block_time;
        self
    }

    /// Sets the timeout duration for network operations.
    ///
    /// # Parameters
    /// - `timeout`: The timeout duration in seconds.
    ///
    /// # Returns
    /// The updated `TychoClientBuilder` instance.
    pub fn timeout(mut self, timeout: u64) -> Self {
        self.timeout = timeout;
        self
    }

    /// Configures the client to exclude state updates from the stream.
    ///
    /// # Parameters
    /// - `no_state`: If `true`, only components and tokens are streamed.
    ///
    /// # Returns
    /// The updated `TychoClientBuilder` instance.
    pub fn no_state(mut self, no_state: bool) -> Self {
        self.no_state = no_state;
        self
    }

    /// Sets the API key for authenticating with the Tycho server.
    ///
    /// # Parameters
    /// - `auth_key`: An optional string containing the API key.
    ///
    /// # Returns
    /// The updated `TychoClientBuilder` instance.
    pub fn auth_key(mut self, auth_key: Option<String>) -> Self {
        self.auth_key = auth_key;
        self.no_tls = false;
        self
    }

    /// Disables TLS/SSL for the connection, using `http` and `ws` protocols.
    ///
    /// # Parameters
    /// - `no_tls`: If `true`, disables TLS/SSL.
    ///
    /// # Returns
    /// The updated `TychoClientBuilder` instance.
    pub fn no_tls(mut self, no_tls: bool) -> Self {
        self.no_tls = no_tls;
        self
    }

    /// Builds and starts the Tycho client, connecting to the Tycho server and
    /// setting up the synchronization of exchange components.
    ///
    /// # Returns
    /// A `Result` containing the `Receiver<FeedMessage>` if successful, or an error message if not.
    ///
    /// # Errors
    /// Returns an error if no exchanges have been registered.
    pub async fn build(self) -> Result<Receiver<FeedMessage>, &'static str> {
        if self.exchanges.is_empty() {
            return Err("At least one exchange must be registered before building the client.");
        }

        // Set up logging
        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
            )
            .init();

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
        let ws_client = WsDeltasClient::new(&tycho_ws_url, self.auth_key.as_deref()).unwrap();
        let ws_jh = ws_client
            .connect()
            .await
            .expect("ws client connection error");

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
                HttpRPCClient::new(&tycho_rpc_url, self.auth_key.as_deref()).unwrap(),
                ws_client.clone(),
            );
            block_sync = block_sync.register_synchronizer(id, sync);
        }

        // Start the BlockSynchronizer and monitor for disconnections
        let (sync_jh, rx) = block_sync
            .run()
            .await
            .expect("block sync start error");

        // Monitor WebSocket and BlockSynchronizer futures
        tokio::spawn(async move {
            tokio::select! {
                res = ws_jh => {
                    let _ = res.expect("WebSocket connection dropped unexpectedly");
                }
                res = sync_jh => {
                    res.expect("BlockSynchronizer stopped unexpectedly");
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
        let receiver = TychoClientBuilder::new("localhost:4242", Chain::Ethereum)
            .auth_key(Some("my_api_key".into()))
            .build()
            .await;
        assert!(receiver.is_err(), "Client should fail to build when no exchanges are registered.");
    }
}
