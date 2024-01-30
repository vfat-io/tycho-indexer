use futures03::{stream::SplitSink, SinkExt, StreamExt};
use hyper::{client::HttpConnector, Body, Client, Request, Uri};
use std::{
    collections::{hash_map::Entry, HashMap},
    string::ToString,
    sync::Arc,
};
use thiserror::Error;
use tracing::{debug, error, info, instrument, warn};
use uuid::Uuid;

use async_trait::async_trait;

use tokio::{
    net::TcpStream,
    sync::{
        mpsc::{self, Receiver, Sender},
        oneshot, Mutex, Notify,
    },
    task::JoinHandle,
};
use tokio_tungstenite::{
    connect_async, tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream,
};
use tycho_types::dto::{
    BlockAccountChanges, Command, ExtractorIdentity, Response, StateRequestBody,
    StateRequestParameters, StateRequestResponse, WebSocketMessage,
};

/// TODO read consts from config
pub const TYCHO_SERVER_VERSION: &str = "v1";
pub const AMBIENT_EXTRACTOR_HANDLE: &str = "vm:ambient";
pub const AMBIENT_ACCOUNT_ADDRESS: &str = "0xaaaaaaaaa24eeeb8d57d431224f73832bc34f688";

#[derive(Error, Debug)]
pub enum TychoClientError {
    #[error("Failed to parse URI: {0}. Error: {1}")]
    UriParsing(String, String),
    #[error("Failed to format request: {0}")]
    FormatRequest(String),
    #[error("Unexpected HTTP client error: {0}")]
    HttpClient(String),
    #[error("Failed to parse response: {0}")]
    ParseResponse(String),
}

#[derive(Debug, Clone)]
pub struct TychoHttpClient {
    http_client: Client<HttpConnector>,
    uri: Uri,
}

impl TychoHttpClient {
    pub fn new(base_uri: &str) -> Result<Self, TychoClientError> {
        let uri = base_uri
            .parse::<Uri>()
            .map_err(|e| TychoClientError::UriParsing(base_uri.to_string(), e.to_string()))?;

        Ok(Self { http_client: Client::new(), uri })
    }
}

#[async_trait]
pub trait TychoRPCClient {
    async fn get_contract_state(
        &self,
        filters: &StateRequestParameters,
        request: &StateRequestBody,
    ) -> Result<StateRequestResponse, TychoClientError>;
}

#[async_trait]
impl TychoRPCClient for TychoHttpClient {
    #[instrument(skip(self, filters, request))]
    async fn get_contract_state(
        &self,
        filters: &StateRequestParameters,
        request: &StateRequestBody,
    ) -> Result<StateRequestResponse, TychoClientError> {
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
            "{}/{}/contract_state?{}",
            self.uri
                .to_string()
                .trim_end_matches('/'),
            TYCHO_SERVER_VERSION,
            filters.to_query_string()
        );
        debug!(%uri, "Sending contract_state request to Tycho server");
        let body = serde_json::to_string(&request)
            .map_err(|e| TychoClientError::FormatRequest(e.to_string()))?;

        let header = hyper::header::HeaderValue::from_str("application/json")
            .map_err(|e| TychoClientError::FormatRequest(e.to_string()))?;

        let req = Request::post(uri)
            .header(hyper::header::CONTENT_TYPE, header)
            .body(Body::from(body))
            .map_err(|e| TychoClientError::FormatRequest(e.to_string()))?;
        debug!(?req, "Sending request to Tycho server");

        let response = self
            .http_client
            .request(req)
            .await
            .map_err(|e| TychoClientError::HttpClient(e.to_string()))?;
        debug!(?response, "Received response from Tycho server");

        let body = hyper::body::to_bytes(response.into_body())
            .await
            .map_err(|e| TychoClientError::ParseResponse(e.to_string()))?;

        let accounts: StateRequestResponse = serde_json::from_slice(&body)
            .map_err(|e| TychoClientError::ParseResponse(e.to_string()))?;
        info!(?accounts, "Received contract_state response from Tycho server");

        Ok(accounts)
    }
}

#[async_trait]
pub trait TychoUpdatesClient {
    /// Subscribe to an extractor and receive realtime messages
    async fn subscribe(
        &self,
        extractor_id: ExtractorIdentity,
    ) -> Result<Receiver<BlockAccountChanges>, TychoClientError>;

    /// Unsubscribe from an extractor
    async fn unsubscribe(&self, subscription_id: Uuid) -> Result<(), TychoClientError>;

    /// Consumes realtime messages from the WebSocket server
    async fn connect(&self) -> anyhow::Result<JoinHandle<anyhow::Result<()>>>;

    /// Close the connection
    async fn close(&self) -> anyhow::Result<()>;
}

#[derive(Clone)]
pub struct TychoWsClient {
    uri: Uri,
    conn_notify: Arc<Notify>,
    // unset on startup or after connection ended
    inner: Arc<Mutex<Option<Inner>>>,
}

type WebSocketSink = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;

struct SubscriptionInfo {
    state: SubscriptionStatus,
}

enum SubscriptionStatus {
    RequestedSubscription(oneshot::Sender<Receiver<BlockAccountChanges>>),
    Active,
    RequestedUnsubscription(oneshot::Sender<()>),
}

struct Inner {
    // may be unset during reconnection
    sink: Option<WebSocketSink>,
    cmd_tx: Sender<()>,
    // could be a vec and we take the first one matching if we
    // wanted to support multiple duplicated subs.
    pending: HashMap<ExtractorIdentity, SubscriptionInfo>,
    subscriptions: HashMap<Uuid, SubscriptionInfo>,
    sender: HashMap<Uuid, Sender<BlockAccountChanges>>,
}

impl Inner {
    fn new(cmd_tx: Sender<()>) -> Self {
        Self {
            sink: None,
            cmd_tx,
            pending: HashMap::new(),
            subscriptions: HashMap::new(),
            sender: HashMap::new(),
        }
    }

    fn new_subscription(
        &mut self,
        id: &ExtractorIdentity,
        ready_tx: oneshot::Sender<Receiver<BlockAccountChanges>>,
    ) {
        // TODO: deny subscribing twice to same extractor
        self.pending.insert(
            id.clone(),
            SubscriptionInfo { state: SubscriptionStatus::RequestedSubscription(ready_tx) },
        );
    }

    fn mark_active(&mut self, id: &ExtractorIdentity, uuid: Uuid) -> anyhow::Result<()> {
        let mut info = self
            .pending
            .remove(id)
            .expect("subsciption was not pending");
        if let SubscriptionStatus::RequestedSubscription(ready_tx) = info.state {
            let (tx, rx) = mpsc::channel(1);
            info.state = SubscriptionStatus::Active;
            self.sender.insert(uuid, tx);
            self.subscriptions.insert(uuid, info);
            dbg!("Sending ready signal");
            ready_tx
                .send(rx)
                .expect("ready channel closed");
            Ok(())
        } else {
            anyhow::bail!("invalid state transition")
        }
    }

    async fn send(&mut self, id: &Uuid, msg: BlockAccountChanges) -> anyhow::Result<()> {
        self.sender
            .get_mut(id)
            .expect("lacking sender for uuid")
            .send(msg)
            .await?;
        Ok(())
    }

    fn end_subscription(&mut self, id: &Uuid, ready_tx: oneshot::Sender<()>) {
        let info = self
            .subscriptions
            .get_mut(id)
            .expect("no active subscription found");

        if let SubscriptionStatus::Active = &info.state {
            info.state = SubscriptionStatus::RequestedUnsubscription(ready_tx);
        }
    }

    fn remove_subscription(&mut self, id: Uuid) -> anyhow::Result<()> {
        if let Entry::Occupied(e) = self.subscriptions.entry(id) {
            if let SubscriptionStatus::RequestedUnsubscription(tx) = e.remove().state {
                tx.send(())
                    .expect("failed notifying about removed subscription");
                self.sender
                    .remove(&id)
                    .expect("sender channel missing");
            } else {
                anyhow::bail!("invalid state transition");
            }
        } else {
            anyhow::bail!("invalid state transition");
        }
        Ok(())
    }

    async fn ws_send(&mut self, msg: Message) {
        self.sink
            .as_mut()
            .expect("ws not connected")
            .send(msg)
            .await
            .expect("ws send failed");
    }

    fn reset(&mut self) {
        self.pending.clear();
        self.subscriptions.clear();
        self.sender.clear();
        self.sink = None;
    }
}

impl TychoWsClient {
    pub fn new(ws_uri: &str) -> Result<Self, TychoClientError> {
        let uri = ws_uri
            .parse::<Uri>()
            .map_err(|e| TychoClientError::UriParsing(ws_uri.to_string(), e.to_string()))?;

        Ok(Self { uri, inner: Arc::new(Mutex::new(None)), conn_notify: Arc::new(Notify::new()) })
    }

    async fn is_connected(&self) -> bool {
        let guard = self.inner.as_ref().lock().await;
        guard.is_some()
    }

    async fn ensure_connection(&self) {
        if !self.is_connected().await {
            self.conn_notify.notified().await;
        }
    }

    async fn handle_msg(
        &self,
        msg: Result<Message, tokio_tungstenite::tungstenite::error::Error>,
    ) -> Result<(), anyhow::Error> {
        let mut guard = self.inner.lock().await;
        let inner = guard
            .as_mut()
            .ok_or_else(|| anyhow::format_err!("Not connected"))?;
        match msg {
            Ok(Message::Text(text)) => match serde_json::from_str::<WebSocketMessage>(&text) {
                Ok(WebSocketMessage::BlockAccountChanges { subscription_id, data }) => {
                    info!(?data, "Received a block state change, sending to channel");
                    if let Err(_) = inner.send(&subscription_id, data).await {
                        todo!()
                    }
                }
                Ok(WebSocketMessage::Response(Response::NewSubscription {
                    extractor_id,
                    subscription_id,
                })) => {
                    info!(?extractor_id, ?subscription_id, "Received a new subscription");

                    dbg!("Start marking subscription active");
                    inner
                        .mark_active(&extractor_id, subscription_id)
                        .expect("failed internal transition");

                    dbg!("Subscription marked active!");
                }
                Ok(WebSocketMessage::Response(Response::SubscriptionEnded { subscription_id })) => {
                    info!(?subscription_id, "Received a subscription ended");
                    inner
                        .remove_subscription(subscription_id)
                        .expect("failed internal transition");
                }
                Err(e) => {
                    dbg!(&e);
                    error!(error = %e, "Failed to deserialize message");
                }
            },
            Ok(Message::Ping(_)) => {
                // Respond to pings with pongs.
                inner
                    .ws_send(Message::Pong(Vec::new()))
                    .await;
            }
            Ok(Message::Pong(_)) => {
                // Do nothing.
            }
            Ok(Message::Close(_)) => {
                inner.reset();
                return Err(anyhow::anyhow!("Connection closed!"));
            }
            Ok(unknown_msg) => {
                info!("Received an unknown message type: {:?}", unknown_msg);
            }
            Err(e) => {
                inner.reset();
                error!("Failed to get a websocket message: {}", e);
            }
        };
        Ok(())
    }
}

#[async_trait]
impl TychoUpdatesClient for TychoWsClient {
    #[allow(unused_variables)]
    async fn subscribe(
        &self,
        extractor_id: ExtractorIdentity,
    ) -> Result<Receiver<BlockAccountChanges>, TychoClientError> {
        self.ensure_connection().await;
        let (ready_tx, ready_rx) = oneshot::channel();
        {
            let mut guard = self.inner.lock().await;
            let inner = guard
                .as_mut()
                .expect("ws not connected");
            inner.new_subscription(&extractor_id, ready_tx);
            let cmd = Command::Subscribe { extractor_id };
            inner
                .ws_send(Message::Text(
                    serde_json::to_string(&cmd).expect("serialize cmd encode error"),
                ))
                .await;
        }
        let rx = ready_rx
            .await
            .expect("ready channel closed");
        Ok(rx)
    }

    #[allow(unused_variables)]
    async fn unsubscribe(&self, subscription_id: Uuid) -> Result<(), TychoClientError> {
        self.ensure_connection().await;
        let (ready_tx, ready_rx) = oneshot::channel();
        {
            let mut guard = self.inner.lock().await;
            let inner = guard
                .as_mut()
                .expect("ws not connected");
            inner.end_subscription(&subscription_id, ready_tx);
            let cmd = Command::Unsubscribe { subscription_id };
            inner
                .ws_send(Message::Text(
                    serde_json::to_string(&cmd).expect("serialize cmd encode error"),
                ))
                .await;
        }
        ready_rx
            .await
            .expect("ready channel closed");
        Ok(())
    }

    async fn connect(&self) -> anyhow::Result<JoinHandle<anyhow::Result<()>>> {
        if self.is_connected().await {
            anyhow::bail!("Already connected");
        }
        let ws_uri = format!("{}{}/ws", self.uri, TYCHO_SERVER_VERSION);
        info!(?ws_uri, "Starting TychoWebsocketClient");
        let (ws_stream, _) = connect_async(&ws_uri).await?;
        let mut maybe_ws = Some(ws_stream);
        let (cmd_tx, mut cmd_rx) = mpsc::channel(30);
        {
            let mut guard = self.inner.as_ref().lock().await;
            *guard = Some(Inner::new(cmd_tx));
        }

        let this = self.clone();
        let jh = tokio::spawn(async move {
            let mut retry_count = 0;
            'retry: while retry_count < 5 {
                info!(?ws_uri, "Connecting to WebSocket server");

                let ws = match maybe_ws {
                    Some(ws) => ws,
                    None => connect_async(&ws_uri).await?.0,
                };
                let (ws_sink, ws_stream) = ws.split();

                let mut msg_rx = ws_stream.boxed();
                {
                    let mut guard = this.inner.as_ref().lock().await;
                    if let Some(inner) = guard.as_mut() {
                        inner.sink = Some(ws_sink);
                    };
                }

                this.conn_notify.notify_waiters();
                loop {
                    let res = tokio::select! {
                        Some(msg) = msg_rx.next() => this.handle_msg(msg).await,
                        _ = cmd_rx.recv() => {break 'retry},
                    };
                    // TODO scope to connection errors only, other errors will end this task without
                    // a reconnection attempt.
                    if let Err(e) = res {
                        dbg!(&e);
                        warn!(?e, ?retry_count, "Connection dropped unexpectedly; Reconnecting");
                        retry_count += 1;
                        maybe_ws = None;
                        break
                    }
                }
            }
            // clean up before exiting
            let mut guard = this.inner.as_ref().lock().await;
            *guard = None;

            Ok(())
        });
        self.conn_notify.notified().await;
        Ok(jh)
    }

    async fn close(&self) -> anyhow::Result<()> {
        let mut guard = self.inner.lock().await;
        let inner = guard
            .as_mut()
            .ok_or_else(|| anyhow::format_err!("Not connected"))?;
        inner.cmd_tx.send(()).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tycho_types::{dto::Chain, Bytes};

    use super::*;

    use mockito::Server;

    use std::{net::SocketAddr, time::Duration};
    use tokio::{
        net::TcpListener,
        time::{sleep, timeout},
    };

    #[derive(Clone)]
    enum ExpectedComm {
        Receive(u64, Message),
        Send(Message),
    }

    async fn mock_tycho_ws(messages: &[ExpectedComm]) -> (SocketAddr, JoinHandle<()>) {
        let server = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("localhost bind failed");
        let addr = server.local_addr().unwrap();
        let messages = messages.to_vec();

        let jh = tokio::spawn(async move {
            // Accept only the first connection
            if let Ok((stream, _)) = server.accept().await {
                let mut websocket = tokio_tungstenite::accept_async(stream)
                    .await
                    .unwrap();

                for c in messages.into_iter() {
                    match c {
                        ExpectedComm::Receive(t, exp) => {
                            let msg = timeout(Duration::from_millis(t), websocket.next())
                                .await
                                .expect("Receive timeout")
                                .expect("Stream exhausted")
                                .expect("Failed to receive message.");

                            assert_eq!(msg, exp)
                        }
                        ExpectedComm::Send(data) => websocket
                            .send(data)
                            .await
                            .expect("Failed to send message"),
                    };
                }
                sleep(Duration::from_millis(100)).await;
                // Close the WebSocket connection
                let _ = websocket.close(None);
            }
        });
        (addr, jh)
    }

    #[tokio::test]
    async fn test_subscribe_receive() {
        let exp_comm = [
            ExpectedComm::Receive(100, Message::Text(r#"
                {
                    "method":"subscribe",
                    "extractor_id":{
                    "chain":"ethereum",
                    "name":"vm:ambient"
                    }
                }"#.to_owned().replace(|c: char| c.is_whitespace(), "")
            )),
            ExpectedComm::Send(Message::Text(r#"
                {
                    "method":"newsubscription",
                    "extractor_id":{
                    "chain":"ethereum",
                    "name":"vm:ambient"
                    },
                    "subscription_id":"30b740d1-cf09-4e0e-8cfe-b1434d447ece"
                }"#.to_owned().replace(|c: char| c.is_whitespace(), "")
            )),
            ExpectedComm::Send(Message::Text(r#"
                {
                    "subscription_id": "30b740d1-cf09-4e0e-8cfe-b1434d447ece",
                    "data": {
                        "extractor": "vm:ambient",
                        "chain": "ethereum",
                        "block": {
                            "number": 123,
                            "hash": "0x0000000000000000000000000000000000000000000000000000000000000000",
                            "parent_hash": "0x0000000000000000000000000000000000000000000000000000000000000000",
                            "chain": "ethereum",             
                            "ts": "2023-09-14T00:00:00"
                        },
                        "account_updates": {
                            "0x7a250d5630b4cf539739df2c5dacb4c659f2488d": {
                                "address": "0x7a250d5630b4cf539739df2c5dacb4c659f2488d",
                                "chain": "ethereum",
                                "slots": {},
                                "balance": "0x01f4",
                                "code": "",
                                "change": "Update"
                            }
                        }
                    }
                }
                "#.to_owned()
            ))
        ];
        let (addr, server_thread) = mock_tycho_ws(&exp_comm).await;

        let client = TychoWsClient::new(&format!("ws://{}", addr)).unwrap();
        let jh = client
            .connect()
            .await
            .expect("connect failed");
        let mut rx = timeout(
            Duration::from_millis(100),
            client.subscribe(ExtractorIdentity::new(Chain::Ethereum, "vm:ambient")),
        )
        .await
        .expect("subscription timed out")
        .expect("subscription failed");
        let _ = timeout(Duration::from_millis(100), rx.recv())
            .await
            .expect("awaiting message timeout out")
            .expect("receiving message failed");
        timeout(Duration::from_millis(100), client.close())
            .await
            .expect("close timed out")
            .expect("close failed");
        jh.await
            .expect("ws loop errored")
            .unwrap();
        server_thread.await.unwrap();
    }

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
            .mock("POST", "/v1/contract_state?chain=ethereum")
            .expect(1)
            .with_body(server_resp)
            .create_async()
            .await;

        let client = TychoHttpClient::new(server.url().as_str()).expect("create client");

        let response = client
            .get_contract_state(&Default::default(), &Default::default())
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
