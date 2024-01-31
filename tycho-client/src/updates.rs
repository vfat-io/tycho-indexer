use futures03::{stream::SplitSink, SinkExt, StreamExt};
use hyper::Uri;
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
    connect_async, tungstenite, tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream,
};
use tycho_types::dto::{
    BlockAccountChanges, Command, ExtractorIdentity, Response, WebSocketMessage,
};

use crate::TYCHO_SERVER_VERSION;

#[derive(Error, Debug)]
pub enum TychoUpdateClientError {
    #[error("Failed to parse URI: {0}. Error: {1}")]
    UriParsing(String, String),
    #[error("{0}")]
    TransportError(String),
    #[error("The client is not connected!.")]
    NotConnected,
    #[error("The client is already connected.")]
    AlreadyConnected,
    #[error("The server closed the connection.")]
    ConnectionClosed,
    #[error("ConnectionError {source}")]
    ConnectionError {
        #[from]
        source: tungstenite::Error,
    },
    #[error("Tycho FatalError: {0}")]
    Fatal(String),
}

#[async_trait]
pub trait TychoUpdatesClient {
    /// Subscribe to an extractor and receive realtime messages
    async fn subscribe(
        &self,
        extractor_id: ExtractorIdentity,
    ) -> Result<Receiver<BlockAccountChanges>, TychoUpdateClientError>;

    /// Unsubscribe from an extractor
    async fn unsubscribe(&self, subscription_id: Uuid) -> Result<(), TychoUpdateClientError>;

    /// Consumes realtime messages from the WebSocket server
    async fn connect(
        &self,
    ) -> Result<JoinHandle<Result<(), TychoUpdateClientError>>, TychoUpdateClientError>;

    /// Close the connection
    async fn close(&self) -> Result<(), TychoUpdateClientError>;
}

#[derive(Clone)]
pub struct TychoWsClient {
    uri: Uri,
    max_reconnects: u32,
    conn_notify: Arc<Notify>,
    // unset on startup or after connection ended
    inner: Arc<Mutex<Option<Inner>>>,
}

type WebSocketSink = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;

struct SubscriptionInfo {
    state: SubscriptionStatus,
}

#[derive(Debug)]
enum SubscriptionStatus {
    RequestedSubscription(oneshot::Sender<Receiver<BlockAccountChanges>>),
    Active,
    RequestedUnsubscription(oneshot::Sender<()>),
}

struct Inner {
    // may be unset during reconnection
    sink: WebSocketSink,
    cmd_tx: Sender<()>,
    // could be a vec and we take the first one matching if we
    // wanted to support multiple duplicated subs.
    pending: HashMap<ExtractorIdentity, SubscriptionInfo>,
    subscriptions: HashMap<Uuid, SubscriptionInfo>,
    sender: HashMap<Uuid, Sender<BlockAccountChanges>>,
}

impl Inner {
    fn new(cmd_tx: Sender<()>, sink: WebSocketSink) -> Self {
        Self {
            sink,
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

    fn mark_active(&mut self, extractor_id: &ExtractorIdentity, subscription_id: Uuid) {
        if let Some(mut info) = self.pending.remove(extractor_id) {
            if let SubscriptionStatus::RequestedSubscription(ready_tx) = info.state {
                let (tx, rx) = mpsc::channel(1);
                info.state = SubscriptionStatus::Active;
                self.sender.insert(subscription_id, tx);
                self.subscriptions
                    .insert(subscription_id, info);
                let _ = ready_tx.send(rx).map_err(|_| {
                    warn!(
                        ?extractor_id,
                        ?subscription_id,
                        "Subscriber for has gone away. Ignoring."
                    )
                });
            } else {
                error!(
                    ?extractor_id,
                    ?subscription_id,
                    "Pending subscription was not in the correct state to 
                    transition to active. Ignoring!"
                )
            }
        } else {
            error!(
                ?extractor_id,
                ?subscription_id,
                "Tried to mark an unkown subscription as active. Ignoring!"
            );
        }
    }

    async fn send(&mut self, id: &Uuid, msg: BlockAccountChanges) -> anyhow::Result<()> {
        if let Some(sender) = self.sender.get_mut(id) {
            sender.send(msg).await?;
        }
        Ok(())
    }

    fn end_subscription(&mut self, subscription_id: &Uuid, ready_tx: oneshot::Sender<()>) {
        if let Some(info) = self
            .subscriptions
            .get_mut(subscription_id)
        {
            if let SubscriptionStatus::Active = &info.state {
                info.state = SubscriptionStatus::RequestedUnsubscription(ready_tx);
            }
        } else {
            // no big deal imo so only debug lvl..
            debug!(?subscription_id, "Tried unsubscribing from a non existent subscription");
        }
    }

    fn remove_subscription(&mut self, subscription_id: Uuid) {
        if let Entry::Occupied(e) = self
            .subscriptions
            .entry(subscription_id)
        {
            let info = e.remove();
            if let SubscriptionStatus::RequestedUnsubscription(tx) = info.state {
                let _ = tx.send(()).map_err(|_| {
                    warn!(?subscription_id, "failed to notify about removed subscription")
                });
                self.sender
                    .remove(&subscription_id)
                    .expect(
                        "Inconsistent internal client state: `sender` state 
                        drifted from `info` while removing a subscription.",
                    );
            } else {
                warn!(?subscription_id, "Subscription ended unexpectedly!");
                self.sender
                    .remove(&subscription_id)
                    .expect("sender channel missing");
            }
        } else {
            error!(
                ?subscription_id,
                "Received `SubscriptionEnded` for but the never subscribed 
                to it. This is likely a bug!"
            );
        }
    }

    async fn ws_send(&mut self, msg: Message) -> Result<(), TychoUpdateClientError> {
        self.sink.send(msg).await.map_err(|e| {
            TychoUpdateClientError::TransportError(format!(
                "Failed to send message to websocket: {e}"
            ))
        })
    }
}

impl TychoWsClient {
    pub fn new(ws_uri: &str) -> Result<Self, TychoUpdateClientError> {
        let uri = ws_uri
            .parse::<Uri>()
            .map_err(|e| TychoUpdateClientError::UriParsing(ws_uri.to_string(), e.to_string()))?;

        Ok(Self {
            uri,
            inner: Arc::new(Mutex::new(None)),
            conn_notify: Arc::new(Notify::new()),
            max_reconnects: 5,
        })
    }

    pub fn new_with_reconnects(
        ws_uri: &str,
        max_reconnects: u32,
    ) -> Result<Self, TychoUpdateClientError> {
        let uri = ws_uri
            .parse::<Uri>()
            .map_err(|e| TychoUpdateClientError::UriParsing(ws_uri.to_string(), e.to_string()))?;

        Ok(Self {
            uri,
            inner: Arc::new(Mutex::new(None)),
            conn_notify: Arc::new(Notify::new()),
            max_reconnects,
        })
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

    #[instrument(skip(self))]
    async fn handle_msg(
        &self,
        msg: Result<Message, tokio_tungstenite::tungstenite::error::Error>,
    ) -> Result<(), TychoUpdateClientError> {
        let mut guard = self.inner.lock().await;
        dbg!(&msg);
        match msg {
            Ok(Message::Text(text)) => match serde_json::from_str::<WebSocketMessage>(&text) {
                Ok(WebSocketMessage::BlockAccountChanges { subscription_id, data }) => {
                    info!(?data, "Received a block state change, sending to channel");
                    let inner = guard
                        .as_mut()
                        .ok_or_else(|| TychoUpdateClientError::NotConnected)?;
                    if let Err(_) = inner.send(&subscription_id, data).await {
                        warn!(?subscription_id, "Receiver for has gone away, unsubscribing!");
                        let (tx, _) = oneshot::channel();
                        let _ = TychoWsClient::unsubscribe_inner(inner, subscription_id, tx).await;
                    }
                }
                Ok(WebSocketMessage::Response(Response::NewSubscription {
                    extractor_id,
                    subscription_id,
                })) => {
                    info!(?extractor_id, ?subscription_id, "Received a new subscription");
                    let inner = guard
                        .as_mut()
                        .ok_or_else(|| TychoUpdateClientError::NotConnected)?;
                    inner.mark_active(&extractor_id, subscription_id);
                    dbg!("subscription active");
                }
                Ok(WebSocketMessage::Response(Response::SubscriptionEnded { subscription_id })) => {
                    info!(?subscription_id, "Received a subscription ended");
                    dbg!("subscription ended start");
                    let inner = guard
                        .as_mut()
                        .ok_or_else(|| TychoUpdateClientError::NotConnected)?;
                    inner.remove_subscription(subscription_id);
                    dbg!("subscription ended");
                }
                Err(e) => {
                    dbg!("deserialisation failed");
                    error!(error = %e, message=text, "Failed to deserialize message");
                }
            },
            Ok(Message::Ping(_)) => {
                // Respond to pings with pongs.
                let inner = guard
                    .as_mut()
                    .ok_or_else(|| TychoUpdateClientError::NotConnected)?;
                if let Err(error) = inner
                    .ws_send(Message::Pong(Vec::new()))
                    .await
                {
                    debug!(?error, "Failed to send pong!");
                }
            }
            Ok(Message::Pong(_)) => {
                // Do nothing.
            }
            Ok(Message::Close(_)) => {
                dbg!("closed");
                return Err(TychoUpdateClientError::ConnectionClosed);
            }
            Ok(unknown_msg) => {
                dbg!(&unknown_msg);
                info!("Received an unknown message type: {:?}", unknown_msg);
            }
            Err(error) => {
                error!(?error, "Websocket error");
                return Err(match &error {
                    tungstenite::Error::ConnectionClosed => TychoUpdateClientError::from(error),
                    tungstenite::Error::AlreadyClosed => {
                        warn!("Received AlreadyClosed error which is indicative of a bug!");
                        TychoUpdateClientError::from(error)
                    }
                    tungstenite::Error::Io(_) => TychoUpdateClientError::from(error),
                    _ => TychoUpdateClientError::Fatal(error.to_string()),
                })
            }
        };
        Ok(())
    }

    async fn unsubscribe_inner(
        inner: &mut Inner,
        subscription_id: Uuid,
        ready_tx: oneshot::Sender<()>,
    ) -> Result<(), TychoUpdateClientError> {
        inner.end_subscription(&subscription_id, ready_tx);
        let cmd = Command::Unsubscribe { subscription_id };
        inner
            .ws_send(Message::Text(
                serde_json::to_string(&cmd).expect("serialize cmd encode error"),
            ))
            .await?;
        Ok(())
    }
}

#[async_trait]
impl TychoUpdatesClient for TychoWsClient {
    #[allow(unused_variables)]
    #[instrument(skip(self))]
    async fn subscribe(
        &self,
        extractor_id: ExtractorIdentity,
    ) -> Result<Receiver<BlockAccountChanges>, TychoUpdateClientError> {
        dbg!("entered subscribe");
        self.ensure_connection().await;
        let (ready_tx, ready_rx) = oneshot::channel();
        {
            let mut guard = self.inner.lock().await;
            let inner = guard
                .as_mut()
                .expect("ws not connected");
            dbg!("started subscribe");
            inner.new_subscription(&extractor_id, ready_tx);
            let cmd = Command::Subscribe { extractor_id };
            inner
                .ws_send(Message::Text(
                    serde_json::to_string(&cmd).expect("serialize cmd encode error"),
                ))
                .await?;
        }
        let rx = ready_rx
            .await
            .expect("ready channel closed");
        dbg!("finished subscribe");
        Ok(rx)
    }

    #[instrument(skip(self))]
    async fn unsubscribe(&self, subscription_id: Uuid) -> Result<(), TychoUpdateClientError> {
        self.ensure_connection().await;
        let (ready_tx, ready_rx) = oneshot::channel();
        {
            let mut guard = self.inner.lock().await;
            let inner = guard
                .as_mut()
                .expect("ws not connected");
            TychoWsClient::unsubscribe_inner(inner, subscription_id, ready_tx).await?;
        }
        ready_rx
            .await
            .expect("ready channel closed");
        Ok(())
    }

    #[instrument(skip(self))]
    async fn connect(
        &self,
    ) -> Result<JoinHandle<Result<(), TychoUpdateClientError>>, TychoUpdateClientError> {
        if self.is_connected().await {
            return Err(TychoUpdateClientError::AlreadyConnected);
        }
        let (cmd_tx, mut cmd_rx) = mpsc::channel(30);
        let ws_uri = format!("{}{}/ws", self.uri, TYCHO_SERVER_VERSION);
        info!(?ws_uri, "Starting TychoWebsocketClient");
        let (conn, _) = connect_async(&ws_uri).await?;
        let (ws_tx, ws_rx) = conn.split();
        let mut ws_rx = Some(ws_rx);

        {
            let mut guard = self.inner.as_ref().lock().await;
            *guard = Some(Inner::new(cmd_tx.clone(), ws_tx));
        }

        let this = self.clone();
        let jh = tokio::spawn(async move {
            let mut retry_count = 0;
            'retry: while retry_count < this.max_reconnects {
                dbg!("client: starting (re)connection");
                info!(?ws_uri, "Connecting to WebSocket server");

                let mut msg_rx = if let Some(stream) = ws_rx.take() {
                    stream.boxed()
                } else {
                    let (conn, _) = connect_async(&ws_uri).await?;
                    let (ws_tx_new, ws_rx_new) = conn.split();
                    let mut guard = this.inner.as_ref().lock().await;
                    *guard = Some(Inner::new(cmd_tx.clone(), ws_tx_new));
                    ws_rx_new.boxed()
                };

                this.conn_notify.notify_waiters();
                dbg!("client: connection established!");
                loop {
                    let res = tokio::select! {
                        Some(msg) = msg_rx.next() => this.handle_msg(msg).await,
                        _ = cmd_rx.recv() => {break 'retry},
                    };
                    if let Err(error) = res {
                        if matches!(
                            error,
                            TychoUpdateClientError::ConnectionClosed |
                                TychoUpdateClientError::ConnectionError { .. }
                        ) {
                            // Code for reconnection connection...
                            retry_count += 1;
                            let mut guard = this.inner.as_ref().lock().await;
                            *guard = None;
                            dbg!("Connection state: reset");
                            warn!(
                                ?error,
                                ?retry_count,
                                "Connection dropped unexpectedly; Reconnecting"
                            );
                            break
                        } else {
                            // Other errors are considered fatal
                            break 'retry;
                        }
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

    #[instrument(skip(self))]
    async fn close(&self) -> Result<(), TychoUpdateClientError> {
        let mut guard = self.inner.lock().await;
        let inner = guard
            .as_mut()
            .ok_or_else(|| TychoUpdateClientError::NotConnected)?;
        inner
            .cmd_tx
            .send(())
            .await
            .map_err(|e| TychoUpdateClientError::TransportError(e.to_string()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tycho_types::dto::Chain;

    use super::*;

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

    async fn mock_tycho_ws(
        messages: &[ExpectedComm],
        reconnects: usize,
    ) -> (SocketAddr, JoinHandle<()>) {
        let server = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("localhost bind failed");
        let addr = server.local_addr().unwrap();
        let messages = messages.to_vec();

        let jh = tokio::spawn(async move {
            dbg!("server starting");
            for _ in 0..(reconnects + 1) {
                if let Ok((stream, _)) = server.accept().await {
                    dbg!("server got connection");
                    let mut websocket = tokio_tungstenite::accept_async(stream)
                        .await
                        .unwrap();

                    for c in messages.iter().cloned() {
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
                    dbg!("server ended");
                }
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
        let (addr, server_thread) = mock_tycho_ws(&exp_comm, 0).await;

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
    async fn test_subscription_end() {
        let exp_comm = [
            ExpectedComm::Receive(
                100,
                Message::Text(
                    r#"
                {
                    "method":"subscribe",
                    "extractor_id":{
                    "chain":"ethereum",
                    "name":"vm:ambient"
                    }
                }"#
                    .to_owned()
                    .replace(|c: char| c.is_whitespace(), ""),
                ),
            ),
            ExpectedComm::Send(Message::Text(
                r#"
                {
                    "method":"newsubscription",
                    "extractor_id":{
                        "chain":"ethereum",
                        "name":"vm:ambient"
                    },
                    "subscription_id":"30b740d1-cf09-4e0e-8cfe-b1434d447ece"
                }"#
                .to_owned()
                .replace(|c: char| c.is_whitespace(), ""),
            )),
            ExpectedComm::Send(Message::Text(
                r#"
                {
                    "method": "subscriptionended",
                    "subscription_id": "30b740d1-cf09-4e0e-8cfe-b1434d447ece"
                }"#
                .to_owned()
                .replace(|c: char| c.is_whitespace(), ""),
            )),
        ];
        let (addr, server_thread) = mock_tycho_ws(&exp_comm, 0).await;

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
        let res = timeout(Duration::from_millis(100), rx.recv())
            .await
            .expect("awaiting message timeout out");

        // If the subscription ended, the channel should have been closed.
        assert!(res.is_none());

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
    async fn test_reconnect() {
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
        let (addr, server_thread) = mock_tycho_ws(&exp_comm, 1).await;
        let client = TychoWsClient::new(&format!("ws://{}", addr)).unwrap();
        let jh: JoinHandle<Result<(), TychoUpdateClientError>> = client
            .connect()
            .await
            .expect("connect failed");

        for iteration in 0..2 {
            dbg!(iteration);
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

            // wait for the connection to drop
            let res = timeout(Duration::from_millis(200), rx.recv())
                .await
                .expect("awaiting closed connection timeout out");
            assert!(res.is_none());
        }
        let res = jh.await.expect("ws client join failed");
        // 3rd client reconnect attempt should fail
        assert!(matches!(res, Err(_)));
        server_thread
            .await
            .expect("ws server loop errored");
    }
}
