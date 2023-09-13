//! This module contains Tycho Websocket implementation

use crate::{
    extractor::runner::ExtractorSubscriber,
    models::{ExtractorIdentity, NormalisedMessage},
};
use actix::{Actor, ActorContext, AsyncContext, SpawnHandle, StreamHandler};
use actix_web::{web, Error, HttpRequest, HttpResponse};
use actix_web_actors::ws;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt::Debug,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use thiserror::Error;
use tracing::error;
use uuid::Uuid;

/// How often heartbeat pings are sent
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
/// How long before lack of client response causes a timeout
const CLIENT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Error, Debug)]
pub enum WebsocketError {
    #[error("Extractor not found: {0}")]
    ExtractorNotFound(ExtractorIdentity),

    #[error("Subscription not found: {0}")]
    SubscriptionNotFound(Uuid),

    #[error("Failed to parse JSON: {0}")]
    ParseError(serde_json::Error),
}

impl From<serde_json::Error> for WebsocketError {
    fn from(e: serde_json::Error) -> Self {
        WebsocketError::ParseError(e)
    }
}

impl Serialize for WebsocketError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            WebsocketError::ExtractorNotFound(extractor_id) => {
                serializer.serialize_str(&format!("Extractor not found: {:?}", extractor_id))
            }
            WebsocketError::SubscriptionNotFound(subscription_id) => {
                serializer.serialize_str(&format!("Subscription not found: {:?}", subscription_id))
            }
            WebsocketError::ParseError(e) => {
                serializer.serialize_str(&format!("Parse error: {:?}", e))
            }
        }
    }
}

type ExtractorSubscriberMap<M> =
    HashMap<ExtractorIdentity, Arc<dyn ExtractorSubscriber<M> + Send + Sync>>;

/// Shared application data between all connections
/// Parameters are hidden behind a Mutex to allow for sharing between threads
pub struct AppState<M> {
    /// There is one extractor subscriber per extractor identity
    pub subscribers: Arc<Mutex<ExtractorSubscriberMap<M>>>,
}

impl<M> AppState<M> {
    pub fn new() -> Self {
        Self { subscribers: Arc::new(Mutex::new(HashMap::new())) }
    }
}

/// Actor handling a single WS connection
struct WsActor<M> {
    _id: Uuid,
    heartbeat: Instant,
    app_state: web::Data<AppState<M>>,
    subscriptions: HashMap<Uuid, SpawnHandle>,
}

impl<M> WsActor<M>
where
    M: NormalisedMessage + Serialize + DeserializeOwned + Sync + Send + 'static,
{
    fn new(app_state: web::Data<AppState<M>>) -> Self {
        Self {
            _id: Uuid::new_v4(),
            heartbeat: Instant::now(),
            app_state,
            subscriptions: HashMap::new(),
        }
    }

    pub async fn ws_index(
        req: HttpRequest,
        stream: web::Payload,
        data: web::Data<AppState<M>>,
    ) -> Result<HttpResponse, Error> {
        ws::start(WsActor::new(data), &req, stream)
    }

    fn heartbeat(&mut self, ctx: &mut <Self as Actor>::Context) {
        // Send a first heartbeat ping
        ctx.run_interval(HEARTBEAT_INTERVAL, |act, ctx| {
            if Instant::now().duration_since(act.heartbeat) > CLIENT_TIMEOUT {
                dbg!("Websocket Client heartbeat failed, disconnecting!");
                ctx.stop();
                return
            }
            ctx.ping(b"");
        });
    }

    fn subscribe(
        &mut self,
        ctx: &mut ws::WebsocketContext<Self>,
        extractor_id: &ExtractorIdentity,
    ) {
        dbg!("Subscribing to extractor: {:?}", extractor_id);
        {
            let extractors_guard = self
                .app_state
                .subscribers
                .lock()
                .unwrap();

            if let Some(extractor_subscriber) = extractors_guard.get(extractor_id) {
                // Generate a unique ID for this subscription
                let subscription_id = Uuid::new_v4();
                dbg!("Generated subscription ID: {:?}", subscription_id);

                match extractor_subscriber.subscribe() {
                    Ok(mut rx) => {
                        let stream = async_stream::stream! {
                            while let Some(item) = rx.recv().await {
                                yield Ok(item);
                            }
                        };

                        let handle = ctx.add_stream(stream);
                        self.subscriptions
                            .insert(subscription_id, handle);
                        dbg!("Added subscription to hashmap: {:?}", subscription_id);

                        let message = Response::NewSubscription { subscription_id };
                        ctx.text(serde_json::to_string(&message).unwrap());
                    }
                    Err(e) => {
                        error!("Failed to subscribe to extractor: {:?}", e);

                        let error = WebsocketError::ExtractorNotFound(extractor_id.clone());
                        ctx.text(serde_json::to_string(&error).unwrap());
                    }
                };
            } else {
                error!("Extractor not found: {:?}", extractor_id);

                let error = WebsocketError::ExtractorNotFound(extractor_id.clone());
                ctx.text(serde_json::to_string(&error).unwrap());
            }
        }
    }

    fn unsubscribe(&mut self, ctx: &mut ws::WebsocketContext<Self>, subscription_id: Uuid) {
        dbg!("Unsubscribing from subscription: {:?}", subscription_id);

        if let Some(handle) = self
            .subscriptions
            .remove(&subscription_id)
        {
            dbg!("Removed subscription from hashmap: {:?}", subscription_id);
            // Cancel the future of the subscription stream
            ctx.cancel_future(handle);
            dbg!("Cancelled subscription future: {:?}", subscription_id);

            let message = Response::SubscriptionEnded { subscription_id };
            ctx.text(serde_json::to_string(&message).unwrap());
        } else {
            error!("Subscription ID not found: {}", subscription_id);

            let error = WebsocketError::SubscriptionNotFound(subscription_id);
            ctx.text(serde_json::to_string(&error).unwrap());
        }
    }
}

impl<M> Actor for WsActor<M>
where
    M: NormalisedMessage + Serialize + DeserializeOwned + Sync + Send + 'static,
{
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        dbg!("Websocket connection established");

        // Start the heartbeat
        self.heartbeat(ctx);
    }

    fn stopped(&mut self, ctx: &mut Self::Context) {
        dbg!("Websocket connection closed");

        // Close all remaining subscriptions
        for (subscription_id, handle) in self.subscriptions.drain() {
            dbg!("Closing subscription: {:?}", subscription_id);
            ctx.cancel_future(handle);
        }
    }
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Eq)]
#[serde(tag = "method", rename_all = "lowercase")]
pub enum Command {
    Subscribe { extractor: ExtractorIdentity },
    Unsubscribe { subscription_id: Uuid },
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Eq)]
#[serde(tag = "method", rename_all = "lowercase")]
pub enum Response {
    NewSubscription { subscription_id: Uuid },
    SubscriptionEnded { subscription_id: Uuid },
}

impl<M> StreamHandler<Result<Arc<M>, ws::ProtocolError>> for WsActor<M>
where
    M: NormalisedMessage + Serialize + DeserializeOwned + Sync + Send + 'static,
{
    fn handle(&mut self, msg: Result<Arc<M>, ws::ProtocolError>, ctx: &mut Self::Context) {
        dbg!("Message received from extractor");
        match msg {
            Ok(message) => {
                dbg!("Forwarding message to client");
                ctx.text(serde_json::to_string(&message).unwrap());
            }
            Err(e) => {
                error!("Failed to receive message from extractor: {:?}", e);
            }
        }
    }
}

/// Handle incoming messages from the WS connection
impl<M> StreamHandler<Result<ws::Message, ws::ProtocolError>> for WsActor<M>
where
    M: NormalisedMessage + Serialize + DeserializeOwned + Sync + Send + 'static,
{
    fn handle(&mut self, msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        dbg!("Websocket message received.");
        match msg {
            Ok(ws::Message::Ping(msg)) => {
                dbg!("Websocket ping message received");
                self.heartbeat = Instant::now();
                ctx.pong(&msg);
            }
            Ok(ws::Message::Pong(_)) => {
                self.heartbeat = Instant::now();
            }
            Ok(ws::Message::Text(text)) => {
                dbg!("Websocket text message received: {:?}", text.clone());

                // Try to deserialize the message to a Message enum
                match serde_json::from_str::<Command>(&text) {
                    Ok(message) => {
                        // Handle the message based on its variant
                        match message {
                            Command::Subscribe { extractor } => {
                                dbg!("Subscribing to extractor: {:?}", extractor.clone());
                                self.subscribe(ctx, &extractor);
                            }
                            Command::Unsubscribe { subscription_id } => {
                                dbg!("Unsubscribing from subscription: {:?}", subscription_id);
                                self.unsubscribe(ctx, subscription_id);
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to parse message: {:?}", e);

                        let error = WebsocketError::ParseError(e);
                        ctx.text(serde_json::to_string(&error).unwrap());
                    }
                }
            }
            Ok(ws::Message::Binary(bin)) => {
                dbg!("Websocket binary message received: {:?}", bin.clone());
                ctx.binary(bin)
            }
            Ok(ws::Message::Close(reason)) => {
                dbg!("Websocket close message received: {:?}", reason.clone());
                ctx.close(reason);
                ctx.stop()
            }
            Err(e) => {
                error!("Failed to receive message: {:?}", e);
                ctx.stop()
            }
            _ => (),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::{extractor::runner::ControlMessage, models::Chain};

    use super::*;
    use actix_rt::time::timeout;
    use actix_test::{start, start_with, TestServerConfig};
    use actix_web::App;
    use futures03::SinkExt;
    use tokio::{
        net::TcpStream,
        sync::mpsc::{self, error::SendError, Receiver},
    };
    use tokio_stream::StreamExt;
    use tokio_tungstenite::{
        tungstenite::{
            protocol::{frame::coding::CloseCode, CloseFrame},
            Message,
        },
        MaybeTlsStream, WebSocketStream,
    };

    #[derive(Clone, Debug, PartialEq, Eq, Hash, Deserialize, Serialize)]
    struct DummyMessage {
        extractor_id: ExtractorIdentity,
    }

    impl DummyMessage {
        pub fn new(extractor_id: ExtractorIdentity) -> Self {
            Self { extractor_id }
        }
    }

    impl NormalisedMessage for DummyMessage {
        fn source(&self) -> ExtractorIdentity {
            self.extractor_id.clone()
        }
    }

    pub struct MyExtractorSubscriber {
        extractor_id: ExtractorIdentity,
    }

    impl MyExtractorSubscriber {
        pub fn new(extractor_id: ExtractorIdentity) -> Self {
            Self { extractor_id }
        }
    }

    impl ExtractorSubscriber<DummyMessage> for MyExtractorSubscriber {
        fn subscribe(
            &self,
        ) -> Result<Receiver<Arc<DummyMessage>>, SendError<ControlMessage<DummyMessage>>> {
            let (tx, rx) = mpsc::channel(1);
            let extractor_id = self.extractor_id.clone();

            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(tokio::time::Duration::from_secs_f32(0.1)).await;
                    dbg!("Sending DummyMessage");
                    let dummy_message = DummyMessage::new(extractor_id.clone());
                    if tx
                        .send(Arc::new(dummy_message))
                        .await
                        .is_err()
                    {
                        dbg!("Receiver dropped");
                        break
                    }
                }
            });

            Ok(rx)
        }
    }

    #[actix_rt::test]
    async fn test_websocket_ping_pong() {
        let app_state = web::Data::new(AppState::<DummyMessage>::new());
        let server = start(move || {
            App::new()
                .app_data(app_state.clone())
                .service(
                    web::resource("/ws/").route(web::get().to(WsActor::<DummyMessage>::ws_index)),
                )
        });

        let url = server
            .url("/ws/")
            .to_string()
            .replacen("http://", "ws://", 1);
        dbg!("Connecting to test server at {}", url.clone());

        // Connect to the server
        let (mut connection, _response) = tokio_tungstenite::connect_async(url)
            .await
            .expect("Failed to connect");

        dbg!("Connected to test server");

        // Test sending ping message and receiving pong message
        connection
            .send(Message::Ping(vec![]))
            .await
            .expect("Failed to send ping message");

        dbg!("Sent ping message");

        let msg = timeout(Duration::from_secs(1), connection.next())
            .await
            .expect("Failed to receive message")
            .unwrap()
            .unwrap();

        if let Message::Pong(_) = msg {
            // Pong received as expected
            dbg!("Received pong message");
        } else {
            panic!("Unexpected message {:?}", msg);
        }

        // Close the connection
        connection
            .send(Message::Close(Some(CloseFrame { code: CloseCode::Normal, reason: "".into() })))
            .await
            .expect("Failed to send close message");
        dbg!("Closed connection");
    }

    async fn wait_for_response<F>(
        connection: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
        mut criteria: F,
    ) -> Result<Message, String>
    where
        F: FnMut(&Message) -> bool,
    {
        loop {
            let response_msg = timeout(Duration::from_secs(5), connection.next())
                .await
                .map_err(|_| "Failed to receive message".to_string())?
                .ok_or("Connection closed".to_string())?
                .map_err(|_| "Failed to receive message".to_string())?;

            if criteria(&response_msg) {
                return Ok(response_msg)
            } else {
                dbg!("Message did not meet criteria, waiting for the correct message");
            }
        }
    }

    async fn wait_for_dummy_message(
        connection: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
        extractor_id: ExtractorIdentity,
    ) -> Result<DummyMessage, String> {
        let criteria = move |msg: &Message| {
            if let Message::Text(text) = msg {
                if let Ok(message) = serde_json::from_str::<DummyMessage>(text) {
                    return message.extractor_id == extractor_id
                }
            }
            false
        };

        if let Message::Text(response_text) = wait_for_response(connection, criteria).await? {
            serde_json::from_str(&response_text).map_err(|e| e.to_string())
        } else {
            Err("Received a non-text message".to_string())
        }
    }

    async fn wait_for_new_subscription(
        connection: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    ) -> Result<Response, String> {
        let criteria = |msg: &Message| {
            if let Message::Text(text) = msg {
                if let Ok(message) = serde_json::from_str::<Response>(text) {
                    matches!(message, Response::NewSubscription { .. })
                } else {
                    false
                }
            } else {
                false
            }
        };

        if let Message::Text(response_text) = wait_for_response(connection, criteria).await? {
            serde_json::from_str(&response_text).map_err(|e| e.to_string())
        } else {
            Err("Received a non-text message".to_string())
        }
    }

    async fn wait_for_subscription_ended(
        connection: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    ) -> Result<Response, String> {
        let criteria = |msg: &Message| {
            if let Message::Text(text) = msg {
                if let Ok(message) = serde_json::from_str::<Response>(text) {
                    matches!(message, Response::SubscriptionEnded { .. })
                } else {
                    false
                }
            } else {
                false
            }
        };

        if let Message::Text(response_text) = wait_for_response(connection, criteria).await? {
            serde_json::from_str(&response_text).map_err(|e| e.to_string())
        } else {
            Err("Received a non-text message".to_string())
        }
    }

    #[actix_rt::test]
    async fn test_subscribe_and_unsubscribe() -> Result<(), String> {
        // Add the extractor handle to AppState
        let extractor_id = ExtractorIdentity::new(Chain::Ethereum, "dummy");
        let extractor_id2 = ExtractorIdentity::new(Chain::Ethereum, "dummy2");

        let app_state = web::Data::new(AppState::<DummyMessage>::new());

        let extractor_subscriber = Arc::new(MyExtractorSubscriber::new(extractor_id.clone()));
        let extractor_subscriber2 = Arc::new(MyExtractorSubscriber::new(extractor_id2.clone()));

        {
            let mut subscribers = app_state.subscribers.lock().unwrap();
            subscribers.insert(extractor_id.clone(), extractor_subscriber);
            subscribers.insert(extractor_id2.clone(), extractor_subscriber2);
        }

        // Setup WebSocket server and client, similar to existing test
        let server = start_with(
            TestServerConfig::default().client_request_timeout(Duration::from_secs(5)),
            move || {
                App::new()
                    .app_data(app_state.clone())
                    .service(
                        web::resource("/ws/")
                            .route(web::get().to(WsActor::<DummyMessage>::ws_index)),
                    )
            },
        );

        let url = server
            .url("/ws/")
            .to_string()
            .replacen("http://", "ws://", 1);
        dbg!("Connecting to test server at {}", url.clone());

        // Connect to the server
        let (mut connection, _response) = tokio_tungstenite::connect_async(url)
            .await
            .expect("Failed to connect");

        dbg!("Connected to test server");

        // Create and send a subscribe message from the client
        let action = Command::Subscribe { extractor: extractor_id.clone() };
        connection
            .send(Message::Text(serde_json::to_string(&action).unwrap()))
            .await
            .expect("Failed to send subscribe message");
        dbg!("Sent subscribe message");

        // Accept the subscription ID
        let response = wait_for_new_subscription(&mut connection)
            .await
            .expect("Failed to get the expected new subscription message");
        let first_subscription_id = if let Response::NewSubscription {
            subscription_id: first_subscription_id,
        } = response
        {
            dbg!("Received first subscription ID: {:?}", first_subscription_id);
            first_subscription_id
        } else {
            panic!("Unexpected response: {:?}", response);
        };

        // Receive the DummyMessage from the server
        let _message = wait_for_dummy_message(&mut connection, extractor_id.clone())
            .await
            .expect("Failed to get the expected DummyMessage");
        dbg!("Received DummyMessage from server");

        // Create and send a second subscribe message from the client
        let action = Command::Subscribe { extractor: extractor_id2.clone() };
        connection
            .send(Message::Text(serde_json::to_string(&action).unwrap()))
            .await
            .expect("Failed to send subscribe message");
        dbg!("Sent subscribe message for second extractor");

        // Accept the second subscription ID
        let response = wait_for_new_subscription(&mut connection)
            .await
            .expect("Failed to get the expected new subscription message");
        if let Response::NewSubscription { subscription_id: second_subscription_id } = response {
            dbg!("Received second subscription ID: {:?}", second_subscription_id);
        } else {
            panic!("Unexpected response: {:?}", response);
        }

        // Receive the DummyMessage from the second exractor
        let _message = wait_for_dummy_message(&mut connection, extractor_id2.clone())
            .await
            .expect("Failed to get the expected DummyMessage");
        dbg!("Received DummyMessage2 from server");

        // Create and send a unsubscribe message from the client
        let action = Command::Unsubscribe { subscription_id: first_subscription_id };
        connection
            .send(Message::Text(serde_json::to_string(&action).unwrap()))
            .await
            .expect("Failed to send unsubscribe message");
        dbg!("Sent unsubscribe message");

        // Accept the unsubscription ID
        let response = wait_for_subscription_ended(&mut connection)
            .await
            .expect("Failed to get the expected subscription ended message");
        if let Response::SubscriptionEnded { subscription_id } = response {
            dbg!("Received unsubscription ID: {:?}", subscription_id);
        } else {
            panic!("Unexpected response: {:?}", response);
        }

        // Try to receive a DummyMessage from the first extractor (expecting timeout to occur)
        let result =
            timeout(Duration::from_secs(2), wait_for_dummy_message(&mut connection, extractor_id))
                .await;
        assert!(result.is_err(), "Received a message from the first extractor after unsubscribing");

        // Receive the DummyMessage from the second exractor
        let _message = wait_for_dummy_message(&mut connection, extractor_id2)
            .await
            .expect("Failed to get the expected DummyMessage");
        dbg!("Received DummyMessage2 from server");

        // Close the connection
        connection
            .send(Message::Close(Some(CloseFrame { code: CloseCode::Normal, reason: "".into() })))
            .await
            .expect("Failed to send close message");
        dbg!("Closed connection");

        Ok(())
    }
}
