//! This module contains Tycho Websocket implementation

use crate::{
    extractor::runner::ExtractorHandle,
    models::{ExtractorIdentity, NormalisedMessage},
};
use actix::{Actor, ActorContext, ActorFutureExt, AsyncContext, SpawnHandle, StreamHandler};
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
use tracing::{debug, error, info};
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

/// Shared application data between all connections
/// Parameters are hidden behind a Mutex to allow for sharing between threads
pub struct AppState<M> {
    /// There is one extractor handle per extractor
    pub extractors: Arc<Mutex<HashMap<ExtractorIdentity, ExtractorHandle<M>>>>,
}

impl<M> AppState<M> {
    pub fn new() -> Self {
        Self { extractors: Arc::new(Mutex::new(HashMap::new())) }
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

    fn subscribe(
        &mut self,
        ctx: &mut ws::WebsocketContext<Self>,
        extractor_id: &ExtractorIdentity,
    ) {
        info!("Subscribing to extractor: {:?}", extractor_id);

        let extractors_guard = self
            .app_state
            .extractors
            .lock()
            .unwrap();

        if let Some(extractor_handle) = extractors_guard.get(extractor_id) {
            // Generate a unique ID for this subscription
            let _subscription_id = Uuid::new_v4();

            let fut = async move {
                match extractor_handle.subscribe().await {
                    Ok(mut rx) => {
                        let stream = async_stream::stream! {
                            while let Some(item) = rx.recv().await {
                                yield item;
                            }
                        };
                        Ok((stream, _subscription_id))
                    }
                    Err(e) => Err(e),
                }
            };

            let fut = actix::fut::wrap_future::<_, Self>(fut);
            let fut = fut.map(|res, act, ctx| {
                match res {
                    Ok((stream, subscription_id)) => {
                        let handle = Self::add_stream(stream, ctx);
                        act.subscriptions
                            .insert(subscription_id, handle);
                    }
                    Err(e) => {
                        // Handle error properly here
                        println!("Failed to subscribe: {:?}", e);
                    }
                }
            });
            ctx.spawn(fut);
        } else {
            error!("Extractor not found: {:?}", extractor_id);

            let error = WebsocketError::ExtractorNotFound(extractor_id.clone());
            ctx.text(serde_json::to_string(&error).unwrap());
        }
    }

    fn unsubscribe(&mut self, ctx: &mut ws::WebsocketContext<Self>, subscription_id: Uuid) {
        info!("Unsubscribing from subscription: {:?}", subscription_id);

        if let Some(handle) = self
            .subscriptions
            .remove(&subscription_id)
        {
            // Cancel the future of the subscription stream
            ctx.cancel_future(handle);

            let message = OutgoingMessage::<M>::SubscriptionEnded { subscription_id };

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

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        dbg!("Websocket connection closed");
    }
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Eq)]
#[serde(tag = "method", rename_all = "lowercase")]
pub enum IncomingMessage {
    Subscribe { extractor: ExtractorIdentity },
    Unsubscribe { subscription_id: Uuid },
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Eq)]
#[serde(tag = "method", rename_all = "lowercase")]
pub enum OutgoingMessage<M>
where
    M: NormalisedMessage + Sync + Send + 'static,
{
    NewSubscription { subscription_id: Uuid },
    SubscriptionEnded { subscription_id: Uuid },
    ForwardFromExtractor { message: Arc<M> },
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
            _ => ctx.stop(),
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
                match serde_json::from_str::<IncomingMessage>(&text) {
                    Ok(message) => {
                        // Handle the message based on its variant
                        match message {
                            IncomingMessage::Subscribe { extractor } => {
                                dbg!("Subscribing to extractor: {:?}", extractor.clone());
                                self.subscribe(ctx, &extractor);
                            }
                            IncomingMessage::Unsubscribe { subscription_id } => {
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
            _ => ctx.stop(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::models::Chain;

    use super::*;
    use actix_rt::time::timeout;
    use actix_test::start;
    use actix_web::App;
    use futures03::SinkExt;
    use tokio::{sync::mpsc, task::JoinHandle};
    use tokio_stream::StreamExt;
    use tokio_tungstenite::tungstenite::{
        protocol::{frame::coding::CloseCode, CloseFrame},
        Message,
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
        println!("Connecting to test server at {}", url);

        // Connect to the server
        let (mut connection, _response) = tokio_tungstenite::connect_async(url)
            .await
            .expect("Failed to connect");

        println!("Connected to test server");

        // Test sending ping message and receiving pong message
        connection
            .send(Message::Ping(vec![]))
            .await
            .expect("Failed to send ping message");

        println!("Sent ping message");

        let msg = timeout(Duration::from_secs(1), connection.next())
            .await
            .expect("Failed to receive message")
            .unwrap()
            .unwrap();

        if let Message::Pong(_) = msg {
            // Pong received as expected
            info!("Received pong message");
        } else {
            panic!("Unexpected message {:?}", msg);
        }

        // Close the connection
        connection
            .send(Message::Close(Some(CloseFrame { code: CloseCode::Normal, reason: "".into() })))
            .await
            .expect("Failed to send close message");
        println!("Closed connection");
    }

    #[actix_rt::test]
    async fn test_subscribe_and_unsubscribe() -> Result<(), String> {
        let (ctrl_tx, _ctrl_rx) = mpsc::channel(1);
        let handle: JoinHandle<()> = tokio::spawn(async {});
        let extractor_handle = ExtractorHandle::<DummyMessage>::new(handle, ctrl_tx);

        // Add the extractor handle to AppState
        let extractor_name = "dummy".to_string();
        let extractor_id =
            ExtractorIdentity { chain: Chain::Ethereum, name: extractor_name.clone() };
        let app_state = web::Data::new(AppState::<DummyMessage>::new());

        app_state
            .extractors
            .lock()
            .unwrap()
            .insert(extractor_id.clone(), extractor_handle);

        // Setup WebSocket server and client, similar to existing test
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
        println!("Connecting to test server at {}", url);

        // Connect to the server
        let (mut connection, _response) = tokio_tungstenite::connect_async(url)
            .await
            .expect("Failed to connect");

        println!("Connected to test server");

        // Create and send a subscribe message from the client
        let action = IncomingMessage::Subscribe { extractor: extractor_id };
        connection
            .send(Message::Text(serde_json::to_string(&action).unwrap()))
            .await
            .expect("Failed to send subscribe message");

        // Receive the new subscription ID from the server
        let response_msg = timeout(Duration::from_secs(1), connection.next())
            .await
            .expect("Failed to receive message")
            .unwrap()
            .unwrap();
        let new_subscription_id = if let Message::Text(response_msg) = response_msg {
            let message: OutgoingMessage<DummyMessage> =
                serde_json::from_str(&response_msg).expect("Failed to parse message");
            match message {
                OutgoingMessage::NewSubscription { subscription_id } => {
                    info!("Received new subscription ID: {:?}", subscription_id);
                    subscription_id
                }
                _ => panic!("Unexpected outgoing message: {:?}", message),
            }
        } else {
            panic!("Unexpected message {:?}", response_msg);
        };

        // Create and send a unsubscribe message from the client
        let action = IncomingMessage::Unsubscribe { subscription_id: new_subscription_id };
        connection
            .send(Message::Text(serde_json::to_string(&action).unwrap()))
            .await
            .expect("Failed to send unsubscribe message");

        // Get the confirmation that the subscription has ended
        let response_msg = timeout(Duration::from_secs(1), connection.next())
            .await
            .expect("Failed to receive message")
            .unwrap()
            .unwrap();
        let ended_subscription_id = if let Message::Text(response_msg) = response_msg {
            let message: OutgoingMessage<DummyMessage> =
                serde_json::from_str(&response_msg).expect("Failed to parse message");
            match message {
                OutgoingMessage::SubscriptionEnded { subscription_id } => {
                    info!("Received ended subscription ID: {:?}", subscription_id);
                    subscription_id
                }
                _ => panic!("Unexpected outgoing message: {:?}", message),
            }
        } else {
            panic!("Unexpected message {:?}", response_msg);
        };
        assert_eq!(new_subscription_id, ended_subscription_id);

        Ok(())
    }
}
