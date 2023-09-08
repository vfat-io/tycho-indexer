//! This module contains Tycho WS implementation

use actix::{Actor, ActorContext, StreamHandler};
use actix_web::{web, App, Error, HttpRequest, HttpResponse, HttpServer};
use actix_web_actors::ws;
use std::sync::Mutex;
use tracing::info;
use tracing_actix_web::TracingLogger;
use uuid::Uuid;

/// Application data shared between threads
struct AppState {
    /// Application variable wrapped in Mutext to allow for sharing between threads
    app_name: Mutex<String>,
}

struct WsActor {
    _id: Uuid,
    _app_name: String,
}

impl WsActor {
    fn new(app_name: String) -> Self {
        Self { _id: Uuid::new_v4(), _app_name: app_name }
    }
}

impl Actor for WsActor {
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        info!("Websocket connection established");
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!("Websocket connection closed");
    }
}

/// Handle incoming messages from the WS connection
impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for WsActor {
    fn handle(&mut self, msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        info!("Websocket message received: {:?}", msg);
        match msg {
            Ok(ws::Message::Ping(msg)) => ctx.pong(&msg),
            Ok(ws::Message::Pong(_)) => (),
            Ok(ws::Message::Text(text)) => {
                info!("Websocket text message received: {:?}", text);
                ctx.text(text)
            }
            Ok(ws::Message::Binary(bin)) => {
                info!("Websocket binary message received: {:?}", bin);
                ctx.binary(bin)
            }
            Ok(ws::Message::Close(reason)) => {
                info!("Websocket close message received: {:?}", reason);
                ctx.close(reason);
                ctx.stop()
            }
            _ => ctx.stop(),
        }
    }
}

async fn ws_index(
    req: HttpRequest,
    stream: web::Payload,
    data: web::Data<AppState>,
) -> Result<HttpResponse, Error> {
    let app_name = data.app_name.lock().unwrap();
    ws::start(WsActor::new(app_name.clone()), &req, stream)
}

/// Start the HTTP server
/// You can test the server by running:
/// ```bash
/// websocat -v ws://127.0.0.1:8080/ws/ --ping-interval=1
/// ```
#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let state = web::Data::new(AppState { app_name: "Tycho Indexer".to_string().into() });
    info!("Starting server");
    HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            .service(web::resource("/ws/").route(web::get().to(ws_index)))
            .wrap(TracingLogger::default())
    })
    .workers(2)
    .bind("127.0.0.1:8080")?
    .run()
    .await
}
