//! This module contains Tycho web services implementation

use std::{collections::HashMap, sync::Arc};

use actix_web::{web, App, HttpServer};

use crate::{extractor::runner::ExtractorHandle, models::NormalisedMessage};

use self::ws::{AppState, MessageSenderMap, WsActor};

pub mod deserialization_helpers;
mod rpc;
mod ws;

pub struct ServicesBuilder<M> {
    prefix: String,
    port: u16,
    bind: String,
    extractor_handles: MessageSenderMap<M>,
}

impl<M: NormalisedMessage> ServicesBuilder<M> {
    pub fn new() -> Self {
        Self {
            prefix: "v1".to_owned(),
            port: 4242,
            bind: "0.0.0.0".to_owned(),
            extractor_handles: HashMap::new(),
        }
    }

    pub fn register_extractor(mut self, handle: ExtractorHandle<M>) -> Self {
        let id = handle.get_id();
        self.extractor_handles
            .insert(id, Arc::new(handle));
        self
    }

    pub fn prefix(mut self, v: &str) -> Self {
        self.prefix = v.to_owned();
        self
    }

    pub fn bind(mut self, v: &str) -> Self {
        self.bind = v.to_owned();
        self
    }

    pub fn port(mut self, v: u16) -> Self {
        self.port = v;
        self
    }

    pub async fn run(self) -> std::io::Result<()> {
        let app_state = web::Data::new(AppState::<M>::new(self.extractor_handles));
        HttpServer::new(move || {
            App::new()
                .app_data(app_state.clone())
                .service(
                    web::resource(format!("/{}/ws/", self.prefix))
                        .route(web::get().to(WsActor::<M>::ws_index)),
                )
        })
        .bind((self.bind, self.port))?
        .run()
        .await
    }
}
