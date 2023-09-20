//! This module contains Tycho web services implementation

use std::{collections::HashMap, sync::Arc};

use actix_web::{dev::ServerHandle, web, App, HttpServer};
use tokio::task::JoinHandle;

use crate::{
    extractor::{runner::ExtractorHandle, ExtractionError},
    models::NormalisedMessage,
};

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

impl<M> Default for ServicesBuilder<M> {
    fn default() -> Self {
        Self {
            prefix: "v1".to_owned(),
            port: 4242,
            bind: "0.0.0.0".to_owned(),
            extractor_handles: HashMap::new(),
        }
    }
}

impl<M: NormalisedMessage> ServicesBuilder<M> {
    pub fn new() -> Self {
        Self::default()
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

    pub fn run(
        self,
    ) -> Result<(ServerHandle, JoinHandle<Result<(), ExtractionError>>), ExtractionError> {
        let app_state = web::Data::new(AppState::<M>::new(self.extractor_handles));
        let server = HttpServer::new(move || {
            App::new()
                .app_data(app_state.clone())
                .service(
                    web::resource(format!("/{}/ws/", self.prefix))
                        .route(web::get().to(WsActor::<M>::ws_index)),
                )
        })
        .bind((self.bind, self.port))
        .map_err(|err| ExtractionError::ServiceError(err.to_string()))?
        .run();
        let handle = server.handle();
        let server = async move {
            let res = server.await;
            res.map_err(|err| ExtractionError::Unknown(err.to_string()))
        };
        let task = tokio::spawn(server);
        Ok((handle, task))
    }
}
