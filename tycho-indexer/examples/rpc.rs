/// Script to run only the RPC server
/// Usage: cargo run --example rpc
use diesel_async::{pooled_connection::deadpool::Pool, AsyncPgConnection};
use futures03::future::select_all;

use extractor::{
    evm::ambient::{AmbientContractExtractor, AmbientPgGateway},
    runner::{ExtractorHandle, ExtractorRunnerBuilder},
};
use models::Chain;

use crate::{
    extractor::{evm, ExtractionError},
    services::ServicesBuilder,
    storage::postgres::{self, cache::CachedGateway, PostgresGateway},
};
use actix_web::dev::ServerHandle;
use clap::Parser;
use core::panic;
use std::sync::Arc;
use tokio::{sync::mpsc, task, task::JoinHandle};
use tracing::info;

use tycho_indexer::{
    extractor, hex_bytes, models, pb, serde_helpers, services, storage, substreams,
};

#[tokio::main]
async fn main() -> Result<(), ExtractionError> {
    // Set up the subscriber
    tracing_subscriber::fmt::init();

    let pool =
        postgres::connect(&"postgres://postgres:mypassword@localhost:5432/tycho_indexer_0").await?;
    let evm_gw = PostgresGateway::<
        evm::Block,
        evm::Transaction,
        evm::Account,
        evm::AccountUpdate,
        evm::ERC20Token,
    >::new(pool.clone())
    .await?;

    info!("Starting Tycho RPC");

    let server_addr = "0.0.0.0";
    let server_port = 4242;
    let server_version_prefix = "v1";
    let server_url = format!("http://{}:{}", server_addr, server_port);
    let (server_handle, server_task) = ServicesBuilder::new(evm_gw, pool)
        .prefix(server_version_prefix)
        .bind(server_addr)
        .port(server_port)
        .run()?;
    info!(server_url, "Http and Ws server started");
    let shutdown_task = tokio::spawn(shutdown_handler(server_handle));
    let (res, _, _) = select_all([server_task, shutdown_task]).await;
    res.expect("Extractor- nor ServiceTasks should panic!")
}

async fn shutdown_handler(server_handle: ServerHandle) -> Result<(), ExtractionError> {
    // listen for ctrl-c
    tokio::signal::ctrl_c().await.unwrap();
    server_handle.stop(true).await;
    Result::<(), ExtractionError>::Ok(())
}
