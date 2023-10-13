#![doc = include_str!("../../Readme.md")]
use diesel_async::{pooled_connection::deadpool::Pool, AsyncPgConnection};
use extractor::{
    evm::{
        ambient::{AmbientContractExtractor, AmbientPgGateway},
        EVMStateGateway,
    },
    runner::{ExtractorHandle, ExtractorRunnerBuilder},
};
use futures03::future::select_all;
use models::Chain;

use actix_web::dev::ServerHandle;
use clap::Parser;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tracing::info;

mod extractor;
mod hex_bytes;
mod models;
mod pb;
mod serde_helpers;
mod services;
mod storage;
mod substreams;

use crate::{
    extractor::{evm, evm::BlockAccountChanges, ExtractionError},
    models::NormalisedMessage,
    services::ServicesBuilder,
    storage::postgres::{self, PostgresGateway},
};

#[cfg(test)]
#[macro_use]
extern crate pretty_assertions;

/// Tycho Indexer using Substreams
///
/// Extracts state from the Ethereum blockchain and stores it in a Postgres database.
#[derive(Parser, Debug, Clone, PartialEq, Eq)]
#[clap(version = "0.1.0")]
struct CliArgs {
    /// Substreams API endpoint URL
    #[clap(name = "endpoint", long)]
    endpoint_url: String,

    /// Substreams API token
    ///
    /// Defaults to SUBSTREAMS_API_TOKEN env var.
    #[clap(long, env, hide_env_values = true, alias = "api_token")]
    substreams_api_token: String,

    /// DB Connection Url
    ///
    /// Defaults to DATABASE_URL env var.
    #[clap(long, env, hide_env_values = true, alias = "db_url")]
    database_url: String,

    /// Substreams Package file
    #[clap(long)]
    spkg: String,

    /// Substreams Module name
    #[clap(long)]
    module: String,

    /// Substreams start block
    /// Defaults to START_BLOCK env var or default_value below.
    #[clap(long, env, default_value = "17361664")]
    start_block: i64,

    /// Substreams stop block
    ///
    /// If prefixed with a `+` the value is interpreted as an increment to the start block.
    /// Defaults to STOP_BLOCK env var or default_value below.
    #[clap(long, env, default_value = "17362664")]
    stop_block: String,
}

impl CliArgs {
    fn stop_block(&self) -> i64 {
        if self.stop_block.starts_with('+') {
            let increment: i64 = self.stop_block[1..]
                .parse()
                .expect("Invalid increment value");
            self.start_block + increment
        } else {
            self.stop_block
                .parse()
                .expect("Invalid stop block value")
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), ExtractionError> {
    // Set up the subscriber
    tracing_subscriber::fmt::init();

    let args = CliArgs::parse();

    let pool = postgres::connect(&args.database_url).await?;
    postgres::ensure_chains(&[Chain::Ethereum], pool.clone()).await;
    let evm_gw =
        PostgresGateway::<evm::Block, evm::Transaction, evm::Account, evm::AccountUpdate>::new(
            pool.clone(),
        )
        .await?;

    info!("Starting Tycho");
    let mut extractor_handles = Vec::new();
    let (ambient_task, ambient_handle) =
        start_ambient_extractor(&args, pool.clone(), evm_gw.clone()).await?;
    extractor_handles.push(ambient_handle.clone());
    info!("Extractor {} started!", ambient_handle.get_id());

    info!("Starting WS and RPC service");
    let (server_handle, server_task) = ServicesBuilder::new(evm_gw, pool)
        .prefix("v1")
        .bind("127.0.0.1")
        .port(4242)
        .register_extractor(ambient_handle)
        .run()?;
    let shutdown_task = tokio::spawn(shutdown_handler(server_handle, extractor_handles));
    let (res, _, _) = select_all([ambient_task, server_task, shutdown_task]).await;
    res.expect("Extractor- nor ServiceTasks should panic!")
}

async fn start_ambient_extractor(
    args: &CliArgs,
    pool: Pool<AsyncPgConnection>,
    evm_gw: EVMStateGateway<AsyncPgConnection>,
) -> Result<
    (JoinHandle<Result<(), ExtractionError>>, ExtractorHandle<BlockAccountChanges>),
    ExtractionError,
> {
    let ambient_name = "vm:ambient";
    let ambient_gw = AmbientPgGateway::new(ambient_name, Chain::Ethereum, pool, evm_gw);
    let extractor =
        AmbientContractExtractor::new(ambient_name, Chain::Ethereum, ambient_gw).await?;

    let start_block = args.start_block;
    let stop_block = args.stop_block();
    let spkg = &args.spkg;
    info!(%ambient_name, %start_block, %stop_block, block_span = stop_block - start_block, %spkg, "Starting Ambient extractor");
    let builder = ExtractorRunnerBuilder::new(&args.spkg, Arc::new(extractor))
        .start_block(start_block)
        .end_block(stop_block);
    builder.run().await
}

async fn shutdown_handler<M: NormalisedMessage>(
    server_handle: ServerHandle,
    extractors: Vec<ExtractorHandle<M>>,
) -> Result<(), ExtractionError> {
    // listen for ctrl-c
    tokio::signal::ctrl_c().await.unwrap();
    for e in extractors.iter() {
        e.stop().await.unwrap();
    }
    server_handle.stop(true).await;
    Ok(())
}

#[cfg(test)]
mod cli_tests {
    use std::env;

    use super::CliArgs;
    use clap::Parser;

    #[tokio::test]
    async fn test_arg_parsing_long() {
        // Set the SUBSTREAMS_API_TOKEN environment variable for testing.
        env::set_var("SUBSTREAMS_API_TOKEN", "your_api_token");
        env::set_var("DATABASE_URL", "my_db");
        let args = CliArgs::try_parse_from(vec![
            "tycho-indexer",
            "--endpoint",
            "http://example.com",
            "--spkg",
            "package.spkg",
            "--module",
            "module_name",
        ]);

        env::remove_var("SUBSTREAMS_API_TOKEN");
        env::remove_var("DATABASE_URL");
        assert!(args.is_ok());
        let args = args.unwrap();
        let expected_args = CliArgs {
            endpoint_url: "http://example.com".to_string(),
            substreams_api_token: "your_api_token".to_string(),
            database_url: "my_db".to_string(),
            spkg: "package.spkg".to_string(),
            module: "module_name".to_string(),
            start_block: 17361664,
            stop_block: "17362664".to_string(),
        };

        assert_eq!(args, expected_args);
    }

    #[tokio::test]
    async fn test_arg_parsing_missing_val() {
        let args = CliArgs::try_parse_from(vec![
            "tycho-indexer",
            "--spkg",
            "package.spkg",
            "--module",
            "module_name",
        ]);

        assert!(args.is_err());
    }
}
