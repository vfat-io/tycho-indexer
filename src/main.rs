#![allow(dead_code)] // FIXME: remove after initial development
#![doc = include_str!("../Readme.md")]
use anyhow::Error;
use diesel_async::{pooled_connection::bb8::Pool, AsyncPgConnection};
use extractor::{
    evm::{
        ambient::{AmbientContractExtractor, AmbientPgGateway},
        EVMStateGateway,
    },
    runner::{ExtractorHandle, ExtractorRunnerBuilder},
};
use models::Chain;

use std::sync::Arc;
use tracing::info;

mod extractor;
mod models;
mod pb;
mod services;
mod storage;
mod substreams;

use clap::Parser;
use futures03::try_join;

use crate::{
    extractor::evm,
    services::ServicesBuilder,
    storage::postgres::{self, PostgresGateway},
};

#[cfg(test)]
#[macro_use]
extern crate pretty_assertions;

/// Tycho Indexer using Substreams
#[derive(Parser, Debug, Clone, PartialEq, Eq)]
#[clap(
    version = "0.1.0",
    about = "Extracts Ambient contract state. Requires the environment variable \
    SUBSTREAMS_API_TOKEN and DATABASE_URL to be set."
)]
struct Args {
    /// Substreams API endpoint URL
    #[clap(name = "endpoint", short, long)]
    endpoint_url: String,

    /// Substreams API token
    #[clap(long, env)]
    substreams_api_token: String,

    /// DB Connection Url
    #[clap(long, env, short)]
    db_url: String,

    /// Substreams Package file
    #[clap(name = "spkg", short, long)]
    package_file: String,

    /// Substreams Module name
    #[clap(name = "module", short, long)]
    module_name: String,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Set up the subscriber
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    let pool = postgres::connect(&args.db_url).await?;
    postgres::ensure_chains(&[Chain::Ethereum], pool.clone()).await;
    let evm_gw =
        PostgresGateway::<evm::Block, evm::Transaction, evm::Account, evm::AccountUpdate>::new(
            pool.clone(),
        )
        .await?;

    info!("Starting Tycho");
    let mut ambient_handle =
        start_ambient_extractor(&args.package_file, pool.clone(), evm_gw.clone()).await?;
    info!("{} started!", ambient_handle.get_id());

    let ambient_fut = ambient_handle.join_handle()?;
    let ambient_fut = async {
        let _ = ambient_fut.await;
        // TODO: change ExtractionRunner output type to return result
        Ok::<(), std::io::Error>(())
    };

    info!("Starting services");
    let services_fut = ServicesBuilder::new()
        .register_extractor(ambient_handle)
        .run();
    try_join!(ambient_fut, services_fut)
        .map(|_| ())
        .map_err(|err| err.into())
}

async fn start_ambient_extractor(
    spkg: &str,
    pool: Pool<AsyncPgConnection>,
    evm_gw: EVMStateGateway<AsyncPgConnection>,
) -> Result<ExtractorHandle<evm::BlockAccountChanges>, Error> {
    let ambient_gw = AmbientPgGateway::new("vm:ambient", Chain::Ethereum, pool, evm_gw);
    let extractor =
        AmbientContractExtractor::new("vm:ambient", Chain::Ethereum, ambient_gw).await?;

    let builder = ExtractorRunnerBuilder::new(spkg, Arc::new(extractor))
        .start_block(17361664)
        // for testing only
        .end_block(17375000);
    let handle = builder.run().await?;
    Ok(handle)
}

#[cfg(test)]
mod cli_tests {
    use std::env;

    use super::Args;
    use clap::Parser;

    #[tokio::test]
    async fn test_arg_parsing_short() {
        // Set the SUBSTREAMS_API_TOKEN environment variable for testing.
        env::set_var("SUBSTREAMS_API_TOKEN", "your_api_token");
        env::set_var("DB_URL", "my_db");

        let args = Args::try_parse_from(vec![
            "tycho-indexer",
            "-e",
            "http://example.com",
            "-s",
            "package.spkg",
            "-m",
            "module_name",
        ]);

        env::remove_var("SUBSTREAMS_API_TOKEN");
        env::remove_var("DB_URL");

        assert!(args.is_ok());
        let args = args.unwrap();
        let expected_args = Args {
            endpoint_url: "http://example.com".to_string(),
            substreams_api_token: "your_api_token".to_string(),
            db_url: "my_db".to_string(),
            package_file: "package.spkg".to_string(),
            module_name: "module_name".to_string(),
        };

        assert_eq!(args, expected_args);
    }

    #[tokio::test]
    async fn test_arg_parsing_long() {
        // Set the SUBSTREAMS_API_TOKEN environment variable for testing.
        env::set_var("SUBSTREAMS_API_TOKEN", "your_api_token");
        env::set_var("DB_URL", "my_db");
        let args = Args::try_parse_from(vec![
            "tycho-indexer",
            "--endpoint",
            "http://example.com",
            "--spkg",
            "package.spkg",
            "--module",
            "module_name",
        ]);

        env::remove_var("SUBSTREAMS_API_TOKEN");
        env::remove_var("DB_URL");
        assert!(args.is_ok());
        let args = args.unwrap();
        let expected_args = Args {
            endpoint_url: "http://example.com".to_string(),
            substreams_api_token: "your_api_token".to_string(),
            db_url: "my_db".to_string(),
            package_file: "package.spkg".to_string(),
            module_name: "module_name".to_string(),
        };

        assert_eq!(args, expected_args);
    }

    #[tokio::test]
    async fn test_arg_parsing_missing_val() {
        let args = Args::try_parse_from(vec![
            "tycho-indexer",
            "--spkg",
            "package.spkg",
            "--module",
            "module_name",
        ]);

        assert!(args.is_err());
    }
}
