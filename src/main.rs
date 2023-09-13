#![allow(dead_code)] // FIXME: remove after initial development

//! # Substreams Rust Sink Boilerplate
//!
//! Currently this module only contains the raw boilerplate code to
//! connect to a substream from within rust.
use anyhow::{format_err, Context, Error};
use futures03::StreamExt;
use pb::sf::substreams::{
    rpc::v2::{BlockScopedData, BlockUndoSignal},
    v1::Package,
};

use prost::Message;
use std::sync::Arc;
use substreams::{
    stream::{BlockResponse, SubstreamsStream},
    SubstreamsEndpoint,
};
use tracing::info;

mod extractor;
mod models;
mod pb;
mod services;
mod storage;
mod substreams;

use clap::Parser;

use crate::pb::tycho::evm::v1::BlockContractChanges;

/// Tycho Indexer using Substreams
#[derive(Parser, Debug, Clone, PartialEq, Eq)]
#[clap(
    version = "0.1.0",
    about = "Does awesome things. Requires the environment variable SUBSTREAMS_API_TOKEN to be set."
)]
struct Args {
    /// Substreams API endpoint URL
    #[clap(name = "endpoint", short, long)]
    endpoint_url: String,

    /// Substreams API token
    #[clap(long, env)]
    substreams_api_token: String,

    /// Package file
    #[clap(name = "spkg", short, long)]
    package_file: String,

    /// Module name
    #[clap(name = "module", short, long)]
    module_name: String,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Set up the subscriber
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    let package = read_package(&args.package_file).await?;
    let endpoint = Arc::new(
        SubstreamsEndpoint::new(&args.endpoint_url, Some(args.substreams_api_token)).await?,
    );

    let cursor: Option<String> = load_persisted_cursor()?;

    let mut stream = SubstreamsStream::new(
        endpoint.clone(),
        cursor,
        package.modules.clone(),
        args.module_name,
        // Start/stop block are not handled within this project, feel free to play with it
        17361664, // FIXME: remove magic value
        17362000, // FIXME: remove magic value
    );

    info!("Starting stream");

    loop {
        match stream.next().await {
            None => {
                info!("Stream consumed");
                break
            }
            Some(Ok(BlockResponse::New(data))) => {
                process_block_scoped_data(&data)?;
                persist_cursor(data.cursor)?;
            }
            Some(Ok(BlockResponse::Undo(undo_signal))) => {
                process_block_undo_signal(&undo_signal)?;
                persist_cursor(undo_signal.last_valid_cursor)?;
            }
            Some(Err(err)) => {
                panic!("Stream terminated with error: {:?}", err);
            }
        }
    }

    Ok(())
}

fn process_block_scoped_data(data: &BlockScopedData) -> Result<(), Error> {
    let output = data
        .output
        .as_ref()
        .unwrap()
        .map_output
        .as_ref()
        .unwrap();

    let block_changes = BlockContractChanges::decode(output.value.as_slice())?;

    info!(
        "Block #{} - Payload {} ({} bytes)",
        data.clock.as_ref().unwrap().number,
        output
            .type_url
            .replace("type.googleapis.com/", ""),
        output.value.len()
    );
    info!("Message: {:?}", block_changes);
    Ok(())
}

fn process_block_undo_signal(_undo_signal: &BlockUndoSignal) -> Result<(), Error> {
    // `BlockUndoSignal` must be treated as "delete every data that has been recorded after
    // block height specified by block in BlockUndoSignal". In the example above, this means
    // you must delete changes done by `Block #7b` and `Block #6b`. The exact details depends
    // on your own logic. If for example all your added record contain a block number, a
    // simple way is to do `delete all records where block_num > 5` which is the block num
    // received in the `BlockUndoSignal` (this is true for append only records, so when only
    // `INSERT` are allowed).
    unimplemented!("you must implement some kind of block undo handling, or request only final blocks (tweak substreams_stream.rs)")
}

fn persist_cursor(_cursor: String) -> Result<(), Error> {
    // FIXME: Handling of the cursor is missing here. It should be saved each time
    // a full block has been correctly processed/persisted. The saving location
    // is your responsibility.
    //
    // By making it persistent, we ensure that if we crash, on startup we are
    // going to read it back from database and start back our SubstreamsStream
    // with it ensuring we are continuously streaming without ever losing a single
    // element.
    Ok(())
}

fn load_persisted_cursor() -> Result<Option<String>, Error> {
    // FIXME: Handling of the cursor is missing here. It should be loaded from
    // somewhere (local file, database, cloud storage) and then `SubstreamStream` will
    // be able correctly resume from the right block.
    Ok(None)
}

async fn read_package(input: &str) -> Result<Package, Error> {
    if input.starts_with("http") {
        return read_http_package(input).await
    }

    // Assume it's a local file

    let content =
        std::fs::read(input).context(format_err!("read package from file '{}'", input))?;
    Package::decode(content.as_ref()).context("decode command")
}

async fn read_http_package(input: &str) -> Result<Package, Error> {
    let body = reqwest::get(input)
        .await?
        .bytes()
        .await?;

    Package::decode(body).context("decode command")
}

async fn create_stream(
    spkg_file: &str,
    cursor: Option<&str>,
    endpoint_url: &str,
    module_name: &str,
    start_block: i64,
    token: &str,
) -> Result<SubstreamsStream, Error> {
    let content =
        std::fs::read(spkg_file).context(format_err!("read package from file '{}'", spkg_file))?;
    let spkg = Package::decode(content.as_ref()).context("decode command")?;
    let endpoint = Arc::new(SubstreamsEndpoint::new(&endpoint_url, Some(token.to_owned())).await?);

    Ok(SubstreamsStream::new(
        endpoint,
        cursor.map(|s| s.to_owned()),
        spkg.modules.clone(),
        module_name.to_string(),
        start_block,
        0, // FIXME: remove magic value
    ))
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

        assert!(args.is_ok());
        let args = args.unwrap();
        let expected_args = Args {
            endpoint_url: "http://example.com".to_string(),
            substreams_api_token: "your_api_token".to_string(),
            package_file: "package.spkg".to_string(),
            module_name: "module_name".to_string(),
        };

        assert_eq!(args, expected_args);
    }

    #[tokio::test]
    async fn test_arg_parsing_long() {
        // Set the SUBSTREAMS_API_TOKEN environment variable for testing.
        env::set_var("SUBSTREAMS_API_TOKEN", "your_api_token");
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
        assert!(args.is_ok());
        let args = args.unwrap();
        let expected_args = Args {
            endpoint_url: "http://example.com".to_string(),
            substreams_api_token: "your_api_token".to_string(),
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
