#![doc = include_str!("../../Readme.md")]

use ethrpc::{http::HttpTransport, Web3, Web3Transport};
use futures03::future::select_all;
use reqwest::Client;
use serde::Deserialize;
use std::{fs::File, io::Read, str::FromStr, sync::Arc};
use url::Url;

use extractor::{
    evm::token_pre_processor::TokenPreProcessor,
    runner::{ExtractorBuilder, ExtractorHandle},
};

use actix_web::dev::ServerHandle;
use clap::Parser;
use ethers::{
    prelude::{Http, Provider},
    providers::Middleware,
};
use tokio::{select, task::JoinHandle};
use tracing::info;

use tycho_core::models::Chain;
use tycho_indexer::{
    cli::{AnalyzeTokenArgs, Cli, Command, GlobalArgs, IndexArgs},
    extractor::{
        self,
        evm::{chain_state::ChainState, token_analysis_cron::analyze_tokens},
        runner::{ExtractorConfig, HandleResult},
        ExtractionError,
    },
    services::ServicesBuilder,
};
use tycho_storage::postgres::{builder::GatewayBuilder, cache::CachedGateway};

// TODO: We need to use `use pretty_assertions::{assert_eq, assert_ne}` per test module.
#[allow(unused_imports)]
#[cfg(test)]
#[macro_use]
extern crate pretty_assertions;

#[derive(Debug, Deserialize)]
struct ExtractorConfigs {
    extractors: std::collections::HashMap<String, ExtractorConfig>,
}

impl ExtractorConfigs {
    fn from_yaml(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let mut file = File::open(path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let config: ExtractorConfigs = serde_yaml::from_str(&contents)?;
        Ok(config)
    }
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    // Set up the subscriber
    let console_flag = std::env::var("ENABLE_CONSOLE").unwrap_or_else(|_| "false".to_string());
    if console_flag == "true" {
        console_subscriber::init();
    } else {
        tracing_subscriber::fmt::init();
    }

    let cli: Cli = Cli::parse();
    let global_args = cli.args();

    match cli.command() {
        Command::Run(_) => {
            todo!();
        }
        Command::Index(indexer_args) => {
            run_indexer(global_args, indexer_args).await?;
        }
        Command::AnalyzeTokens(analyze_args) => {
            run_token_analyzer(global_args, analyze_args).await?;
        }
    }
    Ok(())
}

async fn run_indexer(
    global_args: GlobalArgs,
    index_args: IndexArgs,
) -> Result<(), ExtractionError> {
    info!("Starting Tycho");
    let rpc_client: Provider<Http> =
        Provider::<Http>::try_from(&global_args.rpc_url).expect("Error creating HTTP provider");
    let block_number = rpc_client
        .get_block_number()
        .await
        .expect("Error getting block number")
        .as_u64();

    let chain_state = ChainState::new(chrono::Local::now().naive_utc(), block_number);
    let extractors_config = ExtractorConfigs::from_yaml(&index_args.extractors_config)
        .map_err(|e| ExtractionError::Setup(format!("Failed to load extractors.yaml. {}", e)))?;

    let protocol_systems: Vec<String> = extractors_config
        .extractors
        .keys()
        .cloned()
        .collect();

    let (cached_gw, gw_writer_thread) = GatewayBuilder::new(&global_args.database_url)
        .set_chains(&[Chain::Ethereum])
        .set_protocol_systems(&protocol_systems)
        .build()
        .await?;

    let transport = Web3Transport::new(HttpTransport::new(
        Client::new(),
        Url::from_str(
            "https://ethereum-mainnet.core.chainstack.com/71bdd37d35f18d55fed5cc5d138a8fac",
        )
        .unwrap(),
        "transport".to_owned(),
    ));
    let w3 = Web3::new(transport);

    let token_processor = TokenPreProcessor::new(rpc_client, w3);

    let (mut tasks, extractor_handles): (Vec<_>, Vec<_>) =
        // TODO: accept substreams configuration from cli.
        build_all_extractors(&extractors_config, chain_state, &cached_gw, &token_processor)
            .await
            .map_err(|e| ExtractionError::Setup(format!("Failed to create extractors: {}", e)))?
            .into_iter()
            .unzip();

    // TODO: add configurations to cli for these values
    let server_addr = "0.0.0.0";
    let server_port = 4242;
    let server_version_prefix = "v1";
    let server_url = format!("http://{}:{}", server_addr, server_port);
    let (server_handle, server_task) = ServicesBuilder::new(cached_gw.clone())
        .prefix(server_version_prefix)
        .bind(server_addr)
        .port(server_port)
        .register_extractors(extractor_handles.clone())
        .run()?;
    info!(server_url, "Http and Ws server started");

    let shutdown_task = tokio::spawn(shutdown_handler(
        server_handle,
        extractor_handles
            .into_iter()
            .map(|h| h.0)
            .collect::<Vec<_>>(),
        gw_writer_thread,
    ));

    tasks.extend(vec![server_task, shutdown_task]);

    let (res, _, _) = select_all(tasks).await;
    res.expect("Extractor- nor ServiceTasks should panic!")
}

async fn build_all_extractors(
    config: &ExtractorConfigs,
    chain_state: ChainState,
    cached_gw: &CachedGateway,
    token_pre_processor: &TokenPreProcessor,
) -> Result<Vec<HandleResult>, ExtractionError> {
    let mut extractor_handles = Vec::new();

    for extractor_config in config.extractors.values() {
        let (task, handle) = ExtractorBuilder::new(extractor_config)
            .build(chain_state, cached_gw, token_pre_processor)
            .await?
            .run()
            .await?;

        info!("Extractor {} started!", handle.0.get_id());
        extractor_handles.push((task, handle));
    }

    Ok(extractor_handles)
}

async fn shutdown_handler(
    server_handle: ServerHandle,
    extractors: Vec<ExtractorHandle>,
    db_write_executor_handle: JoinHandle<()>,
) -> Result<(), ExtractionError> {
    // listen for ctrl-c
    tokio::signal::ctrl_c().await.unwrap();
    for e in extractors.iter() {
        e.stop().await.unwrap();
    }
    server_handle.stop(true).await;
    db_write_executor_handle.abort();
    Ok(())
}

async fn run_token_analyzer(
    global_args: GlobalArgs,
    analyzer_args: AnalyzeTokenArgs,
) -> Result<(), anyhow::Error> {
    let (cached_gw, gw_writer_thread) = GatewayBuilder::new(&global_args.database_url)
        .set_chains(&[analyzer_args.chain])
        .build()
        .await?;
    let cached_gw = Arc::new(cached_gw);
    let analyze_thread = analyze_tokens(analyzer_args, &global_args.rpc_url, cached_gw.clone());
    select! {
         res = analyze_thread => {
            res?;
         },
         res = gw_writer_thread => {
            res?;
        }
    }
    Ok(())
}
