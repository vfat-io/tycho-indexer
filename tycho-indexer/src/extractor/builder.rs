use std::sync::Arc;

use serde::Deserialize;
use tycho_core::models::{Chain, FinancialType, ImplementationType, ProtocolType};
use tycho_storage::postgres::cache::CachedGateway;

use super::{
    evm::{
        chain_state::ChainState,
        native::{NativeContractExtractor, NativePgGateway},
        token_pre_processor::TokenPreProcessor,
        vm::{VmContractExtractor, VmPgGateway},
    },
    ExtractionError, Extractor,
};

#[derive(Debug, Deserialize)]
struct ProtocolTypeConfig {
    name: String,
    financial_type: FinancialType,
}

#[derive(Debug, Deserialize)]
pub struct ExtractorConfig {
    name: String,
    chain: Chain,
    implementation_type: ImplementationType,
    sync_batch_size: usize,
    start_block: i64,
    protocol_types: Vec<ProtocolTypeConfig>,
    spkg: String,
    module_name: String,
}

impl ExtractorConfig {
    pub fn spkg(&self) -> String {
        self.spkg.clone()
    }

    pub fn module_name(&self) -> String {
        self.module_name.clone()
    }

    pub fn start_block(&self) -> i64 {
        self.start_block
    }
}
pub struct ExtractorBuilder {
    token_pre_processor: TokenPreProcessor,
    cached_gw: CachedGateway,
    chain_state: ChainState,
}

impl ExtractorBuilder {
    pub fn new(
        token_pre_processor: TokenPreProcessor,
        cached_gw: CachedGateway,
        chain_state: ChainState,
    ) -> Self {
        Self { token_pre_processor, cached_gw, chain_state }
    }

    pub async fn build(
        &self,
        config: ExtractorConfig,
    ) -> Result<Arc<dyn Extractor>, ExtractionError> {
        let protocol_types = config
            .protocol_types
            .into_iter()
            .map(|pt| {
                (
                    pt.name.clone(),
                    ProtocolType::new(
                        pt.name,
                        pt.financial_type,
                        None,
                        config.implementation_type.clone(),
                    ),
                )
            })
            .collect();

        match config.implementation_type {
            ImplementationType::Vm => {
                let gw = VmPgGateway::new(
                    &config.name,
                    config.chain,
                    config.sync_batch_size,
                    self.cached_gw.clone(),
                    self.token_pre_processor.clone(),
                );

                Ok(Arc::new(
                    VmContractExtractor::new(
                        &config.name,
                        config.chain,
                        self.chain_state,
                        gw,
                        protocol_types,
                        config.name.clone(),
                        None,
                    )
                    .await?,
                ))
            }
            ImplementationType::Custom => {
                let gw = NativePgGateway::new(
                    &config.name,
                    config.chain,
                    config.sync_batch_size,
                    self.cached_gw.clone(),
                    self.token_pre_processor.clone(),
                );

                Ok(Arc::new(
                    NativeContractExtractor::new(
                        &config.name,
                        config.chain,
                        self.chain_state,
                        gw,
                        protocol_types,
                        config.name.clone(),
                        None,
                    )
                    .await?,
                ))
            }
        }
    }
}
