use ethers::prelude::{H160, U256};
use futures03::{future::try_join_all, FutureExt};
use std::{collections::HashMap, str::FromStr, sync::Arc, time::Instant};
use token_analyzer::{trace_call::TraceCallDetector, BadTokenDetecting, TokenFinder, TokenQuality};
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};
use tycho_core::{
    models::{token::CurrencyToken, Chain, PaginationParams},
    storage::ProtocolGateway,
};
use web3::types::BlockNumber;

#[derive(Debug, Clone, Default)]
pub struct AnalyzeTokenArgs {
    rpc_url: String,
    chain: Chain,
    concurrency: usize,
    update_batch_size: usize,
    fetch_batch_size: usize,
}

pub async fn analyze_tokens(
    args: AnalyzeTokenArgs,
    gw: Arc<dyn ProtocolGateway + Send + Sync>,
) -> anyhow::Result<()> {
    let mut tokens = Vec::new();
    let mut page = 0;
    loop {
        let start = Instant::now();
        let pagination_params = PaginationParams::new(page, args.fetch_batch_size as i64);
        tokens.clone_from(
            &(gw.get_tokens(args.chain, None, None, Some(&pagination_params))
                .await?),
        );
        let sem = Arc::new(Semaphore::new(args.concurrency));
        let tasks = tokens
            .chunks(args.update_batch_size)
            .into_iter()
            .map(|chunk| {
                analyze_batch(
                    args.chain,
                    args.rpc_url.clone(),
                    chunk.to_vec(),
                    sem.clone(),
                    gw.clone(),
                )
                .boxed()
            })
            .collect::<Vec<_>>();

        _ = try_join_all(tasks).await?;
        let duration = Instant::now().duration_since(start);
        info!(processed = tokens.len(), page = page, duration = duration.as_secs(), "Progress");

        page += 1;
        if tokens.len() < 10000 {
            break;
        }
    }

    Ok(())
}

async fn analyze_batch(
    chain: Chain,
    eth_rpc_url: String,
    mut tokens: Vec<CurrencyToken>,
    sem: Arc<Semaphore>,
    gw: Arc<dyn ProtocolGateway + Send + Sync>,
) -> anyhow::Result<()> {
    let _guard = sem.acquire().await?;
    let addresses = tokens
        .iter()
        .map(|t| t.address.clone())
        .collect::<Vec<_>>();

    let component = gw
        .get_protocol_components_by_tokens(&chain, Some(&addresses), Some(100_000f64))
        .await?;

    let liquidity_token_owners = component
        .into_iter()
        .filter_map(|pc| {
            let pool = if !pc.contract_addresses.is_empty() {
                Some(H160::from_slice(&pc.contract_addresses[0]))
            } else {
                H160::from_str(&pc.id).ok()
            };

            if let Some(pool) = pool {
                let entries = pc
                    .tokens
                    .clone()
                    .into_iter()
                    .map(move |t| (H160::from_slice(&t), (pool, U256::from(100_000))));
                Some(entries)
            } else {
                None
            }
        })
        .flatten()
        .collect::<HashMap<_, _>>();
    let tf = Arc::new(TokenFinder::new(liquidity_token_owners));
    let analyzer = TraceCallDetector::new(eth_rpc_url.as_str(), tf);
    for t in tokens.iter_mut() {
        if t.quality <= 5 {
            continue;
        }

        let address = H160::from_slice(&t.address);
        let (token_quality, gas, tax) = match analyzer
            .detect(address, BlockNumber::Latest)
            .await
        {
            Ok(t) => t,
            Err(error) => {
                warn!(?error, "Token quality detection failed");
                continue;
            }
        };

        if let TokenQuality::Bad { reason } = token_quality {
            debug!(?address, ?reason, "Token quality detected as bad!");
            // Don't try to analyze again.
            t.quality = 5;
        };

        // If it's a fee token, set quality to 50
        if tax.map_or(false, |tax_value| tax_value > U256::zero()) {
            t.quality = 50;
        }

        t.tax = tax.unwrap_or(U256::zero()).as_u64();
        t.gas = gas.map_or_else(Vec::new, |g| vec![Some(g.as_u64())]);
    }

    gw.update_tokens(&tokens).await?;

    Ok(())
}

#[cfg(test)]
mod test {
    use crate::{
        extractor::evm::token_analysis_cron::{analyze_tokens, AnalyzeTokenArgs},
        testing,
    };
    use chrono::NaiveDateTime;
    use std::{collections::HashMap, sync::Arc};
    use tycho_core::{
        models::{protocol::ProtocolComponent, token::CurrencyToken, Chain, ChangeType},
        Bytes,
    };

    // requires a running ethereum node
    #[ignore]
    #[test_log::test(tokio::test)]
    async fn test_analyze_tokens() {
        let args = AnalyzeTokenArgs {
            rpc_url:
                "https://ethereum-mainnet.core.chainstack.com/71bdd37d35f18d55fed5cc5d138a8fac"
                    .to_string(),
            chain: Chain::Ethereum,
            concurrency: 10,
            update_batch_size: 100,
            fetch_batch_size: 100,
        };
        let mut gw = testing::MockGateway::new();
        gw.expect_get_tokens()
            .returning(|_, _, _, _| {
                Box::pin(async {
                    Ok(vec![CurrencyToken::new(
                        &Bytes::from("0x45804880de22913dafe09f4980848ece6ecbaf78"),
                        "PAXG",
                        18,
                        0,
                        &[],
                        Chain::Ethereum,
                        10,
                    )])
                })
            });
        let exp = vec![CurrencyToken::new(
            &Bytes::from("0x45804880de22913dafe09f4980848ece6ecbaf78"),
            "PAXG",
            18,
            2,
            &[Some(59_264)],
            Chain::Ethereum,
            50,
        )];
        gw.expect_get_protocol_components_by_tokens()
            .returning(|_, _, _| {
                Box::pin(async move {
                    Ok(vec![ProtocolComponent::new(
                        "0xe25a329d385f77df5d4ed56265babe2b99a5436e",
                        "uniswap_v2",
                        "pool",
                        Chain::Ethereum,
                        vec![Bytes::from("0x45804880de22913dafe09f4980848ece6ecbaf78")],
                        vec![],
                        HashMap::new(),
                        ChangeType::Creation,
                        Bytes::from("0x00"),
                        NaiveDateTime::default(),
                    )])
                })
            });
        gw.expect_update_tokens()
            .once()
            .returning(move |updated| {
                assert_eq!(updated, &exp);
                Box::pin(async { Ok(()) })
            });

        analyze_tokens(args, Arc::new(gw))
            .await
            .expect("analyze tokens failed");
    }
}
