use async_trait::async_trait;
use chrono::{Local, NaiveDateTime};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

use tycho_core::{
    models::{protocol::ProtocolComponent, token::CurrencyToken, Chain, ComponentId},
    storage::{ProtocolGateway, StorageError},
    Bytes,
};

#[async_trait]
pub trait ProtocolDataCache: Send + Sync {
    async fn get_token_prices<'a>(
        &'a self,
        addresses: &'a [Bytes],
    ) -> Result<Vec<Option<f64>>, StorageError>;

    async fn get_tokens<'a>(
        &'a self,
        addresses: &'a [Bytes],
    ) -> Result<Vec<Option<CurrencyToken>>, StorageError>;

    async fn add_tokens(
        &self,
        tokens: impl IntoIterator<Item = CurrencyToken> + Send + Sync,
    ) -> Result<(), StorageError>;

    async fn get_protocol_components<'a>(
        &'a self,
        system: &'a str,
        component_ids: &'a [ComponentId],
    ) -> Result<HashMap<ComponentId, ProtocolComponent>, StorageError>;

    async fn add_components(
        &self,
        components: impl IntoIterator<Item = ProtocolComponent> + Send + Sync,
    ) -> Result<(), StorageError>;
}

type ProtocolComponentStore = HashMap<String, HashMap<ComponentId, ProtocolComponent>>;

#[derive(Clone)]
pub struct ProtocolMemoryCache {
    chain: Chain,
    tokens: Arc<RwLock<HashMap<Bytes, CurrencyToken>>>,
    token_prices: Arc<RwLock<TokenPrices>>,
    components: Arc<RwLock<ProtocolComponentStore>>,
    max_price_age: chrono::Duration,
    gateway: Arc<dyn ProtocolGateway + Send + Sync>,
}

#[derive(Default)]
struct TokenPrices {
    prices: HashMap<Bytes, f64>,
    last_price_update: NaiveDateTime,
}

impl ProtocolMemoryCache {
    pub fn new(
        chain: Chain,
        max_price_age: chrono::Duration,
        gateway: Arc<dyn ProtocolGateway + Send + Sync>,
    ) -> Self {
        Self {
            chain,
            max_price_age,
            gateway,
            tokens: Arc::new(RwLock::new(HashMap::new())),
            components: Arc::new(RwLock::new(HashMap::new())),
            token_prices: Arc::new(RwLock::new(TokenPrices::default())),
        }
    }

    pub async fn load_all_tokens(&self) -> Result<(), StorageError> {
        let mut cached_tokens = self.tokens.write().await;
        self.gateway
            .get_tokens(self.chain, None, None, None, None)
            .await?
            .into_iter()
            .for_each(|t| {
                cached_tokens.insert(t.address.clone(), t);
            });
        Ok(())
    }
}

#[async_trait]
impl ProtocolDataCache for ProtocolMemoryCache {
    async fn get_token_prices<'a>(
        &'a self,
        addresses: &'a [Bytes],
    ) -> Result<Vec<Option<f64>>, StorageError> {
        let last_update = self
            .token_prices
            .read()
            .await
            .last_price_update;

        let now = Local::now().naive_utc();
        if now.signed_duration_since(last_update) > self.max_price_age {
            let mut token_prices = self.token_prices.write().await;
            token_prices.prices = self
                .gateway
                .get_token_prices(&self.chain)
                .await?;
            token_prices.last_price_update = now;
        }
        let mut res = Vec::with_capacity(addresses.len());
        let inner = self.token_prices.read().await;
        for addr in addresses.iter() {
            res.push(inner.prices.get(addr).copied());
        }
        Ok(res)
    }

    async fn get_tokens<'a>(
        &'a self,
        addresses: &'a [Bytes],
    ) -> Result<Vec<Option<CurrencyToken>>, StorageError> {
        let missing = {
            let cached_tokens = self.tokens.read().await;
            addresses
                .iter()
                .filter(|&a| !cached_tokens.contains_key(a))
                .collect::<Vec<_>>()
        };
        if !missing.is_empty() {
            let mut cached_tokens = self.tokens.write().await;
            self.gateway
                .get_tokens(self.chain, Some(&missing), None, None, None)
                .await?
                .into_iter()
                .for_each(|t| {
                    cached_tokens.insert(t.address.clone(), t);
                });
        }
        let cached_tokens = self.tokens.read().await;
        Ok(addresses
            .iter()
            .map(|addr| cached_tokens.get(addr).cloned())
            .collect::<Vec<_>>())
    }

    async fn add_tokens(
        &self,
        tokens: impl IntoIterator<Item = CurrencyToken> + Send + Sync,
    ) -> Result<(), StorageError> {
        let mut guard = self.tokens.write().await;
        guard.extend(
            tokens
                .into_iter()
                .map(|t| (t.address.clone(), t)),
        );
        Ok(())
    }

    async fn get_protocol_components<'a>(
        &'a self,
        system: &'a str,
        component_ids: &'a [ComponentId],
    ) -> Result<HashMap<ComponentId, ProtocolComponent>, StorageError> {
        let empty = HashMap::new();
        let missing = {
            let guard = self.components.read().await;
            let cached_components = guard.get(system).unwrap_or(&empty);
            component_ids
                .iter()
                .filter(|&id| !cached_components.contains_key(id))
                .collect::<Vec<_>>()
        };
        if !missing.is_empty() {
            let mut guard = self.components.write().await;
            let cached_components = guard
                .entry(system.to_string())
                .or_default();
            self.gateway
                .get_protocol_components(
                    &self.chain,
                    Some(system.to_string()),
                    Some(
                        &missing
                            .iter()
                            .map(|id| id.as_str())
                            .collect::<Vec<_>>(),
                    ),
                    None,
                )
                .await?
                .into_iter()
                .for_each(|c| {
                    cached_components.insert(c.id.clone(), c);
                });
        }
        let guard = self.components.read().await;
        let cached_components = guard.get(system).unwrap_or(&empty);
        Ok(component_ids
            .iter()
            .filter_map(|id| cached_components.get(id).cloned())
            .map(|c| (c.id.clone(), c))
            .collect())
    }

    async fn add_components(
        &self,
        components: impl IntoIterator<Item = ProtocolComponent> + Send + Sync,
    ) -> Result<(), StorageError> {
        let mut guard = self.components.write().await;
        components.into_iter().for_each(|pc| {
            let components = guard
                .entry(pc.protocol_system.clone())
                .or_default();
            components.insert(pc.id.clone(), pc);
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::MockGateway;
    use chrono::Duration;
    use mockall::predicate::*;
    use tycho_core::models::ChangeType;

    #[tokio::test]
    async fn test_get_token_prices() {
        let chain = Chain::Ethereum;
        let max_price_age = Duration::seconds(60);
        let mut gateway = MockGateway::new();

        let token_prices = HashMap::from([(Bytes::from("0x01"), 1.0), (Bytes::from("0x02"), 2.0)]);
        gateway
            .expect_get_token_prices()
            .with(eq(chain))
            .times(1)
            .return_once(move |_| Box::pin(async move { Ok(token_prices.clone()) }));

        let cache = ProtocolMemoryCache::new(chain, max_price_age, Arc::new(gateway));

        let addresses = &[Bytes::from("0x01"), Bytes::from("0x02")];
        let prices = cache
            .get_token_prices(addresses)
            .await
            .unwrap();

        assert_eq!(prices, vec![Some(1.0), Some(2.0)]);
    }

    #[tokio::test]
    async fn test_get_tokens() {
        let chain = Chain::Ethereum;
        let max_price_age = Duration::seconds(60);
        let mut gateway = MockGateway::new();

        let tokens = tokens();
        let ret_tokens = tokens.clone();
        gateway
            .expect_get_tokens()
            .return_once(|_, _, _, _, _| Box::pin(async move { Ok(ret_tokens) }));
        let cache = ProtocolMemoryCache::new(chain, max_price_age, Arc::new(gateway));

        let addresses = tokens
            .iter()
            .map(|t| t.address.clone())
            .collect::<Vec<_>>();
        let cached_tokens = cache
            .get_tokens(&addresses)
            .await
            .unwrap();

        assert_eq!(cached_tokens.len(), 2);
        assert_eq!(
            cached_tokens[0]
                .as_ref()
                .unwrap()
                .address,
            Bytes::from("0x01")
        );
        assert_eq!(
            cached_tokens[1]
                .as_ref()
                .unwrap()
                .address,
            Bytes::from("0x02")
        );
    }

    fn tokens() -> Vec<CurrencyToken> {
        vec![
            CurrencyToken::new(&Bytes::from("0x01"), "T1", 18, 0, &[None], Chain::Ethereum, 100),
            CurrencyToken::new(&Bytes::from("0x02"), "T2", 18, 0, &[None], Chain::Ethereum, 100),
        ]
    }

    #[tokio::test]
    async fn test_get_protocol_components() {
        let chain = Chain::Ethereum;
        let max_price_age = Duration::seconds(60);
        let mut gateway = MockGateway::new();

        let components = vec![
            ProtocolComponent::new(
                "component1",
                "sys1",
                "pool1",
                Chain::Ethereum,
                Vec::new(),
                Vec::new(),
                HashMap::new(),
                ChangeType::Creation,
                Bytes::default(),
                NaiveDateTime::default(),
            ),
            ProtocolComponent::new(
                "component2",
                "sys1",
                "pool1",
                Chain::Ethereum,
                Vec::new(),
                Vec::new(),
                HashMap::new(),
                ChangeType::Creation,
                Bytes::default(),
                NaiveDateTime::default(),
            ),
        ];
        let ret_components = components.clone();
        gateway
            .expect_get_protocol_components()
            .return_once(move |_, _, _, _| Box::pin(async { Ok(ret_components) }));

        let cache = ProtocolMemoryCache::new(chain, max_price_age, Arc::new(gateway));
        let component_ids = components
            .iter()
            .map(|c| c.id.clone())
            .collect::<Vec<_>>();
        let cached_components = cache
            .get_protocol_components("sys1", &component_ids)
            .await
            .unwrap();

        assert_eq!(cached_components.len(), 2);
        assert!(cached_components.contains_key("component1"));
        assert!(cached_components.contains_key("component2"));
    }

    #[tokio::test]
    async fn test_load_all_tokens() {
        let chain = Chain::Ethereum;
        let max_price_age = Duration::seconds(60);
        let mut gateway = MockGateway::new();

        let tokens = tokens();
        let ret_tokens = tokens.clone();

        gateway
            .expect_get_tokens()
            .return_once(move |_, _, _, _, _| Box::pin(async { Ok(ret_tokens) }));

        let cache = ProtocolMemoryCache::new(chain, max_price_age, Arc::new(gateway));

        cache.load_all_tokens().await.unwrap();

        let cached_tokens = cache.tokens.read().await;
        assert_eq!(cached_tokens.len(), 2);
        assert!(cached_tokens.contains_key(&Bytes::from("0x01")));
        assert!(cached_tokens.contains_key(&Bytes::from("0x02")));
    }
}
