use std::{fmt, time::Duration};

use anyhow::{anyhow, Result};
use reqwest::{Client, ClientBuilder, Response};

const USER_AGENT: &str = "propeller-tycho-ethereum";

/// An HTTP client factory.
///
/// This ensures a common configuration for all our HTTP clients used in various
/// places, while allowing for separate configurations, connection pools, and
/// cookie stores (for things like sessions and default headers) across
/// different APIs.
#[derive(Clone, Debug)]
pub struct HttpClientFactory {
    timeout: Duration,
}

impl HttpClientFactory {
    pub fn new(args: &Arguments) -> Self {
        Self { timeout: args.http_timeout }
    }

    /// Creates a new HTTP client with the default settings.
    pub fn create(&self) -> Client {
        self.builder().build().unwrap()
    }

    /// Creates a new HTTP client, allowing for additional configuration.
    pub fn configure(&self, config: impl FnOnce(ClientBuilder) -> ClientBuilder) -> Client {
        config(self.builder()).build().unwrap()
    }

    /// Returns a `ClientBuilder` with the default settings.
    pub fn builder(&self) -> ClientBuilder {
        ClientBuilder::new()
            .timeout(self.timeout)
            .user_agent(USER_AGENT)
    }
}

impl Default for HttpClientFactory {
    fn default() -> Self {
        Self { timeout: Duration::from_secs(10) }
    }
}

/// Command line arguments for the common HTTP factory.
#[derive(clap::Parser)]
#[group(skip)]
pub struct Arguments {
    /// Default timeout in seconds for http requests.
    #[clap(
        long,
        env,
        default_value = "10s",
        value_parser = humantime::parse_duration,
    )]
    pub http_timeout: Duration,
}

impl fmt::Display for Arguments {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let Self { http_timeout } = self;

        writeln!(f, "http_timeout: {:?}", http_timeout)
    }
}

/// Extracts the bytes of the response up to some size limit.
///
/// Returns an error if the byte limit was exceeded.
pub async fn response_body_with_size_limit(
    response: &mut Response,
    limit: usize,
) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        let slice: &[u8] = &chunk;
        if bytes.len() + slice.len() > limit {
            return Err(anyhow!("size limit exceeded"));
        }
        bytes.extend_from_slice(slice);
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore]
    async fn real() {
        let client = Client::default();

        let mut response = client
            .get("https://cow.fi")
            .send()
            .await
            .unwrap();
        let bytes = response_body_with_size_limit(&mut response, 10).await;
        dbg!(&bytes);
        assert!(bytes.is_err());

        let mut response = client
            .get("https://cow.fi")
            .send()
            .await
            .unwrap();
        let bytes = response_body_with_size_limit(&mut response, 1_000_000)
            .await
            .unwrap();
        dbg!(bytes.len());
        let text = std::str::from_utf8(&bytes).unwrap();
        dbg!(text);
    }
}
