pub mod ethrpc;
pub mod http_client;
pub mod trace_call;
pub mod trace_many;

use anyhow::Result;
use ethcontract::U256;
use primitive_types::H160;

/// How well behaved a token is.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum TokenQuality {
    Good,
    Bad { reason: String },
}

impl TokenQuality {
    pub fn is_good(&self) -> bool {
        matches!(self, Self::Good { .. })
    }

    pub fn bad(reason: impl ToString) -> Self {
        Self::Bad { reason: reason.to_string() }
    }
}

/// Detect how well behaved a token is.
#[mockall::automock]
#[async_trait::async_trait]
pub trait BadTokenDetecting: Send + Sync {
    async fn detect(&self, token: H160) -> Result<(TokenQuality, Option<U256>, Option<U256>)>;
}
