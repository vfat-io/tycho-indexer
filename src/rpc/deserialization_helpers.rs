use crate::models::Chain;
use serde::de::{self, Deserializer};
use serde::Deserialize;

// This will convert a hex string (with or without 0x) to a Vec<u8>
pub fn hex_to_bytes<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    let hex_str = s.strip_prefix("0x").unwrap_or(&s);
    hex::decode(hex_str).map_err(de::Error::custom)
}

// This will convert a chain name as a string (with or without 0x) to Chain element
pub fn chain_from_str<'de, D>(deserializer: D) -> Result<Chain, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;

    Chain::try_from(s).map_err(de::Error::custom)
}
