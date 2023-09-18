use serde::{
    de::{self, Deserializer},
    Deserialize, Serializer,
};

/// Serializes a Vec<u8> as a hex string with '0x' prefix.
pub fn serialize_hex<S>(bytes: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let hex_str = format!("0x{}", hex::encode(bytes));
    serializer.serialize_str(&hex_str)
}

/// Deserializes a hex string to a Vec<u8>. The hex string can either have a '0x' prefix or not.
pub fn deserialize_hex<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: &str = Deserialize::deserialize(deserializer)?;
    let hex_str = s.strip_prefix("0x").unwrap_or(s);
    hex::decode(hex_str).map_err(de::Error::custom)
}
