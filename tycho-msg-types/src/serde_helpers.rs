use hex::FromHexError;

fn decode_hex_with_prefix(val: &str) -> Result<Vec<u8>, FromHexError> {
    let mut stripped: String =
        if let Some(stripped) = val.strip_prefix("0x") { stripped } else { val }.into();

    // Check if the length of the string is odd
    if stripped.len() % 2 != 0 {
        // If it's odd, prepend a zero
        stripped.insert(0, '0');
    }

    hex::decode(&stripped)
}

/// serde functions for handling bytes as hex strings, such as [bytes::Bytes]
pub mod hex_bytes {
    use serde::{Deserialize, Deserializer, Serializer};

    use super::decode_hex_with_prefix;

    /// Serialize a byte vec as a hex string with 0x prefix
    pub fn serialize<S, T>(x: T, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        T: AsRef<[u8]>,
    {
        s.serialize_str(&format!("0x{}", hex::encode(x.as_ref())))
    }

    /// Deserialize a hex string into a byte vec
    /// Accepts a hex string with optional 0x prefix
    pub fn deserialize<'de, T, D>(d: D) -> Result<T, D::Error>
    where
        D: Deserializer<'de>,
        T: From<Vec<u8>>,
    {
        let value = String::deserialize(d)?;
        decode_hex_with_prefix(&value)
            .map(Into::into)
            .map_err(|e| serde::de::Error::custom(e.to_string()))
    }
}

/// serde functions for handling Option of bytes
pub mod hex_bytes_option {
    use super::decode_hex_with_prefix;
    use serde::{Deserialize, Deserializer, Serializer};

    /// Serialize a byte vec as a Some hex string with 0x prefix
    pub fn serialize<S, T>(x: &Option<T>, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        T: AsRef<[u8]>,
    {
        if let Some(x) = x {
            s.serialize_str(&format!("0x{}", hex::encode(x.as_ref())))
        } else {
            s.serialize_none()
        }
    }

    /// Deserialize a hex string into a byte vec or None
    /// Accepts a hex string with optional 0x prefix
    pub fn deserialize<'de, T, D>(d: D) -> Result<Option<T>, D::Error>
    where
        D: Deserializer<'de>,
        T: From<Vec<u8>>,
    {
        let value: Option<String> = Option::deserialize(d)?;

        match value {
            Some(val) => decode_hex_with_prefix(&val)
                .map(Into::into)
                .map(Some)
                .map_err(|e| serde::de::Error::custom(e.to_string())),
            None => Ok(None),
        }
    }
}

pub mod hex_hashmap_key {
    use super::decode_hex_with_prefix;
    use std::collections::HashMap;

    use serde::{de, ser::SerializeMap, Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S, K, V>(x: &HashMap<K, V>, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        K: AsRef<[u8]>,
        V: Serialize,
    {
        let mut map = s.serialize_map(Some(x.len()))?;
        for (k, v) in x.into_iter() {
            map.serialize_entry(&format!("0x{}", hex::encode(k)), v)?;
        }
        map.end()
    }

    pub fn deserialize<'de, V, D>(d: D) -> Result<HashMap<Vec<u8>, V>, D::Error>
    where
        D: Deserializer<'de>,
        V: Deserialize<'de>,
    {
        let interim = HashMap::<String, V>::deserialize(d)?;

        interim
            .into_iter()
            .map(|(k, v)| {
                let k = decode_hex_with_prefix(&k).map_err(|e| de::Error::custom(e.to_string()))?;
                return Ok((k, v))
            })
            .collect::<Result<HashMap<_, _>, _>>()
    }
}

pub mod hex_hashmap_key_value {
    use std::collections::HashMap;

    use serde::{de, ser::SerializeMap, Deserialize, Deserializer, Serializer};

    use super::decode_hex_with_prefix;

    pub fn serialize<S>(x: &HashMap<Vec<u8>, Vec<u8>>, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = s.serialize_map(Some(x.len()))?;
        for (k, v) in x.into_iter() {
            map.serialize_entry(
                &format!("0x{}", hex::encode(k)),
                &format!("0x{}", hex::encode(v)),
            )?;
        }
        map.end()
    }

    pub fn deserialize<'de, D>(d: D) -> Result<HashMap<Vec<u8>, Vec<u8>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let interim = HashMap::<String, String>::deserialize(d)?;
        interim
            .into_iter()
            .map(|(k, v)| {
                let k = decode_hex_with_prefix(&k).map_err(|e| de::Error::custom(e.to_string()))?;
                let v = decode_hex_with_prefix(&v).map_err(|e| de::Error::custom(e.to_string()))?;
                return Ok((k, v))
            })
            .collect::<Result<HashMap<_, _>, _>>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use serde_json;

    #[derive(Debug, Serialize, Deserialize)]
    struct TestStruct {
        #[serde(with = "hex_bytes")]
        bytes: Vec<u8>,

        #[serde(with = "hex_bytes_option")]
        bytes_option: Option<Vec<u8>>,
    }

    #[test]
    fn hex_bytes_serialize_deserialize() {
        let test_struct = TestStruct { bytes: vec![0u8; 10], bytes_option: Some(vec![0u8; 10]) };

        // Serialize to JSON
        let serialized = serde_json::to_string(&test_struct).unwrap();
        assert_eq!(
            serialized,
            "{\"bytes\":\"0x00000000000000000000\",\"bytes_option\":\"0x00000000000000000000\"}"
        );

        // Deserialize from JSON
        let deserialized: TestStruct = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized.bytes, vec![0u8; 10]);
        assert_eq!(deserialized.bytes_option, Some(vec![0u8; 10]));
    }

    #[test]
    fn hex_bytes_option_none() {
        let test_struct = TestStruct { bytes: vec![0u8; 10], bytes_option: None };

        // Serialize to JSON
        let serialized = serde_json::to_string(&test_struct).unwrap();
        assert_eq!(serialized, "{\"bytes\":\"0x00000000000000000000\",\"bytes_option\":null}");

        // Deserialize from JSON
        let deserialized: TestStruct = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized.bytes, vec![0u8; 10]);
        assert_eq!(deserialized.bytes_option, None);
    }
}
