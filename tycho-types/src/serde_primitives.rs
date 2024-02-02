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

/// serde functions for handling Vec of bytes
pub mod hex_bytes_vec {
    use super::decode_hex_with_prefix;
    use serde::{Deserialize, Deserializer, Serializer};

    /// Serialize a byte vec as a vector of hex strings with 0x prefix
    pub fn serialize<S, T>(x: &[T], s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        T: AsRef<[u8]>,
    {
        let vec_str: Vec<String> = x
            .iter()
            .map(|val| format!("0x{}", hex::encode(val.as_ref())))
            .collect();
        s.collect_seq(vec_str)
    }

    /// Deserialize a vector of hex strings into a vector of byte vecs
    /// Each string is a hex string with optional 0x prefix
    pub fn deserialize<'de, T, D>(d: D) -> Result<Vec<T>, D::Error>
    where
        D: Deserializer<'de>,
        T: From<Vec<u8>>,
    {
        let values: Vec<String> = Vec::deserialize(d)?;

        let mut output = Vec::new();
        for value in values {
            let decoded_value = decode_hex_with_prefix(&value)
                .map(Into::into)
                .map_err(|e| serde::de::Error::custom(e.to_string()))?;
            output.push(decoded_value);
        }
        Ok(output)
    }
}

/// serde functions for handling HashMap with a bytes key
pub mod hex_hashmap_key {
    use crate::Bytes;

    use super::decode_hex_with_prefix;
    use std::collections::HashMap;

    use serde::{de, ser::SerializeMap, Deserialize, Deserializer, Serialize, Serializer};

    #[allow(clippy::mutable_key_type)]
    pub fn serialize<S, V>(x: &HashMap<Bytes, V>, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        V: Serialize,
    {
        let mut map = s.serialize_map(Some(x.len()))?;
        for (k, v) in x.iter() {
            map.serialize_entry(&format!("{k:#x}"), v)?;
        }
        map.end()
    }

    pub fn deserialize<'de, V, D>(d: D) -> Result<HashMap<Bytes, V>, D::Error>
    where
        D: Deserializer<'de>,
        V: Deserialize<'de>,
    {
        let interim = HashMap::<String, V>::deserialize(d)?;

        interim
            .into_iter()
            .map(|(k, v)| {
                let k = decode_hex_with_prefix(&k).map_err(|e| de::Error::custom(e.to_string()))?;
                Ok((Bytes::from(k), v))
            })
            .collect::<Result<HashMap<_, _>, _>>()
    }
}

/// serde functions for handling HashMap with a bytes key and value
pub mod hex_hashmap_key_value {
    use std::collections::HashMap;

    use serde::{de, ser::SerializeMap, Deserialize, Deserializer, Serializer};

    use crate::Bytes;

    use super::decode_hex_with_prefix;

    #[allow(clippy::mutable_key_type)]
    pub fn serialize<S>(x: &HashMap<Bytes, Bytes>, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = s.serialize_map(Some(x.len()))?;
        for (k, v) in x.iter() {
            map.serialize_entry(&format!("{k:#x}"), &format!("{v:#x}"))?;
        }
        map.end()
    }

    pub fn deserialize<'de, D>(d: D) -> Result<HashMap<Bytes, Bytes>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let interim = HashMap::<String, String>::deserialize(d)?;
        interim
            .into_iter()
            .map(|(k, v)| {
                let k = decode_hex_with_prefix(&k).map_err(|e| de::Error::custom(e.to_string()))?;
                let v = decode_hex_with_prefix(&v).map_err(|e| de::Error::custom(e.to_string()))?;
                Ok((k.into(), v.into()))
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

        #[serde(with = "hex_bytes_vec")]
        bytes_vec: Vec<Vec<u8>>,
    }

    #[test]
    fn hex_bytes_serialize_deserialize() {
        let test_struct = TestStruct {
            bytes: vec![0u8; 10],
            bytes_option: Some(vec![0u8; 10]),
            bytes_vec: vec![vec![1u8; 10], vec![2u8; 10]],
        };

        // Serialize to JSON
        let serialized = serde_json::to_string(&test_struct).unwrap();
        assert_eq!(
            serialized,
            "{\"bytes\":\"0x00000000000000000000\",\"bytes_option\":\"0x00000000000000000000\",\"bytes_vec\":[\"0x01010101010101010101\",\"0x02020202020202020202\"]}"
        );

        // Deserialize from JSON
        let deserialized: TestStruct = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized.bytes, vec![0u8; 10]);
        assert_eq!(deserialized.bytes_option, Some(vec![0u8; 10]));
        assert_eq!(deserialized.bytes_vec, vec![vec![1u8; 10], vec![2u8; 10]]);
    }

    #[test]
    fn hex_bytes_option_none() {
        let test_struct =
            TestStruct { bytes: vec![0u8; 10], bytes_option: None, bytes_vec: vec![] };

        // Serialize to JSON
        let serialized = serde_json::to_string(&test_struct).unwrap();
        assert_eq!(
            serialized,
            "{\"bytes\":\"0x00000000000000000000\",\"bytes_option\":null,\"bytes_vec\":[]}"
        );

        // Deserialize from JSON
        let deserialized: TestStruct = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized.bytes, vec![0u8; 10]);
        assert_eq!(deserialized.bytes_option, None);
    }
}
