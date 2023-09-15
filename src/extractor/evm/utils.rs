use ethers::types::{H160, U256};

pub fn u256_to_bytes(v: &U256) -> Vec<u8> {
    let mut bytes32 = [0u8; 32];
    v.to_big_endian(&mut bytes32);
    bytes32.to_vec()
}

/// Parses an evm address hash from the db
///
/// The db id is required to provide additional error context in case the
/// parsing fails.
pub fn parse_id_h160(v: &[u8]) -> Result<H160, String> {
    if v.len() != 20 {
        return Err(format!("Invalid contract address found: {}", hex::encode(v)))
    }
    Ok(H160::from_slice(v))
}

/// Parses a tuple of U256 representing an slot entry
///
/// In case the value is None it will assume a value of zero.
pub fn parse_u256_slot_entry(
    raw_key: &[u8],
    raw_val: Option<&[u8]>,
) -> Result<(U256, U256), String> {
    if raw_key.len() != 32 {
        return Err(format!(
            "Invalid byte length for U256 in slot key! Found: 0x{}",
            hex::encode(raw_key)
        ))
    }
    let v = if let Some(val) = raw_val {
        if val.len() != 32 {
            return Err(format!(
                "Invalid byte length for U256 in slot value! Found: 0x{}",
                hex::encode(val)
            ))
        }
        U256::from_big_endian(val)
    } else {
        U256::zero()
    };

    let k = U256::from_big_endian(raw_key);
    Ok((k, v))
}
