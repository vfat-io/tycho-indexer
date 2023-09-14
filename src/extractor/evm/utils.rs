use ethers::types::{H160, U256};

pub fn u256_to_bytes(v: &U256) -> Vec<u8> {
    let mut bytes32 = [0u8; 32];
    v.to_big_endian(&mut bytes32);
    bytes32.to_vec()
}

/// Parses an evm address hash from a slice
pub fn parse_id_h160(v: &[u8]) -> Result<H160, String> {
    if v.len() != 20 {
        return Err(format!("H160: Invalid address format: 0x{}", hex::encode(v)))
    }
    Ok(H160::from_slice(v))
}

// Parse an address potentially zero left padding the value
pub fn pad_and_parse_h160(v: &[u8]) -> Result<H160, String> {
    if v.len() > 20 {
        return Err(format!(
            "H160: too long; expected 20, got {}, val: 0x{}",
            v.len(),
            hex::encode(v)
        ))
    }

    let mut data: [u8; 20] = [0; 20];
    let start_index = 20 - v.len();
    data[start_index..].copy_from_slice(v);

    Ok(H160::from(data))
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
            "Invalid byte length for U256 in slot key! Val: 0x{}",
            hex::encode(raw_key)
        ))
    }
    let v = if let Some(val) = raw_val {
        if val.len() != 32 {
            return Err(format!(
                "Invalid byte length for U256 in slot value! Val: 0x{}",
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

// Parse a 32 bytes value potentially zero left padding the value
pub fn pad_and_parse_32bytes<T>(v: &[u8]) -> Result<T, String>
where
    T: From<[u8; 32]>,
{
    if v.len() > 32 {
        return Err(format!(
            "Byte slice too long: Expected 32, got {}, val: 0x{}",
            v.len(),
            hex::encode(v)
        ))
    }
    let mut data: [u8; 32] = [0; 32];
    let start_index = 32 - v.len();
    data[start_index..].copy_from_slice(v);

    Ok(T::from(data))
}

#[cfg(test)]
mod test {
    use super::*;
    use ethers::types::H256;
    use rstest::rstest;

    #[rstest]
    #[case::zero(U256::zero(), [0;32].to_vec())]
    #[case::one(U256::one(), [0;31].into_iter().chain([1].into_iter()).collect())]
    #[case::number(U256::from(257), [0;30].into_iter().chain([1, 1].into_iter()).collect())]
    fn test_u256_bytes(#[case] n: U256, #[case] exp: Vec<u8>) {
        assert_eq!(u256_to_bytes(&n), exp);
    }

    #[rstest]
    #[case::ok([0;20].to_vec(), Ok(H160::zero()))]
    #[case::err([0;1].to_vec(), Err("H160: Invalid address format: 0x00".to_owned()))]
    fn test_parse_h160(#[case] inp: Vec<u8>, #[case] exp: Result<H160, String>) {
        assert_eq!(parse_id_h160(&inp), exp);
    }

    #[rstest]
    #[case::ok([0;20].to_vec(), Ok(H160::zero()))]
    #[case::pad([0;1].to_vec(), Ok(H160::zero()))]
    #[case::err([0;21].to_vec(), Err("H160: too long; expected 20, got 21, val: 0x000000000000000000000000000000000000000000".to_owned()))]
    fn test_pad_and_parse_h160(#[case] inp: Vec<u8>, #[case] exp: Result<H160, String>) {
        assert_eq!(pad_and_parse_h160(&inp), exp);
    }

    #[rstest]
    #[case::ok([0;32].to_vec(), Some([0;32].to_vec()), Ok((U256::from(0), U256::from(0))))]
    #[case::ok_none_val([0;32].to_vec(), None, Ok((U256::from(0), U256::from(0))))]
    #[case::key_bad([0;1].to_vec(), Some([0;32].to_vec()), Err("Invalid byte length for U256 in slot key! Val: 0x00".to_owned()))]
    #[case::val_bad([0;32].to_vec(), Some([0;1].to_vec()), Err("Invalid byte length for U256 in slot value! Val: 0x00".to_owned()))]
    fn test_parse_u256_slot_entry(
        #[case] key: Vec<u8>,
        #[case] val: Option<Vec<u8>>,
        #[case] exp: Result<(U256, U256), String>,
    ) {
        assert_eq!(parse_u256_slot_entry(&key, val.as_deref()), exp);
    }

    #[rstest]
    #[case::ok([0;32].to_vec(), Ok(H256::zero()))]
    #[case::pad([0;1].to_vec(), Ok(H256::zero()))]
    #[case::err([0;33].to_vec(), Err("Byte slice too long: Expected 32, got 33, val: 0x000000000000000000000000000000000000000000000000000000000000000000".to_owned()))]
    fn test_pad_and_parse_bytes32(#[case] inp: Vec<u8>, #[case] exp: Result<H256, String>) {
        assert_eq!(pad_and_parse_32bytes(&inp), exp);
    }
}
