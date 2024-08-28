use ethers::types::H160;

use tycho_core::Bytes;

// Parse an address potentially zero left padding the value
pub fn pad_and_parse_h160(v: &Bytes) -> Result<H160, String> {
    if v.len() > 20 {
        return Err(format!(
            "H160: too long; expected 20, got {}, val: 0x{}",
            v.len(),
            hex::encode(v)
        ));
    }

    let mut data: [u8; 20] = [0; 20];
    let start_index = 20 - v.len();
    data[start_index..].copy_from_slice(v);

    Ok(H160::from(data))
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
        ));
    }
    let mut data: [u8; 32] = [0; 32];
    let start_index = 32 - v.len();
    data[start_index..].copy_from_slice(v);

    Ok(T::from(data))
}

// Parse a 20 bytes value potentially zero left padding the value
pub fn pad_and_parse_20bytes<T>(v: &[u8]) -> Result<T, String>
where
    T: From<[u8; 20]>,
{
    if v.len() > 20 {
        return Err(format!(
            "Byte slice too long: Expected 20, got {}, val: 0x{}",
            v.len(),
            hex::encode(v)
        ));
    }
    let mut data: [u8; 20] = [0; 20];
    let start_index = 20 - v.len();
    data[start_index..].copy_from_slice(v);

    Ok(T::from(data))
}

pub fn format_duration(duration: &chrono::Duration) -> String {
    let hours = duration.num_hours();
    let minutes = (duration.num_minutes()) % 60;
    format!("{:02}h{:02}m", hours, minutes)
}

#[cfg(test)]
mod test {
    use super::*;
    use ethers::types::H256;
    use rstest::rstest;

    #[rstest]
    #[case::ok([0;20].into(), Ok(H160::zero()))]
    #[case::pad([0;1].into(), Ok(H160::zero()))]
    #[case::err([0;21].into(), Err("H160: too long; expected 20, got 21, val: 0x000000000000000000000000000000000000000000".to_owned()))]
    fn test_pad_and_parse_h160(#[case] inp: Bytes, #[case] exp: Result<H160, String>) {
        assert_eq!(pad_and_parse_h160(&inp), exp);
    }

    #[rstest]
    #[case::ok([0;32].into(), Ok(H256::zero()))]
    #[case::pad([0;1].into(), Ok(H256::zero()))]
    #[case::err([0;33].into(), Err("Byte slice too long: Expected 32, got 33, val: 0x000000000000000000000000000000000000000000000000000000000000000000".to_owned()))]
    fn test_pad_and_parse_bytes32(#[case] inp: Bytes, #[case] exp: Result<H256, String>) {
        assert_eq!(pad_and_parse_32bytes(&inp), exp);
    }

    #[rstest]
    #[case::ok([0;20].into(), Ok(H160::zero()))]
    #[case::pad([0;1].into(), Ok(H160::zero()))]
    #[case::err([0;21].into(), Err("Byte slice too long: Expected 20, got 21, val: 0x000000000000000000000000000000000000000000".to_owned()))]
    fn test_pad_and_parse_bytes20(#[case] inp: Bytes, #[case] exp: Result<H160, String>) {
        assert_eq!(pad_and_parse_20bytes(&inp), exp);
    }
}
