use crate::extractor::{
    evm::{BlockAccountChanges, BlockEntityChangesResult},
    u256_num::bytes_to_f64,
};
use ethers::types::U256;
use tycho_types::Bytes;

fn transcode_ascii_balance_to_be(ascii_encoded: &Bytes) -> anyhow::Result<Bytes> {
    let ascii_string = String::from_utf8(ascii_encoded.clone().to_vec())
        .map_err(|e| anyhow::format_err!("Invalid UTF-8 sequence: {ascii_encoded}: {e}"))?;
    let as_integer = U256::from_dec_str(&ascii_string)
        .map_err(|e| anyhow::format_err!("Invalid integer: {e}"))?;
    Ok(Bytes::from(as_integer))
}

fn transcode_le_balance_to_be(le_encoded: &Bytes) -> Bytes {
    let mut be_encoded = le_encoded.clone().to_vec();
    be_encoded.reverse(); // Reverse the vector to convert from little-endian to big-endian
    Bytes::from(be_encoded)
}

pub fn transcode_ambient_balances(mut changes: BlockAccountChanges) -> BlockAccountChanges {
    changes
        .component_balances
        .iter_mut()
        .for_each(|(_, balance)| {
            balance
                .iter_mut()
                .for_each(|(_, value)| {
                    value.balance = transcode_ascii_balance_to_be(&value.balance)
                        .expect("Balance transcoding failed");
                    value.balance_float = bytes_to_f64(value.balance.as_ref())
                        .expect("failed converting balance to float");
                });
        });
    changes
}

pub fn transcode_usv2_balances(mut changes: BlockEntityChangesResult) -> BlockEntityChangesResult {
    changes
        .component_balances
        .iter_mut()
        .for_each(|(_, balance)| {
            balance
                .iter_mut()
                .for_each(|(_, value)| {
                    value.balance = transcode_le_balance_to_be(&value.balance);
                    value.balance_float = bytes_to_f64(value.balance.as_ref())
                        .expect("failed converting balance to float");
                });
        });
    changes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trancode_ascii_balance_to_be() {
        let ascii_balance = Bytes::from("0x373131383430313238");

        let be_balance = transcode_ascii_balance_to_be(&ascii_balance).unwrap();

        assert_eq!(be_balance, Bytes::from(U256::from(711840128)));
    }

    #[test]
    fn test_transcode_le_balance_to_be() {
        let le_balance = Bytes::from("0xb55cb8eef8db0b5c");

        let be_balance = transcode_le_balance_to_be(&le_balance);

        assert_eq!(be_balance, Bytes::from("0x5c0bdbf8eeb85cb5"));
    }
}
