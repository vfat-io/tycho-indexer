use crate::extractor::{
    evm::{BlockContractChanges, BlockEntityChanges},
    u256_num::bytes_to_f64,
};
use ethers::types::U256;
use tycho_core::Bytes;

fn transcode_ascii_balance_to_be(ascii_encoded: &Bytes) -> anyhow::Result<Bytes> {
    let ascii_string = String::from_utf8(ascii_encoded.clone().to_vec())
        .map_err(|e| anyhow::format_err!("Invalid UTF-8 sequence: {ascii_encoded}: {e}"))?;
    // Balances can go negative, so if the ascii string starts with a -, we default to
    // U256::zero(), see WBTC/USDC pool at block 19297943
    if ascii_string.starts_with('-') {
        Ok(Bytes::from(U256::zero()))
    } else {
        let as_integer = U256::from_dec_str(&ascii_string)
            .map_err(|e| anyhow::format_err!("Invalid integer: {e}"))?;
        Ok(Bytes::from(as_integer))
    }
}

fn transcode_le_balance_to_be(le_encoded: &Bytes) -> anyhow::Result<Bytes> {
    let mut be_encoded = le_encoded.clone().to_vec();
    be_encoded.reverse(); // Reverse the vector to convert from little-endian to big-endian
    Ok(Bytes::from(be_encoded))
}

pub fn transcode_ambient_balances(mut changes: BlockContractChanges) -> BlockContractChanges {
    changes
        .tx_updates
        .iter_mut()
        .for_each(|tx_changes| {
            tx_changes
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
        });
    changes
}

pub fn transcode_usv2_balances(mut changes: BlockEntityChanges) -> BlockEntityChanges {
    changes
        .txs_with_update
        .iter_mut()
        .for_each(|tx_changes| {
            tx_changes
                .balance_changes
                .iter_mut()
                .for_each(|(_, balance)| {
                    balance
                        .iter_mut()
                        .for_each(|(_, value)| {
                            value.balance = transcode_le_balance_to_be(&value.balance).unwrap();
                            value.balance_float = bytes_to_f64(value.balance.as_ref())
                                .expect("failed converting balance to float");
                        });
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

        let be_balance = transcode_le_balance_to_be(&le_balance).unwrap();

        assert_eq!(be_balance, Bytes::from("0x5c0bdbf8eeb85cb5"));
    }
}
