use crate::{
    extractor::{
        evm::{BlockContractChanges, BlockEntityChanges},
        u256_num::bytes_to_f64,
    },
    storage::postgres::{
        orm,
        schema::{component_balance, protocol_component, protocol_system},
    },
};
use diesel::prelude::*;
use diesel_async::{pooled_connection::deadpool::Pool, AsyncPgConnection, RunQueryDsl};
use ethers::types::U256;
use futures03::future::join;
use std::{collections::HashMap, time::Duration};
use tokio::time::Instant;
use tracing::{debug, error, info};
use tycho_types::Bytes;

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

pub async fn transcode_protocol_system_balances_db(
    pool: Pool<AsyncPgConnection>,
    system: &str,
    transcode_balance_f: fn(&Bytes) -> anyhow::Result<Bytes>,
) {
    let mut conn = pool
        .get()
        .await
        .expect("failed to get a connection");
    let count: i64 = component_balance::table
        .filter(component_balance::valid_to.is_null())
        .count()
        .first(&mut conn)
        .await
        .expect("retrieving table count failed");

    let start_time = Instant::now();
    let mut last_log_time = Instant::now();

    let page_size: i64 = 1000;
    let total_pages = (count as f64 / page_size as f64).ceil() as i64;
    let mut balance_updates = HashMap::new();

    for pages_processed in 0..total_pages {
        let offset = pages_processed * page_size;
        let res = component_balance::table
            .select(orm::ComponentBalance::as_select())
            .filter(component_balance::valid_to.is_null())
            .inner_join(protocol_component::table.inner_join(protocol_system::table))
            .filter(protocol_system::name.eq(system))
            .order(component_balance::id.desc())
            .offset(offset)
            .limit(page_size)
            .load::<orm::ComponentBalance>(&mut conn)
            .await
            .expect("retrieving chunk failed");

        for balance in res {
            // Need a fail safe against default balance zero:
            if balance.new_balance == Bytes::from(U256::from(0)) {
                continue;
            }
            if let Ok(corrected_balance) = transcode_balance_f(&balance.new_balance) {
                let balance_float = bytes_to_f64(corrected_balance.as_ref())
                    .expect("failed converting balance to float");
                let prev_balance = transcode_balance_f(&balance.previous_value)
                    .unwrap_or_else(|_| Bytes::from(U256::from(0)));
                balance_updates
                    .insert(balance.id, (corrected_balance, prev_balance, balance_float));
            } else {
                error!(balance=?&balance.new_balance, pc_id=?&balance.protocol_component_id, "Failed transcoding balances");
                continue
            }
        }
        if balance_updates.is_empty() {
            continue
        }
        orm::ComponentBalance::update_many(&balance_updates)
            .execute(&mut conn)
            .await
            .expect("Update failed");
        balance_updates.clear();

        debug!("Update batch sent");

        // Log progress approximately every 1 minute
        if last_log_time.elapsed() > Duration::from_secs(60) {
            let elapsed_secs = start_time.elapsed().as_secs_f64();
            let total_secs = (elapsed_secs / (pages_processed + 1) as f64) * total_pages as f64;
            let remaining_secs = total_secs - elapsed_secs;
            info!(
                pages_processed = pages_processed + 1,
                total_pages,
                system,
                time_remaining = format!("{:.2}m", remaining_secs / 60.0),
                name = "Progress"
            );
            last_log_time = Instant::now();
        }
    }
    info!(system, "Balance transcoding completed");
}

pub async fn transcode_balances_db(pool: Pool<AsyncPgConnection>) {
    let usv2_jh = tokio::spawn(transcode_protocol_system_balances_db(
        pool.clone(),
        "uniswap_v2",
        transcode_le_balance_to_be,
    ));
    let ambient_jh = tokio::spawn(transcode_protocol_system_balances_db(
        pool,
        "ambient",
        transcode_ascii_balance_to_be,
    ));

    let (l, r) = join(usv2_jh, ambient_jh).await;
    l.unwrap();
    r.unwrap();
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
