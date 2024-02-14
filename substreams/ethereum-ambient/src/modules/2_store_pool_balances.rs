use crate::pb::tycho::evm::v1::BlockPoolChanges;
use substreams::{
    scalar::BigInt,
    store::{StoreAdd, StoreAddBigInt, StoreNew},
};

#[substreams::handlers::store]
pub fn store_pool_balances(changes: BlockPoolChanges, balance_store: StoreAddBigInt) {
    for component in changes.protocol_components {
        let pool_hash_hex = hex::encode(&component.id);
        balance_store.add(0, format!("{}{}", pool_hash_hex, "base"), BigInt::from(0));
        balance_store.add(0, format!("{}{}", pool_hash_hex, "quote"), BigInt::from(0));
    }
    let mut deltas = changes.balance_deltas.clone();
    deltas.sort_by_key(|delta| delta.ordinal);
    for balance_delta in deltas {
        let pool_hash_hex = hex::encode(&balance_delta.pool_hash);
        balance_store.add(
            balance_delta.ordinal + 1,
            format!("{}{}", pool_hash_hex, "base"),
            BigInt::from_unsigned_bytes_be(&balance_delta.base_token_delta),
        );
        balance_store.add(
            balance_delta.ordinal + 1,
            format!("{}{}", pool_hash_hex, "quote"),
            BigInt::from_unsigned_bytes_be(&balance_delta.quote_token_delta),
        );
    }
}
