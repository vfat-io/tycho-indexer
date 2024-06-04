use crate::pb::tycho::evm::v1::BlockPoolChanges;
use substreams::{
    scalar::BigInt,
    store::{StoreAdd, StoreAddBigInt, StoreNew},
};

#[substreams::handlers::store]
pub fn store_pool_balances(changes: BlockPoolChanges, balance_store: StoreAddBigInt) {
    let deltas = changes.balance_deltas.clone();
    for balance_delta in deltas {
        let pool_hash_hex = hex::encode(&balance_delta.pool_hash);
        balance_store.add(
            balance_delta.ordinal,
            format!("{}:{}", pool_hash_hex, balance_delta.token_type),
            BigInt::from_signed_bytes_be(&balance_delta.token_delta),
        );
    }
}
