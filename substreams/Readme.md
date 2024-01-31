# Subtreams packages

This directory contains all substream packages that are used by the extractors to access certain data from diffrent
blockchains.

## Adding a new package

To add a new package add folder. The naming convention is `[CHAIN]-[PROTOCOL_SYSTEM]`. In this new folder add a manifest
file `substreams.yaml`. You can use the template below to get started:

```yaml
specVersion: v0.1.0
package:
  name: 'substreams_[CHAIN]_[PROTOCOL_SYSTEM]'
  version: v0.1.0

protobuf:
  files:
    - vm.proto
    - common.proto
  importPaths:
    # This is different compared to the substreams example, 
    # we need to share protobuf definitions with tycho you 
    # are invited to reuse existing definitions if they are 
    # useful to you.
    - ../../proto/evm/v1
    # any private message types only used in internal modules 
    # can remain local to the crate.
    - ./proto

binaries:
  default:
    type: wasm/rust-v1
    # this points to the workspace target directory we use a special 
    # substreams build profile to optimise wasm binaries
    file: ../../target/wasm32-unknown-unknown/substreams/substreams_[CHAIN]_[PROTOCOL_SYSTEM].wasm

modules:
  # sample module provides access to blocks.
  - name: map_block
    kind: map
    inputs:
      - source: sf.ethereum.type.v2.Block
    output:
      type: proto:acme.block_meta.v1.BlockMeta
```

Substreams packages are Rust crates so we also need a `cargo.toml`.
The example from the official docs will serve us just well:

```toml
[package]
name = "substreams_[CHAIN]_[PROTOCOL_SYSTEM]"
version = "0.1.0"
edition = "2021"

[lib]
name = "substreams_[CHAIN]_[PROTOCOL_SYSTEM]"
crate-type = ["cdylib"]

[dependencies]
substreams = "0.5"
substreams-ethereum = "0.9"
prost = "0.11"

```

Now we can generate the Rust protobuf code:

```
substreams protogen substreams.yaml --exclude-paths="sf/substreams,google"
```

The command above should put the generate rust files under `/src/pb`. You
can start using these now in your module handlers: See
the [official substreams documentation](https://thegraph.com/docs/en/substreams/getting-started/quickstart/#create-substreams-module-handlers)
on
how to implement module handlers.

You can also look into already existing substreams packages to see how it
is done. E.g. [ethereum-ambient](./ethereum-ambient/) provides a pretty good
example of how to get access to raw contract storage.

# Run substreams locally

To run using Tycho, please follow the instructions in the [main Readme](../Readme.md#substreams).
To run using the substreams CLI, do
```
substreams run \
  substreams/ethereum-ambient/substreams-ethereum-ambient-v0.3.0.spkg \
  map_changes \
  -e mainnet.eth.streamingfast.io:443 \
  --start-block 17361664 \
  --stop-block +10
```
If you prefer you can also use the substreams gui with
```
substreams gui \
  substreams/ethereum-ambient/substreams-ethereum-ambient-v0.3.0.spkg \
  map_changes \
  -e mainnet.eth.streamingfast.io:443 \
  --start-block 17361664 \
  --stop-block +10
```

# Tests

To create a block test asset for ethereum do the following:

- Follow [this tutorial](https://substreams.streamingfast.io/tutorials/overview/map_block_meta_module). Make sure you
  set up the substreams-explorer repo in the same directory as this repo.
    - Comment out `image: ./ethereum.png` in `ethereum-explorer/substreams.yaml`
    - Add `prost-types = "0.11.0"` to `ethereum-explorer/Cargo.toml`
- Make sure you set up your key env vars.
- Run `sh scripts/download-ethereum-block-to-s3 BLOCK_NUMBER`

Do not commit the block files (they are quite big).

# Guide on how to make a balance store

When creating a new substreams module, you mind find yourself in the need of a way to store the token balances of pools.
This is how:
1. In the `substreams.yaml` you need to add something like:
```yaml
- name: store_pool_balances
  kind: store
  updatePolicy: add
  valueType: bigint
  initialBlock: 17361664
  inputs:
  - map: map_pool_changes
````
This adds a store with an input from the `map_pool_changes` module. 
The update policy is to add to the previous value of each key and the value type is a big integer.
The `initialBlock` is the block from which the store will start indexing (WARNING: it is important to set this value, otherwise it will try to index from the beginning of time).
2. Define the store module like (example from ambient):
```rust
#[substreams::handlers::store]
pub fn store_pool_balances(changes: BlockPoolChanges, balance_store: StoreAddBigInt) {
    let mut deltas = changes.balance_deltas.clone();
    deltas.sort_by_key(|delta| delta.ordinal); // the balances need to be ordered by ordinal !!
    for balance_delta in deltas {
        let pool_hash_hex = hex::encode(&balance_delta.pool_hash);
        balance_store.add(
            balance_delta.ordinal + 1,
            format!("{}{}", pool_hash_hex, "base"),
            BigInt::from_bytes_le(Sign::Plus, &balance_delta.base_token_delta),
        );
    }
}
```
(This snippet is adding only the balance of the base token)
3. Use the store in the next module, like:
```yaml
  - name: map_changes
    kind: map
    initialBlock: 17361664
    inputs:
      - source: sf.ethereum.type.v2.Block
      - map: map_pool_changes
      - store: store_pool_balances
        mode: deltas
```
Use `mode: deltas` if you want to get only values that changed in this block.
In `map_changes` add the store as an input like:
```rust
#[substreams::handlers::map]
fn map_changes(
    block: eth::v2::Block,
    balance_store: StoreGetBigInt,
) -> Result<BlockContractChanges, substreams::errors::Error> {
}
```
If you want to use `mode: deltas` you need to use `StoreDeltas` instead of `StoreGetBigInt`. 
Notice that by using `StoreGetBigInt` you can get the values by key, while using `StoreDeltas` you can only loop through the values of this block.