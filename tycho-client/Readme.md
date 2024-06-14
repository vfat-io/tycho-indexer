# Tycho Client

The following document describes the tycho client library / binary. This is the main
consumer facing component of Tycho.

The guide below focuses on the CLI application but all functionality mentioned below is
also available as a Rust library.

## Install

Use the script for convenience, it takes optionally a version argument. If not given
the latest version is used (ignoring pre releases). The script will download the
correct binary, unpack it and move it under your path.

```bash
./get-tycho.sh [VERSION]
```

Alternatively download the correct binary from the latest release on GitHub:

https://github.com/propeller-heads/tycho-indexer/releases

Once you have downloaded the binary you can unpack it and move it under your path:

```bash
# unpack tar
tar -xvzf tycho-client-aarch64-apple-darwin-{VERSION}.tar.gz
# circumvent quarantine (only macOS users)
xattr -d com.apple.quarantine tycho-client || true 
# move into you PATH
mv tycho-client /usr/local/bin/
```

The client should now be available from your command line:

```bash
$ tycho-client -V
tycho-client 0.1.0
```

## Quickstart

To use the client, you will need a connection to the indexer, we usually
port-forward to our cluster's instance for this. If you are planning to use a local
instance e.g. running within docker-compose you can skip this step.

```
kubectl port-forward -n dev-tycho deploy/tycho-indexer 4242
```

Once you have a connection to an indexer instance, you can simply create a stream using:

```
tycho-client \
    --exchange uniswap_v2 \
    --exchange uniswap_v3 \
    --exchange vm:ambient \
    --exchange vm:balancer \
    --min-tvl 100 
```

## Usage

The main use case of the tycho client is to provide a stream of protocol components,
snapshots, their state changes and associated tokens.

If you choose to stream from multiple extractors, the client will try to align the
messages by their block, if it fails to do so the client will simply exit with an error
message.

You can use the `--block-time` parameter to fine tune this behaviour. This is the
maximum time we will wait for another extractor before emitting a message. If any other
extractor has not replied within this time it is considered as delayed.

*We do currently not provide support to stream from different chains.*

### State tracking

Tycho-client provides automatic state tracking. Also known as snapshot and deltas model.

The client will first query tycho-indexer for all components that match the filter
criteria. You can request individual pools, or use a minimum tvl threshold per
extractor.

E.g. to track a single pool use:

```bash
tycho-client --exchange uniswap_v3-0x....
```

This will stream all relevant messages for this particular uniswap v3 pool.

If you wish to track all pools with a minimum tvl (denominated in native token):

```bash
tycho-client --min-tvl 100 --exchange uniswap_v3 --exchange uniswap_v2
```

This will stream state updates for all uniswap v3 and v2 pools that have a tvl >= 100 ETH.

If you choose min tvl tracking, tycho-client will automatically add snapshots for any
components that start passing the tvl threshold, e.g. because more liquidity was
provided.

The delta messages will also include any new tokens that the client consumer has not
seen yet as well as a map of components that should be removed because tycho-client
stopped tracking them (because they went below the required tvl threshold).

### Message types

For each block, the tycho-client will emit a FeedMessage. Each message is emitted as a
single JSON line to stdout.

#### FeedMessage

The main outer message type. It contains both the individual SyncState (one per
extractor) and metadata about the extractors block synchronisation state. The latter
allows consumers to handle delayed extractors gracefully. Each extractor is supposed
to emit one message per block even if no changes happened in that block.

[Link to structs](https://github.com/propeller-heads/tycho-indexer/blob/main/tycho-client/src/feed/mod.rs#L305)

#### StateSyncMessage

Next we'll look into the StateSyncMessage struct. This struct, as the name states,
serves to synchronize the state of any consumer to be up-to-date with the blockchain.

The main attributes of this struct are snapshots and deltas.

For any components that have NOT been observed yet by the client a snapshot is emitted.
A snapshot contains the entire state at the header as well as the static protocol
component definition (tokens, ids, involved contract addresses, etc.).

Deltas contain state updates, observed after or at the snapshot. Any components
mentioned in the snapshots and in deltas within the same StateSynchronization message,
must have the deltas applied to their snapshot to arrive at a correct state for the
current header.

The removed component attribute is map of components that should be removed by
consumers. Any components mentioned here will not appear in any further
messages/updates.

[Link to structs](https://github.com/propeller-heads/tycho-indexer/blob/main/tycho-client/src/feed/mod.rs#L105)

#### Snapshots

Snapshots are simple messages, they contain the complete state of a component along
with the component itself. Tycho differentiates between component and component state.

The component itself is static: it describes, for example, which tokens are involved or
how much fees are charged (if this value is static).

Dynamic state such as reserves, balances, etc. are kept under component state. This
state is dynamic and can change at any block.

[Link to structs](https://github.com/propeller-heads/tycho-indexer/blob/main/tycho-client/src/feed/synchronizer.rs#L63)

#### Deltas

Deltas contain only targeted changes to the component state. They are designed to be
lightweight and always contain absolute new values. They will never contain delta values
so clients have an easy time updating their internal state.

State updates include both a protocol state key value mapping, with keys being strings
and values being bytes, as well as a contract storage key value mapping for each
involved contract address. Here both keys and values are bytes. Exact byte encoding
might differ depending on the protocol, but as a general guideline integers are
big-endian encoded.

Asides from state updates, Deltas also always include the following few special
attributes:

- `new_components`: Components that were created on this block. Must not necessarily pass
  the tvl filter to appear here.
- `new_tokens`: Token metadata of all newly created components.
- `balances`: Balances changes are emitted for every tracked protocol component.
- `component_tvl`: If there was a balance change in a tracked component, the new tvl for
  the component is emitted.
- `deleted_protocol_components`: Any components mentioned here have been removed from
  the protocol and are not available anymore.

[Link to structs](https://github.com/propeller-heads/tycho-indexer/blob/main/tycho-core/src/dto.rs#L136)

## Debugging

Since all messages are sent directly to stdout in a single line, logs are saved to a
file_ `./logs/dev_logs.log`. You can configure the directory with `--log-dir` option.

To tail the log in real time you can use the `tail` command:

```bash
tail -f ./logs/dev_logs.log
```

To modify the verbosity of logs please use the `RUST_LOG` env variable:

```bash
RUST_LOG=tycho_client=trace tycho-client ...
```

To get a pretty printed representation of all messages emitted by tycho-client you can
stream the messages into a formatter tool such as `jq`:

```bash
tycho-client --exchange uniswap_v3 ... | jq
```

For debugging, it is often useful to stream everything into files, both logs and
messages, then use your own tools to browse those files.

```bash
RUST_LOG=tycho_client=debug tycho-client ... --log-dir /logs/run01.log > messages.jsonl &
# To view the logs
tail -f logs/run01.log
# To view latest emitted messages pretty printed
tail -f -n0 message.jsonl | jq 
# To view and browse the 3rd message pretty printed
sed '3q;d' message.jsonl | jq | less
```

To only stream a preset amount of messages you can use the `-n` flag. E.g. to create a
single snapshot for test fixtures.

```bash
tycho-client -n 1 --exchange uniswap_v3:0x.... > debug_usv3.json
```

If you wish to create an integration test, you can also stream multiple messages into a
[jsonl](https://jsonlines.org/) file:

```bash
tycho-client -n 100 --exchange uniswap_v3:0x....  > integration.jsonl
# or with compression
tycho-client -n 1000 --exchange uniswap_v3:0x....  | gzip -c - > 1kblocks.jsonl.gz
```

This file can then be used as mock input to an integration test or a benchmark script.

### Historical state

Tycho also provides access to historical snapshots. Unfortunately we do not have this
exposed on the tycho-client yet. If you need to retrieve the historical state of a
component you will have to use the RPC for this.

Tycho exposes an openapi docs for its RPC endpoints. You can find them under:

http://localhost:4242/docs/

## Light mode

For use cases that do not require snapshots or state updates, we provide a light mode.
In this mode tycho will not emit any snapshots or state updates, it will only emit
newly created component, associated tokens, tvl changes, balance changes and removed
components. This mode can be turned on via the `--no-state` flag.
