# Tycho Indexer

![Asteroid Belt](./assets/belt.webp)


Low latency indexer for onchain and offchain financial protocols. Tycho allows clients to connect to a stream of delta messages that describe state changes of the underlying financial protocols. It provides an RPC to request historical state at any given tick, timestamp or block. 

Clients do not have to be aware of forks. In case the underlying chain experiences a fork, tycho will automatically resolve the fork and emit events that allow the clients to "revert" (through usual updates) to the correct state as quickly as possible.

Typically clients will connect to tycho, realtime delta messages and start buffering those. Next they will request a historical state. Once the historicaly state has been fully retrieved, the buffered changes are applied to that state. Now the client posseses the latest state of all protocols it is interested in. New incoming message can now be applied directly to the this state.

## Supported implementations

Tycho supports two major types of protocol implementation strategies:

- **Custom protocol implementations:** A custom struct is defined that represents the onchain state. Usually this struct is closer to the state layout within e.g. solidity. State transitions have to be handled by processing events or method calls.
- **VM implementations:** Here state is represented closer to the underlying vm. As in we simply extract and update the storage and other attribtues that belong to a smart contract. This state is then passed to a vm instance for further fucntionality.


### Note
Each state allows annotation of tvl and inertias. This helps clients filter down protocol components that are of intereset to them.


## Protocol Systems and Components

Tycho extracts state for protocol systems, not predetermined components. This means that dynamically created protocol components (e.g. uniswap pairs) are part of the indexing logic and are automatically added and indexed upon creation/detection.


## Message types
To achieve the previously mentioned funtionalities, tycho uses the following message types:

- EntityChange: This message is emitted for protocols that use a custom implementation and describes hot to change the attributes of an entity.
- ContractChange: This message is emitted for protocols that use a VM implementation and contais changes to contracts code, balances and storage.
- ComponentChange: This message is emitted whenever a new protocol component is added or removed and allows clients to update their state if they wish to do so.


# Architecture

The below logical diagram shows the architecture of the system. We will discuss each component indiviudally:

[![Logical Architecture Diagram](./assets/logical.drawio.png)](https://drive.google.com/file/d/1mhbARX2ipAh-YUDfm4gPN3Is4sLvyJxM/view?usp=sharing)

## Substreams

For onchain data sources, tycho makes have use of [substreams](https://thegraph.com/docs/en/substreams/README/). With substreams information can be quickly retrieved from blocks, using rust handlers that will be executed in parallel over a specified block range. Events are then emitted in a linear fashion. Once a substream has cought up with the current state of the underlying chain it switches into a low latency mode and keeps emitting the same events for each now block.

Substream handlers, which extract the actual information for each block are implemented in Rust, compiled to web assembly and sent to a firehose node where they are executed close to the data. Only produced messages are streamed back to the substreams client.


## Extractors

Extractors are responsible to archive states and provide normalised change streams. They receive fork aware messages from substreams, then have to apply those to the current state, create a corresponding delta message if necessary and then immediately forward the message to any subscriptions.

Extractors are a stateful component, they have to manage: protocol components, protocol state, its history and archive messages they emitted. Apart from that, they also need to remember where they stopped processing the substream. The latter is usually done by saving a cursor. The cursor when passed to a substreams stream will continue to emit events from exactly the same point where we left off.

### Note
Tycho will run each extractor in a separate thread. So it is expected that there will be potentially many extractors running in a single process. Extractors should therefore not do any heavy processing if it can be avoided, as this increase the latency of the whole system.


## Service
Last but not least the service manages realtime subscriptions to extractors as well as requests from RPC clients on historical data. 

In the future the service might also provide the ability to stream historical events to enable complex backtesting use cases.


## DB 
As tycho is an indexer concerned with processing and storing data. It needs to save state somewhere. It currently supports Postgres as a storage backend. Below you can find an ER diagram for the currently planned tables:

[![Logical Architecture Diagram](./assets/logical.drawio.png)](https://drive.google.com/file/d/1mhbARX2ipAh-YUDfm4gPN3Is4sLvyJxM/view?usp=sharing)

### Note
Only the most important attributes have been adeded to the entities here. A lot of the versioning is supported by triggers. For the exact details please make sure to have a look at the actual [create.sql](./migrations_/create.sql).



