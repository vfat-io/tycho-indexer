# Tycho Indexer

![Asteroid Belt](./assets/belt.webp)


Low latency indexer for onchain and offchain financial protocols, Tycho allows clients to connect to a stream of delta messages that describe state changes of the underlying financial protocols. It also provides an RPC to request historical state at any given tick, timestamp, or block.

Clients do not have to be aware of forks. In case the underlying chain experiences a fork, Tycho will automatically resolve the fork and emit events. These events allow the clients to "revert" (through usual updates) to the correct state as quickly as possible.

Here's how a typical connection with Tycho works:

- Clients connect to Tycho's real-time delta messages and start buffering them.
- Next, they will request a historical state.
- Once the historical state has been fully retrieved, the buffered changes are applied on top of that state.
- Now the client possess the latest state of all protocols it is interested in.
- New incoming messages can now be applied directly to the existing state.

With Tycho, clients stay updated with the latest state of **all** protocols they are interested in. If they can handle all, they can get all.

## Supported implementations

Tycho supports two major types of protocol implementation strategies:

- **Custom Protocol implementations:** A custom struct is defined that represents the onchain state. Typically, this struct is more closely aligned with the state layout within smart contract languages, such as Solidity. State transitions have to be handled by processing events or method calls.
- **Virtual Machine (VM) implementations:** In this strategy, state representation aligns more closely with the underlying VM. For instance, we extract and update the storage and other attributes associated with a smart contract. This state is then passed to a VM instance for further functionality.

For both strategies, Tycho ensures seamless execution and integration with the overall system.


### Note
Each state allows annotation of TVL (Total Value Locked) and Inertias. These annotations help clients to filter down protocol components that are of particular interest to them.

This means users can easily focus on the aspects of the protocol components relevant to their needs, enhancing usability and efficiency.


## Protocol Systems and Components

Tycho extracts state for whole protocol systems including dynamically created protocol components. This means that components like Uniswap pairs, which were not predetermined, are included in the indexing logic. They are automatically added and indexed upon their creation/detection.


## Message types
To achieve the previously mentioned functionalities, Tycho uses the following message types:

- **EntityChange:** This message type is emitted for protocols that use a custom implementation. It describes how to change the attributes of an entity.
- **ContractChange:** This message type is emitted for protocols that use a VM (Virtual Machine) implementation. It contains changes to a contract's code, balances, and storage.
- **ComponentChange:** This message type is emitted whenever a new protocol component is added or removed. This allows clients to update their state as required.


# Architecture

The logical diagram below illustrates the architecture of the system. Each component will be discussed individually:

[![Logical Architecture Diagram](./assets/logical.drawio.png)](https://drive.google.com/file/d/1mhbARX2ipAh-YUDfm4gPN3Is4sLvyJxM/view?usp=sharing)

## Substreams

For on-chain data sources, Tycho utilizes [substreams](https://thegraph.com/docs/en/substreams/README/). With substreams, information can be quickly retrieved from (historical) blocks using Rust handlers that execute in parallel over a specified block range. Events are then emitted in a linear fashion. Once a substream has caught up with the current state of the underlying chain, it switches into a low latency mode, continuing to emit the same events for each new block.

Substream handlers, which extract the actual information for each block are implemented in Rust, compiled to WebAssembly and sent to a Firehose node where execution takes place near the data. Handlers emit messages that are streamed back to the client.

## Extractors

Extractors are tasked with archiving states and providing normalized change streams: They receive fork-aware messages from substreams and must apply these to the current state. If necessary, they create a corresponding delta message and then immediately forward it to any subscriptions.

As stateful components, extractors manage protocol components, protocol state, its history, and archive messages they have emitted. In addition, they need to remember where they stopped processing the substream. This is typically achieved by saving a cursor. When passed to a substream, the cursor instructs the stream to continue to emit events from precisely the point where it left off, guaranteeing consistent state.

### Note
Tycho runs each extractor in a separate thread. Therefore, it's anticipated that multiple extractors may run concurrently within a single process. To avoid increasing system latency, extractors should not engage in heavy processing if it can be avoided.


## Service
Last but not least, the service manages real-time subscriptions to extractors and handles requests from RPC clients for historical data.

In future iterations, the service might be enhanced with the capability to stream historical events. This feature would enable complex backtesting use cases.

## DB 
Tycho is an indexer designed to process and store data, necessitating the saving of state. Currently, it supports Postgres as a storage backend. Below is the Entity Relationship (ER) diagram illustrating the tables planned for this project:

[![Entity Relation Diagram](./assets/er.drawio.png)](https://drive.google.com/file/d/1mhbARX2ipAh-YUDfm4gPN3Is4sLvyJxM/view?usp=sharing)

### Note
This ER diagram includes only the most significant attributes for each entity. Triggers support much of the versioning logic. For precise details, please ensure to look at the actual [create.sql](./migrations_/create.sql) file.



