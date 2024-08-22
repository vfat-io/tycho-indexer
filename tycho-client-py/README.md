# Tycho Python Client

A Python client library for interacting with the Tycho RPC server and streaming data from the Tycho client.

## Features
- RPC Client: Interact with the Tycho RPC server to query various blockchain data, including protocol components, states, contract states, and tokens.
- Streaming: Stream real-time data using the Tycho client binary, allowing for efficient monitoring and processing of live data.

## Installation

If you want to build and install the package locally, follow these steps:

1. Build the package:

```bash
./build_wheel.sh
```

This script will generate the distribution files (wheels) in the dist directory.

2. Install the package locally:

```bash
pip install dist/tycho_client_py-*.whl
```

## Usage

### RPC Client

The TychoRPCClient class allows you to interact with the Tycho RPC server. You can query for protocol components, protocol states, contract states, and tokens.

#### Example
```python
from tycho_client_py import (
    TychoRPCClient,
    ProtocolComponentsParams,
    ProtocolStateParams,
    ContractStateParams,
    TokensParams,
    Chain
)

client = TychoRPCClient("http://0.0.0.0:4242", chain=Chain.ethereum)

# Query protocol components
protocol_components = client.get_protocol_components(
    ProtocolComponentsParams(protocol_system="test_protocol")
)

# Query protocol state
protocol_state = client.get_protocol_state(
    ProtocolStateParams(protocol_system="test_protocol")
)

# Query contract state
contract_state = client.get_contract_state(
    ContractStateParams(contract_ids=["contract_id_0"])
)

# Query tokens
tokens = client.get_tokens(TokensParams(min_quality=10, traded_n_days_ago=30))

```

### Streaming

The TychoStream class allows you to start the Tycho client binary and stream data asynchronously.

#### Example

```python
import asyncio
from tycho_client_py import Chain, TychoStream
from decimal import Decimal

async def main():
    stream = TychoStream(
        tycho_url="localhost:8888",
        exchanges=["uniswap_v2"],
        min_tvl=Decimal(100),
        blockchain=Chain.ethereum,
    )

    await stream.start()

    async for message in stream:
        print(message)

asyncio.run(main())
```

#### Configurations
- `rpc_url`: The URL of the Tycho RPC server. Defaults to http://0.0.0.0:4242.
- `chain`: The blockchain chain you are querying (e.g., Ethereum).
- `tycho_url`: The URL of the Tycho indexer server.
- `exchanges`: A list of exchanges to monitor in the stream.
- `min_tvl`: The minimum total value locked (TVL) for filtering data in the stream. Defaults to 10 ETH. Note: this is a hard limit. If a pool fluctuates in tvl close to this boundary the client will emit a message to add/remove that pool every time it crosses that boundary. To mitegate this please use the ranged tvl boundary decribed below.
- `min_tvl_range`: A tuple of (removal_threshold, addition_threshold). New components will be added to the data stream if their TVL exceeds addition_threshold and will be removed from the stream if it drops below removal_threshold. Defaults to None. If both min_tvl and min_tvl_range is given, prefernece is given to min_tvl.
- `include_state`: Whether to include protocol states in the stream. Defaults to True.
- `logs_directory`: Directory to store log files. If not specified, a default directory based on the OS is used.
- `tycho_client_path`: Path to the Tycho client binary. If not specified, the binary is searched for in the system's PATH.
