import json
import requests
import argparse
from web3 import Web3
import os

node_url = os.getenv("ETH_RPC_URL")
the_graph_key = os.getenv("THE_GRAPH_API_KEY")
web3 = Web3(Web3.HTTPProvider(node_url))

# The Graph API URL for Uniswap v3
GRAPH_URL = f'https://gateway.thegraph.com/api/{the_graph_key}/subgraphs/id/5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV'


def chunked_list(lst, n):
    """Splits the input list `lst` into chunks of size `n`."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]
        
def fetch_all_protocol_components():
    all_results = []
    uri = "http://0.0.0.0:4242/v1/protocol_components"
    page = 0
    page_size = 500

    while True:
        payload = {
            "chain": "ethereum",
            "pagination": {
                "page": page,
                "page_size": page_size
            },
            "protocol_system": "uniswap_v3"
        }

        res = requests.post(
            uri,
            json=payload,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json"
            }
        )

        data = res.json()
        results = data.get("protocol_components", [])
        all_results.extend(results)

        if len(results) < page_size:
            break

        page += 1

    return all_results

def fetch_tycho_data(block: int, component_ids: list[str], page_size: int = 100):
    all_results = []
    uri = "http://0.0.0.0:4242/v1/protocol_state"
    
    # Split component_ids into chunks of 'page_size'
    for component_chunk in chunked_list(component_ids, page_size):
        page = 0
        while True:
            payload = {
                "protocolIds": [
                    {
                        "chain": "ethereum",
                        "id": cid
                    } for cid in component_chunk
                ],
                "include_balances": True,
                "protocol_system": "uniswap_v3",
                "version": {
                    "block": {
                        "chain": "ethereum",
                        "number": block
                    }
                },
                "pagination": {
                    "page": page,
                    "page_size": page_size
                }
            }
            
            # Make the request
            res = requests.post(
                uri,
                json=payload,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json"
                }
            )
            
            # Parse the response
            data = res.json()
            all_results.extend(data.get("states", []))
            
            # Break if there are no more pages (page_size < 100 indicates the end)
            if len(data.get("states", [])) < page_size:
                break
            
            # Move to the next page
            page += 1
    
    return all_results

def fetch_pool_data(pool_id, block_number=None):
    query = """
    query ($poolId: ID!, $blockNumber: Int) {
      pool(id: $poolId, block: {number: $blockNumber}) {
        id
        liquidity
        tick
        sqrtPrice
        ticks {
          liquidityNet
          tickIdx
        }
      }
    }
    """
    variables = {"poolId": pool_id, "blockNumber": block_number}
    response = requests.post(GRAPH_URL, json={'query': query, 'variables': variables})
    if response.status_code == 200:
        return response.json()['data']['pool']
    else:
        print(f"Error fetching data for pool {pool_id}: {response.status_code}")
        return None
    
def hex_to_int(hex_str, signed=False):
    if hex_str.startswith("0x"):
        hex_str = hex_str[2:]
    # Convert hex string to bytes in little endian
    value_bytes_little = bytes.fromhex(hex_str)
    return int.from_bytes(value_bytes_little, byteorder='big', signed=signed)

def compare_pools(local_pool, fetched_pool, block_number):
    differences = {}
    local_attributes = local_pool['attributes']
    component_id = local_pool['component_id']

    # Compare simple fields
    (liquidity,sqrt_price_x96,tick) = get_values_from_node(component_id, block_number)
    simple_fields = {
        'liquidity': liquidity,
        'tick': tick,
        'sqrt_price_x96': sqrt_price_x96
    }
    for local_key, fetched_value in simple_fields.items():
        # Check if the fetched_key exists and is not None
        if local_key in local_attributes and local_attributes[local_key] is not None:
            local_value = hex_to_int(local_attributes[local_key]) if local_key != 'tick' else hex_to_int(local_attributes[local_key], True)
            if local_value != fetched_value:
                differences[local_key] = (local_value, fetched_value)
        else:
            differences[local_key] = (local_key, 'Key not found or value is None')

    # Compare ticks
    for local_key, local_value in local_attributes.items():
        if local_key.startswith('ticks/'):
            tick_idx = int(local_key.split('/')[1].split('/')[0])
            liquidity_net = hex_to_int(local_value, True)
            # Find corresponding tick in fetched data
            fetched_tick = next((tick for tick in fetched_pool['ticks'] if int(tick['tickIdx']) == tick_idx), None)
            if fetched_tick is not None and int(fetched_tick['liquidityNet']) != liquidity_net:
                differences[local_key] = (liquidity_net, int(fetched_tick['liquidityNet']))

    for tick in fetched_pool['ticks']:
        tick_idx = tick['tickIdx']
        liquidity_net = tick['liquidityNet']

        if liquidity_net != '0':
            tycho_tick_key = f'ticks/{tick_idx}/net-liquidity'
            
            if tycho_tick_key not in local_attributes:
                differences[tycho_tick_key]=(tick_idx, "Not 0 in TheGraph and missing in Tycho")
    # Compare balances
    token0_address = list(local_pool["balances"].keys())[0]
    token1_address = list(local_pool["balances"].keys())[1]
    balance0 = get_token_balance(token0_address, component_id, block_number)
    balance1 = get_token_balance(token1_address, component_id, block_number)

    local_balance0 = hex_to_int(local_pool["balances"][token0_address])
    local_balance1 = hex_to_int(local_pool["balances"][token1_address])
    
    if local_balance0 != balance0:
        differences["balance0"] = (local_balance0, balance0)
    if local_balance1 != balance1:
        differences["balance1"] = (local_balance1, balance1)

    return differences

def main():
    parser = argparse.ArgumentParser(description='Compare uniswap v3 protocol state.')
    # Add arguments
    parser.add_argument('block_number', type=int, help='The block state to query')
    parser.add_argument('pools', nargs='*',default=[],help='A list of component ids (pool addresses)')
    # Parse arguments
    args = parser.parse_args()
    # Use the parsed arguments
    block_number = args.block_number
    pool_addresses = args.pools
    
    if len(pool_addresses) == 0:
        pool_addresses = [pc["id"] for pc in fetch_all_protocol_components()]

    parsed_data = fetch_tycho_data(block_number, pool_addresses)
    for pool in parsed_data:
        pool_id = pool['component_id']
        fetched_pool_data = fetch_pool_data(pool_id, block_number)
        if fetched_pool_data is not None:
            differences = compare_pools(pool, fetched_pool_data, block_number)
            if differences:
                print(f"Differences found for pool {pool_id}: \n")
                for key, (local_val, fetched_val) in differences.items():
                    print(f"  {key}: local={local_val}, fetched={fetched_val}")
            else:
                print(f"No differences found for pool {pool_id}.")
        else:
            print(f"Failed to fetch data for pool {pool_id}.")

def get_values_from_node(pool: str, block: int):
    transaction = {
        'to': web3.toChecksumAddress(pool),
        'from': '0x0000000000000000000000000000000000000000',
        'data': '0x1a686502'
    }

    result = web3.eth.call(transaction, block_identifier=block)
    
    liquidity = int(result.hex(), 16)
    
    transaction["data"] = '0x3850c7bd'

    result = web3.eth.call(transaction, block_identifier=block)

    sqrtPriceX96 = int(result.hex()[2:66], 16)

    tick = int(result.hex()[66:130][-6:], 16) 

    if tick >= 2**23:
        tick -= 2**24
    

    return (liquidity,sqrtPriceX96,tick)

# ABI for ERC20 balanceOf method
erc20_abi = [
    {
        "constant": True,
        "inputs": [{"internalType": "address", "name": "owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "payable": False,
        "stateMutability": "view",
        "type": "function",
    }
]

def get_token_balance(token_address, pool_address, block_number):
    """Query the token balance of a pool at a specific block."""
    token_contract = web3.eth.contract(
        address=Web3.toChecksumAddress(token_address), abi=erc20_abi
    )
    return token_contract.functions.balanceOf(Web3.toChecksumAddress(pool_address)).call(
        block_identifier=block_number
    )


if __name__ == "__main__":
    main()