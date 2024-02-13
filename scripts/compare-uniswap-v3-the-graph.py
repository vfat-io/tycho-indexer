import json
import requests

target_block_number = 12383119

# Reading the JSON data from the file
with open(f"uniswapv3_{target_block_number}.json", 'r') as file:
    parsed_data = json.load(file)


# The Graph API URL for Uniswap v3
GRAPH_URL = 'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3'

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
    
def convert_little_to_big_endian(hex_str, signed=False):
    if hex_str.startswith("0x"):
        hex_str = hex_str[2:]
    # Convert hex string to bytes in little endian
    value_bytes_little = bytes.fromhex(hex_str)
    return int.from_bytes(value_bytes_little, byteorder='little', signed=signed)

def compare_pools(local_pool, fetched_pool):
    differences = {}
    local_attributes = local_pool['attributes']

    # Compare simple fields
    simple_fields = {
        'liquidity': 'liquidity',
        'tick': 'tick',
        'sqrt_price_x96': 'sqrtPrice'
    }
    for local_key, fetched_key in simple_fields.items():
        # Check if the fetched_key exists and is not None
        if fetched_key in fetched_pool and fetched_pool[fetched_key] is not None and local_key in local_attributes and local_attributes[local_key] is not None:
            local_value = convert_little_to_big_endian(local_attributes[local_key]) if local_key != 'tick' else convert_little_to_big_endian(local_attributes[local_key], True)
            fetched_value = int(fetched_pool[fetched_key])
            if local_value != fetched_value:
                differences[local_key] = (local_value, fetched_value)
        else:
            differences[local_key] = (local_key, 'Key not found or value is None')

    # Compare ticks
    for local_key, local_value in local_attributes.items():
        if local_key.startswith('ticks/'):
            tick_idx = int(local_key.split('/')[1].split('/')[0])
            liquidity_net = convert_little_to_big_endian(local_value, True)
            # Find corresponding tick in fetched data
            fetched_tick = next((tick for tick in fetched_pool['ticks'] if int(tick['tickIdx']) == tick_idx), None)
            if fetched_tick is not None and int(fetched_tick['liquidityNet']) != liquidity_net:
                differences[local_key] = (liquidity_net, int(fetched_tick['liquidityNet']))

    return differences

def main():
    for pool in parsed_data['states']:
        pool_id = pool['component_id']
        fetched_pool_data = fetch_pool_data(pool_id, target_block_number)
        if fetched_pool_data:
            differences = compare_pools(pool, fetched_pool_data)
            if differences:
                print(f"Differences found for pool {pool_id}:")
                for key, (local_val, fetched_val) in differences.items():
                    print(f"  {key}: local={local_val}, fetched={fetched_val}")
            else:
                print(f"No differences found for pool {pool_id}.")
        else:
            print(f"Failed to fetch data for pool {pool_id}.")

if __name__ == "__main__":
    main()