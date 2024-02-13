import os
import json
from web3 import Web3

target_block_number = 10055556

# Reading the JSON data from the file
with open(f"uniswapv2_{target_block_number}.json", 'r') as file:
    parsed_data = json.load(file)

# RPC URL
ETH_RPC_URL = os.getenv('ETH_RPC_URL')
if not ETH_RPC_URL:
    raise ValueError("ETH_RPC_URL environment variable not set")

web3 = Web3(Web3.HTTPProvider(ETH_RPC_URL))

# ABI to interact with UniswapV2 pool to get reserves
pool_abi = [{
    "constant": True,
    "inputs": [],
    "name": "getReserves",
    "outputs": [
        {"internalType": "uint112", "name": "reserve0", "type": "uint112"},
        {"internalType": "uint112", "name": "reserve1", "type": "uint112"},
        {"internalType": "uint32", "name": "blockTimestampLast", "type": "uint32"}
    ],
    "payable": False,
    "stateMutability": "view",
    "type": "function"
}]

def convert_little_to_big_endian(hex_str, signed=False):
    if hex_str.startswith("0x"):
        hex_str = hex_str[2:]
    # Convert hex string to bytes in little endian
    value_bytes_little = bytes.fromhex(hex_str)
    return int.from_bytes(value_bytes_little, byteorder='little', signed=signed)

def compare_pool_data(pool, target_block_number):
    component_id = Web3.toChecksumAddress(pool['component_id'])
    contract = web3.eth.contract(address=component_id, abi=pool_abi)
    reserves = contract.functions.getReserves().call(block_identifier=target_block_number)

    local_reserve0_big_endian = convert_little_to_big_endian(pool['attributes']['reserve0'])
    local_reserve1_big_endian = convert_little_to_big_endian(pool['attributes']['reserve1'])

    # Initialize a variable to track if there are differences
    has_diff = False
    diff_messages = []

    # Check for differences in reserve0
    if local_reserve0_big_endian != reserves[0]:
        diff_messages.append(f"Local reserve0 (big endian): {local_reserve0_big_endian}, Historical reserve0: {reserves[0]}")
        has_diff = True

    # Check for differences in reserve1
    if local_reserve1_big_endian != reserves[1]:
        diff_messages.append(f"Local reserve1 (big endian): {local_reserve1_big_endian}, Historical reserve1: {reserves[1]}")
        has_diff = True

    if has_diff:
        # If there are differences, print them
        for msg in diff_messages:
            print(msg)
    else:
        print(f"No differences found in {pool['component_id']}.")

    print("---")
    
    
for state in parsed_data['states']:
    compare_pool_data(state, target_block_number)