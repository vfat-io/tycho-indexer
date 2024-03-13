### Helper script to fetche logs for a specific event from a specific contract within a given block range.

from web3 import Web3

YOUR_PROVIDER_URL = ''
event_topic = '0x2170c741c41531aec20e7c107c24eecfdd15e69c9bb0a8dd37b1840b9e0b207b'# SWAP
contract_address = '0xBA12222222228d8Ba445958a75a0704d566BF2C8'
from_block = '12369300'
to_block = '12370400'

# Convert block numbers from string to integer, then to hexadecimal
from_block_hex = hex(int(from_block))
to_block_hex = hex(int(to_block))

# Initialize a web3 connection
web3 = Web3(Web3.HTTPProvider(YOUR_PROVIDER_URL))

# Validate the connection
if web3.isConnected():
    print("Connected to Ethereum network")
else:
    print("Failed to connect to Ethereum network")

# Fetch logs
logs = web3.eth.get_logs({
    'fromBlock': from_block_hex,
    'toBlock': to_block_hex,
    'address': contract_address,
    'topics': [event_topic]
})

# Check if any logs were found and print them
if logs:
    print(f"Found {len(logs)} events between blocks {from_block} and {to_block}:")
    for log in logs:
        print(log)
else:
    print(f"No events found between blocks {from_block} and {to_block}.")
