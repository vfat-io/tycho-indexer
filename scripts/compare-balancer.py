import asyncio
import aiohttp
import json
from web3 import Web3
from hexbytes import HexBytes

# Block number you are validating against
block_number = 12657079
rps = 1000

# Load your data file
with open(f'balancer_{block_number}.json', 'r') as file:
    data = json.load(file)

NODE_URLS = [
    ''
]
current_node = 0

async def fetch_storage_value(session, address, slot, block_number, retry=0):
    global current_node
    
    # Switch to the next node URL in round-robin fashion to not exceed rate limit.
    node_url = NODE_URLS[current_node]
    current_node = (current_node + 1) % len(NODE_URLS)
    
    payload = json.dumps({
        "jsonrpc": "2.0",
        "method": "eth_getStorageAt",
        "params": [address, slot, hex(block_number)],
        "id": 1
    })
    headers = {'Content-Type': 'application/json'}
    try:
        async with session.post(node_url, data=payload, headers=headers) as response:
            response_json = await response.json()
            if 'result' in response_json:
                return response_json['result']
            else:
                print(f"fail for address {address}, slot {slot}: {response_json}")
                raise Exception("Failed to get storage value")
    except Exception as e:
        if retry < 3:  # Retry up to 3 times
            await asyncio.sleep(2 ** retry)  # Exponential backoff
            return await fetch_storage_value(session, address, slot, block_number, retry + 1)
        else:
            print(f"Final fail for address {address}, slot {slot}: {e}")
            return None

                
async def validate_account_storage(account, block_number, rps=9):
    async with aiohttp.ClientSession() as session:
        address = Web3.toChecksumAddress(account['address'])
        tasks = []
        for slot, expected_value in account['slots'].items():
            tasks.append(fetch_storage_value(session, address, slot, block_number))
        
        # Throttling to stay under the rate limit
        actual_values = []
        while tasks:
            batch, tasks = tasks[:rps], tasks[rps:]
            results = await asyncio.gather(*batch)
            actual_values.extend(results)
            await asyncio.sleep(1)  # Wait for 1 second before the next batch

        invalid_slots = {}
        for i, (slot, expected_value) in enumerate(account['slots'].items()):
            actual_value = actual_values[i]
            if actual_value != expected_value:
                # Record the slot and the discrepancies
                invalid_slots[slot] = {'expected': expected_value, 'actual': actual_value}

        # Print the summary for the account
        if invalid_slots:
            print(f"Account {account['address']} INVALID. Diffs:")
            for slot, diff in invalid_slots.items():
                print(f"  Slot {slot}: Expected {diff['expected']}, Got {diff['actual']}")
        else:
            print(f"Account {account['address']} is VALID.")

async def main():
    # Load your data file
    with open(f'balancer-{block_number}.json', 'r') as file:
        data = json.load(file)

    # Validate each account
    for account in data['accounts']:
        await validate_account_storage(account, block_number, rps)

# Run the main function
asyncio.run(main())

