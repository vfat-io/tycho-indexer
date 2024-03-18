### Helper script to call a function of a smart contract at a specific block number.
from web3 import Web3

node_url = ''
web3 = Web3(Web3.HTTPProvider(node_url))

block_number = 12370982

transaction = {
    'to': '0xBA12222222228d8Ba445958a75a0704d566BF2C8',
    'from': '0x0000000000000000000000000000000000000000',
    'data': '0xf94d4668'+'0b09dea16768f0799065c475be02919503cb2a3500020000000000000000001a'
}

result = web3.eth.call(transaction, block_identifier=block_number)

print(result.hex())