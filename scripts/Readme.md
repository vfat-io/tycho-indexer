# Getting started

## Compare scripts

All comparison scripts rely on using an archive node. You will need to set it using the 
`ETH_RPC_URL` env var.


### UniswapV2 & Balancer

These scripts are made to verify our data against a trusted source.

To run them you will first need to get some data from Tycho RPC. Use the state endpoints to 
get the state of the protocol you want to check and store the result in a json file with this 
name format: `{protocol}_{block_number}.json`. For example `uniswap_v2_10000.json`

Then and run it with the following command:
```bash
python compare-uniswap-v2.py <block_number>
```

Note, the script uses web3. If you have not got it installed already, you will need to do so:
```bash
pip install web3
```


### UniswapV3

You'll need the requests library installed, then pass block and pool addresses to compare:

```bash
python scripts/compare-uniswap-v3-the-graph.py \
    19510400 \
    0x1385fc1fe0418ea0b4fcf7adc61fc7535ab7f80d \
    0x6b6c7beadce465f8f2ada88903bdbbb170fa1f10
```