# Getting started

## Compare scripts


### UniswapV2 & Balancer

These scripts are made to verify our data against a trusted source.

To run them you will first need to get some data from Tycho:

```bash
kubectl port-forward svc/tycho-indexer -n prod-tycho 4242:80
```

Go to http://0.0.0.0:4242/docs/ and use the endpoints to get the state of the protocol you want to check.

And store the result in a json file with this name format `{protocol}_{block_number}.json`.

For example `uniswapv2_10000.json`

Then change the `target_block_number` value in the script file and run it.


### UniswapV3

You'll need the requests library installed, then pass block and pool addresses to compare:

```bash
# port-forward to connect to dev-tycho or skip to use local process
kubectl port-forward svc/tycho-indexer -n prod-tycho 4242&
python scripts/compare-uniswap-v3-the-graph.py \
    19510400 \
    0x1385fc1fe0418ea0b4fcf7adc61fc7535ab7f80d \
    0x6b6c7beadce465f8f2ada88903bdbbb170fa1f10
```