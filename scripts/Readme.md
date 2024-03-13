# Getting started

## Compare scripts

These scripts are made to verify our data against a trusted source.

To run them you will first need to get some data from Tycho:

```bash
kubectl port-forward svc/tycho-indexer -n prod-tycho 4242:80
```

Go to http://0.0.0.0:4242/docs/ and use the endpoints to get the state of the protocol you want to check.

And store the result in a json file with this name format `{protocol}_{block_number}.json`.

For example `uniswapv2_10000.json`

Then change the `target_block_number` value in the script file and run it.
