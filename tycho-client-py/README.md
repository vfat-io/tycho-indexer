# Tycho Python Client

A python client for the tycho rpc endpoints.

### How to publish a new version to AWS codeartifact

#### Step 1: Build

```commandline
build_wheel.sh
```

#### Step 2: Publish to AWS using twine

This step requires that you have twine installed and are logged in aws codeartifact. This can be done like this

```commandline
pip install twine
aws codeartifact login --tool pip --repository your_repo --domain your_domain
```

```commandline
twine upload --repository codeartifact dist/*
```