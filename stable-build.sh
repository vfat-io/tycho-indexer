set -e
WORKDIR=$(pwd)
TYCHO_INDEXER=tycho-indexer

echo "Building tycho: $TYCHO_INDEXER"
RUSTFLAGS="--cfg tokio_unstable" cargo build --package $TYCHO_INDEXER --release
