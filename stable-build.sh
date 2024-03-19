set -e
WORKDIR=$(pwd)
TYCHO_INDEXER=tycho-indexer

echo "Building tycho: $TYCHO_INDEXER"
cargo build --package $TYCHO_INDEXER --release
