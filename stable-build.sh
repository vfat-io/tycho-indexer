# This is a workaround for building all binaries of 
# the workspaces until "per_package_target" features is 
# stabilised.
set -e

SUBSTREAM_PKGS=$(cargo ws list | grep -i substreams)
WORKDIR=$(pwd)
for package in $SUBSTREAM_PKGS
do
   echo "Building wasm package: $package"
   cargo build --package $package --target wasm32-unknown-unknown --profile substreams

   echo "Packaging into spkg..."
   cd $(echo $package | sed 's/-/\//1' )
        substreams pack -o $WORKDIR/target/spkg/$package.spkg
   cd -
done

echo "Building tycho"
cargo build --package tycho-indexer --release
