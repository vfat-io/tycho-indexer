set -e 

cargo +nightly fmt -- --check
cargo +nightly clippy --all --all-features --all-targets -- -D warnings
cargo test --all --all-features
