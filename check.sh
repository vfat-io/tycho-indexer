set -e 

cargo +nightly fmt -- --check
cargo +nightly clippy --all --all-features --all-targets -- -D warnings
cargo nextest run --workspace --all-targets --all-features -E 'not test(serial_db)'
cargo nextest run --workspace --all-targets --all-features -E 'test(serial_db)'
