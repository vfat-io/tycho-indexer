FROM rust:1.72 AS build
WORKDIR /build
RUN echo "fn main() {}" > dummy.rs
COPY Cargo.toml .
COPY Cargo.lock .
RUN sed -i 's#src/main.rs#dummy.rs#' Cargo.toml
RUN cargo build --release
RUN sed -i 's#dummy.rs#src/main.rs#' Cargo.toml
COPY . .
RUN cargo build --release


FROM debian:bullseye-slim
COPY --from=build /build/target/release/tycho-indexer ./target/release/tycho-indexer
ENTRYPOINT ["./target/release/tycho-indexer"]