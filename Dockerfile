FROM rust:1.72-bookworm AS build
WORKDIR /build
ARG TARGETPLATFORM
RUN ARCH=$(echo $TARGETPLATFORM | sed -e 's/\//_/g') && \ 
    LINK=$(curl -s https://api.github.com/repos/streamingfast/substreams/releases/latest | awk '/download.url.*'"$ARCH"'/ {print $2}' | sed 's/"//g') && \
    echo ARCH: $ARCH, LINK: $LINK && \
    curl -L  $LINK  | tar zxf - -C /usr/local/bin/
RUN apt-get update && apt-get install -y libpq-dev
RUN cargo install cargo-workspaces
COPY . .
# the hack below is probably needed because of rust-toolchain.toml
# will fix this later we have time to optimise the build
RUN rustup target add wasm32-unknown-unknown && ./stable-build.sh

FROM debian:bookworm
WORKDIR /the/workdir/path
COPY --from=build /build/target/release/tycho-indexer ./tycho-indexer
COPY --from=build /build/target/spkg/*.spkg ./
RUN apt-get update && apt-get install -y libpq-dev && rm -rf /var/lib/apt/lists/*
ENTRYPOINT ["./tycho-indexer"]