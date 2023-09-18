FROM rust:1.72-bookworm AS build
ARG TARGETPLATFORM
WORKDIR /build
RUN apt-get update && apt-get install -y libpq-dev jq
RUN ARCH=$(echo $TARGETPLATFORM | sed -e 's/\//_/g') && \
    if [ "$ARCH" = "linux_amd64" ]; then \
    ARCH="linux_x86_64"; \
    fi && \
    LINK=$(curl -s https://api.github.com/repos/streamingfast/substreams/releases/latest | jq -r ".assets[] | select(.name | contains(\"$ARCH\")) | .browser_download_url")  && \
    echo ARCH: $ARCH, LINK: $LINK && \
    curl -L  $LINK  | tar zxf - -C /usr/local/bin/
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