# Shopping Sessions: native Rust pipeline (see README.md).
FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY .cargo ./.cargo
# Scorers are Intel Xeon Cascade Lake or newer, so AVX2 is always available.
ENV RUSTFLAGS="-C target-cpu=x86-64-v3"
RUN cargo build --release --locked && strip target/release/shopping

FROM debian:bookworm-slim
COPY --from=build /src/target/release/shopping /usr/local/bin/shopping
USER 1000:1000
CMD ["/usr/local/bin/shopping"]
