FROM rust:latest as builder
WORKDIR /app

RUN apt-get update && apt-get install -y pkg-config libssl-dev

RUN cargo build --release

FROM debian:bookworm-slim
WORKDIR /app

RUN apt-get update && apt-get install -y libssl3 ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/gateway .

EXPOSE 3000
CMD ["./gateway"]
