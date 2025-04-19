# Stage 1: Build
FROM rust:1.75 as builder

WORKDIR /app
COPY . .
RUN apt-get update && apt-get install -y pkg-config libssl-dev
RUN cargo build --release

# Stage 2: Run
FROM debian:buster-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/fluvio-collab-backend   /usr/local/bin/app

EXPOSE 8080
CMD ["/usr/local/bin/app"]

