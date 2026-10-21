# rust-database

A Redis-compatible in-memory key-value server written from scratch in Rust.
It speaks RESP2 over TCP, so `redis-cli`, `redis-benchmark` and standard
client libraries work with it unmodified.

> Status: work in progress. Done so far: the RESP2 codec skeleton.

## Running

```sh
cargo run --release -- --port 6380
redis-cli -p 6380 PING
```

`RUST_LOG=debug` turns on verbose tracing.

## Features

| Area        | Status |
|-------------|--------|
| RESP2 codec | 🚧     |
| Strings     | ⏳     |
| Expiry      | ⏳     |
| Lists, hashes, sets | ⏳ |
| Pub/Sub     | ⏳     |
| AOF         | ⏳     |
| Metrics, graceful shutdown | ⏳ |

Out of scope: clustering, replication, Lua scripting, RESP3.

## Design decisions

### Single crate, library + thin binary
All the logic lives in the library crate. `main.rs` only parses flags, sets
up tracing and starts the server. Integration tests, benchmarks and fuzz
targets need access to the internals, and this layout gives it to them
without a workspace. A workspace would pay off with several deployables,
which this project doesn't have.

### Binary-safe data
Keys and values are `bytes::Bytes`, never `String`. Redis keys can be any
byte sequence, including invalid UTF-8.

### No panics on client input
Clippy's `unwrap_used`, `expect_used` and `panic` lints are set to `deny`
outside tests. Malformed input gets a RESP error reply, and the server
process is never at risk.

### RESP decoder limits
The decoder enforces a maximum bulk length (512 MiB, same as Redis's
`proto-max-bulk-len`), a maximum array length and a maximum nesting depth.
It never preallocates memory based on a length the client sent, so a
13-byte `*2147483647\r\n` can't make the server allocate gigabytes.

<!-- More sections get added with each milestone: the keyspace locking strategy, expiry, the pub/sub backpressure trade-off, AOF fsync policies. -->

## Testing

```sh
cargo test                     # unit, property and integration tests
cargo clippy --all-targets -- -D warnings
```
