# alpenglow-pump-indexer

Prototype Solana indexer in Rust that captures pump.fun events and prints them to the console.

The workspace is split into three services under `crates/`:

- `block-saver`: subscribes to `blockSubscribe` over websockets and stores full finalized blocks in PostgreSQL.
- `block-verifier`: checks that stored slots are contiguous and recovers missing ones over RPC (see [docs/BLOCK_VERIFIER.md](docs/BLOCK_VERIFIER.md)).
- `block-indexer`: placeholder.

## Configuration

Create a `.env` file in the repository root (see `.env.example`):

```
WSS_URL=wss://your-rpc-endpoint
RPC_URL=https://your-rpc-endpoint
POSTGRES_URL=postgres://user:password@localhost:5432/indexer
```

`WSS_URL` must support `blockSubscribe`. Create the schema with:

```sh
psql "$POSTGRES_URL" -f sql/001_blocks.sql
```

## Running the block-saver

```sh
cargo run -p block-saver
```

It prints one line per block with the receive time, slot, block time and delay. Press Ctrl+C to stop.

## Running the block-verifier

```sh
cargo run -p block-verifier
```

It prints one line per detected gap and per recovered or skipped slot. Press Ctrl+C to stop.
