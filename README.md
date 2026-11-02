# alpenglow-pump-indexer

Prototype Solana indexer in Rust that captures pump.fun events and prints them to the console.

The workspace is split into three services under `crates/`:

- `block-saver`: subscribes to `blockSubscribe` over websockets and receives full finalized blocks.
- `block-verifier`: placeholder.
- `block-indexer`: placeholder.

## Configuration

Create a `.env` file in the repository root with the websocket endpoint of an RPC node that supports `blockSubscribe`:

```
WSS_URL=wss://your-rpc-endpoint
```

## Running the block-saver

```sh
cargo run -p block-saver
```

It prints one line per block with the receive time, slot and block time. Press Ctrl+C to stop.
