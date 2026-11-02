# block-verifier

The block-verifier runs independently of the other services. It reads the `blocks` table written by the block-saver and sets `verified_at` on every block once the slot sequence up to that block is complete, recovering missing slots from the RPC node (`RPC_URL` in `.env`).

Block hashes are not checked: blocks are stored at `finalized` commitment, so they all belong to the main chain.

## Decisions

### Meaning of `verified_at`

`verified_at` means "the chain is complete up to this slot". A block is verified only when the previous slot is already verified, so the block-indexer can consume verified blocks in slot order without gaps.

### Anchor

- On startup the anchor is `MAX(slot)` among verified rows.
- If no row is verified, `MIN(slot)` of the table is verified as the first block and becomes the anchor. Gaps are only filled upwards from the anchor, never below it.
- If the table is empty, the verifier does nothing.

The anchor is kept in memory and re-read from the database after a reconnect.

### Gaps

When the next unverified slot is above `anchor + 1`, the slots in between are a gap:

1. `getBlocks(start, end)` lists which slots in the gap have a block, in ranges of at most 1000 slots.
2. Slots missing from that list are confirmed skipped slots: they are inserted with `data = NULL`.
3. Slots present in the list are fetched with `getBlock`, using the same parameters as the block-saver subscription (`finalized`, `json`, `transactionDetails: full`, `maxSupportedTransactionVersion: 1`, no rewards) so `data` has the same format regardless of its source.

Recovered and skipped slots are inserted with `recovered_at` and `verified_at`, in slot order, so the anchor advances one slot at a time and partial progress survives a failure.

### RPC errors

| Error | Meaning | Action |
|---|---|---|
| `-32007`, `-32009` | Slot skipped, no block | Insert with `data = NULL`, verify |
| `-32004`, timeout, HTTP 429 / 5xx, `null` result | Transient | Retry with exponential backoff, do not verify |
| `-32001` | Block cleaned up, the node no longer has it | Stop with an error |
| Any other JSON-RPC error | Unknown | Treated as transient and logged |

### Conflicts with the block-saver

The verifier inserts with `INSERT ... ON CONFLICT (slot) DO UPDATE SET verified_at = now()`. If the block-saver stored the slot first, its data is kept and the row is verified without `recovered_at`.

### Detection and limits

- New blocks are detected by polling the table every 200ms.
- RPC calls are limited to 10 per second.
- Values are constants in the code.

### Logging

One line per detected gap and per recovered or skipped slot. Normally verified blocks are not logged.

### Schema

`sql/001_blocks.sql` was edited in place and the table recreated (its contents are transient):

- `data` is nullable (NULL for skipped slots).
- `verified_at` and `recovered_at` columns.
- Partial index on `slot WHERE verified_at IS NULL` to find pending blocks.

Latency metrics should exclude recovered rows (`recovered_at IS NOT NULL`).

## Out of scope

- Pruning old blocks. When it is added it must never delete the latest verified block, or the verifier loses its anchor.
- Running a single instance (e.g. `pg_try_advisory_lock`). Only one verifier must run at a time.
