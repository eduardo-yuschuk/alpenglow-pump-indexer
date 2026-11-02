-- Staging table for full blocks received by block-saver.
--
-- Rows are short-lived (a few minutes): block-verifier / block-indexer consume
-- them and old slots are pruned. The table is therefore UNLOGGED: no WAL is
-- written, which makes the heavy insert/delete churn much cheaper. Its contents
-- are truncated after a crash, which is acceptable for transient data that can
-- be re-fetched from the RPC.

CREATE UNLOGGED TABLE IF NOT EXISTS blocks (
    -- Duplicate notifications for the same slot are dropped with
    -- INSERT ... ON CONFLICT (slot) DO NOTHING.
    slot        BIGINT      PRIMARY KEY,

    -- UTF-8 bytes of the `block` object from the blockNotification, stored
    -- verbatim. BYTEA instead of TEXT/JSONB because those reject NUL
    -- (\u0000) and invalid UTF-8, and JSONB would re-encode the payload.
    data        BYTEA       COMPRESSION lz4 NOT NULL,

    -- `blockTime` from the block (unix seconds, so whole-second precision),
    -- stored as to_timestamp(blockTime). NULL when the node does not report it.
    -- Storage latency: received_at - block_time.
    block_time  TIMESTAMPTZ,

    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
WITH (
    -- Rows are only inserted and deleted, so vacuum on a fixed row count
    -- instead of a fraction of the table to keep dead tuples and TOAST bloat low.
    autovacuum_vacuum_scale_factor = 0,
    autovacuum_vacuum_threshold = 1000,
    toast.autovacuum_vacuum_scale_factor = 0,
    toast.autovacuum_vacuum_threshold = 1000
);

-- Pruning uses the primary key, since slots advance every ~200ms
-- (5 minutes ≈ 1500 slots):
--   DELETE FROM blocks WHERE slot < $1;
