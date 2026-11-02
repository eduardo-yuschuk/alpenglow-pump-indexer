# Prompt Log

Every user prompt in this project, recorded with its full local timestamp. Prompts are translated to English.

---

## 2026-11-02 09:16:15 (UTC-03:00)

Log every prompt of mine in docs/PROMPTS.md with the full local timestamp and with the text always in English. We speak Spanish, but all content is recorded in English.

## 2026-11-02 09:25:08 (UTC-03:00)

Fix PROMPTS.md, we are in UTC-3.

## 2026-11-02 09:26:10 (UTC-03:00)

Save these instructions for future sessions; I'm going to start another session to continue with the plugins and MCP enabled.

## 2026-11-02 09:32:23 (UTC-03:00)

We are going to work on main the whole time. I want to create a quick prototype of a Solana indexer in Rust that captures pump.fun events and prints them on screen. We are going to split the project into three components: block-saver, bock-verifier and block-indexer. Create a Rust workspace with the placeholders for the three services and let's start with the block-saver. Start by subscribing via websockets to blockSubscribe with the endpoint configured in .env. Don't spend much on testing or performance measurements; implement exclusively the service code up to printing timestamp and slot to the console.

## 2026-11-02 09:36:37 (UTC-03:00)

In crates/block-saver/src/main.rs, change the subscribe JSON to receive the full block, not only the pump.fun transactions; change the commitment to finalized and the encoding to json.

## 2026-11-02 09:38:42 (UTC-03:00)

Write a README.md with the command to run the block-saver.

## 2026-11-02 09:40:34 (UTC-03:00)

Commit and push to main.

## 2026-11-02 09:41:44 (UTC-03:00)

Implement reconnection with exponential backoff.

## 2026-11-02 09:45:02 (UTC-03:00)

This is wrong:
/// A finalized block arrives every ~400ms; silence this long means a dead connection.

Blocks arrive roughly every 200 ms.

## 2026-11-02 09:46:10 (UTC-03:00)

How is `_ = tokio::signal::ctrl_c() => Ok(())` handling Ctrl+C in the terminal?

## 2026-11-02 09:52:06 (UTC-03:00)

Commit and push to main.

## 2026-11-02 10:12:25 (UTC-03:00)

Define a table (blocks) to store in PostgreSQL (POSTGRES_URL in .env) to store whole blocks organized by slot, in a format not susceptible to special characters, and considering that blocks will remain in the table only a few minutes. The place for schemas is the sql folder.

## 2026-11-02 10:16:11 (UTC-03:00)

Add a block_time column to calculate the latency between when the block is produced on the blockchain and when we store it in our database.

## 2026-11-02 10:17:17 (UTC-03:00)

Commit and push everything to main.
