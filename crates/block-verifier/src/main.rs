use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Value, json, value::RawValue};
use tokio::time::{Interval, MissedTickBehavior};
use tokio_postgres::{Client, NoTls, Statement, types::Type};

/// Blocks arrive every ~200ms, so pending rows are polled at the same pace.
const POLL_INTERVAL: Duration = Duration::from_millis(200);
const PENDING_LIMIT: i64 = 1000;

/// At most 10 RPC requests per second.
const RPC_INTERVAL: Duration = Duration::from_millis(100);
const RPC_TIMEOUT: Duration = Duration::from_secs(30);
/// Maximum number of slots asked to `getBlocks` in one request.
const GET_BLOCKS_RANGE: u64 = 1000;

const INITIAL_BACKOFF: Duration = Duration::from_millis(500);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// JSON-RPC error codes: the slot was skipped (no block exists).
const SLOT_SKIPPED: [i64; 2] = [-32007, -32009];
/// JSON-RPC error code: the node no longer has the block.
const BLOCK_CLEANED_UP: i64 = -32001;

const LAST_VERIFIED: &str = "SELECT max(slot) FROM blocks WHERE verified_at IS NOT NULL";
/// Returns no row when the table is empty.
const VERIFY_FIRST: &str = "UPDATE blocks SET verified_at = now() \
     WHERE slot = (SELECT min(slot) FROM blocks) RETURNING slot";
const PENDING: &str = "SELECT slot FROM blocks WHERE verified_at IS NULL AND slot > $1 \
     ORDER BY slot LIMIT $2";
const VERIFY_RANGE: &str = "UPDATE blocks SET verified_at = now() \
     WHERE slot BETWEEN $1 AND $2 AND verified_at IS NULL";
/// If block-saver stored the slot meanwhile, its row is kept and only verified.
const INSERT_RECOVERED: &str = "INSERT INTO blocks (slot, data, block_time, recovered_at, verified_at) \
     VALUES ($1, $2, to_timestamp($3), now(), now()) \
     ON CONFLICT (slot) DO UPDATE SET verified_at = now()";

struct Config {
    rpc_url: String,
    postgres_url: String,
}

struct Statements {
    last_verified: Statement,
    verify_first: Statement,
    pending: Statement,
    verify_range: Statement,
    insert_recovered: Statement,
}

/// Unrecoverable error: the verifier stops instead of retrying.
#[derive(Debug)]
struct Fatal(String);

impl std::fmt::Display for Fatal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Fatal {}

#[derive(Deserialize)]
struct RpcResponse<T> {
    result: Option<T>,
    error: Option<RpcError>,
}

#[derive(Deserialize)]
struct RpcError {
    code: i64,
    message: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BlockMeta {
    block_time: Option<i64>,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("rustls crypto provider already installed");
    let config = Config {
        rpc_url: std::env::var("RPC_URL").context("RPC_URL is not set")?,
        postgres_url: std::env::var("POSTGRES_URL").context("POSTGRES_URL is not set")?,
    };

    tokio::select! {
        _ = tokio::signal::ctrl_c() => Ok(()),
        result = run_forever(&config) => result,
    }
}

/// Retries failed sessions with exponential backoff; returns only on a [`Fatal`] error.
async fn run_forever(config: &Config) -> Result<()> {
    let mut backoff = INITIAL_BACKOFF;
    loop {
        let err = run_session(config, &mut backoff).await.unwrap_err();
        if err.is::<Fatal>() {
            return Err(err);
        }
        eprintln!("session ended: {err:#}; retrying in {backoff:?}");
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

/// Verifies blocks until a database or RPC call fails. Resets `backoff` after every
/// cycle that completes.
async fn run_session(config: &Config, backoff: &mut Duration) -> Result<()> {
    let (db, connection) = tokio_postgres::connect(&config.postgres_url, NoTls)
        .await
        .context("postgres connect failed")?;
    // Ends when `db` is dropped; a broken connection surfaces as a query error.
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            eprintln!("postgres connection error: {err}");
        }
    });
    let statements = Statements {
        last_verified: db.prepare(LAST_VERIFIED).await?,
        verify_first: db.prepare(VERIFY_FIRST).await?,
        pending: db.prepare_typed(PENDING, &[Type::INT8, Type::INT8]).await?,
        verify_range: db
            .prepare_typed(VERIFY_RANGE, &[Type::INT8, Type::INT8])
            .await?,
        insert_recovered: db
            .prepare_typed(INSERT_RECOVERED, &[Type::INT8, Type::BYTEA, Type::INT8])
            .await?,
    };
    let mut rpc = Rpc::new(&config.rpc_url)?;

    let mut anchor = find_anchor(&db, &statements).await?;
    println!("{} starting after verified slot {anchor}", now());

    loop {
        let pending: Vec<u64> = db
            .query(&statements.pending, &[&(anchor as i64), &PENDING_LIMIT])
            .await?
            .iter()
            .map(|row| row.get::<_, i64>(0) as u64)
            .collect();

        // Contiguous slots are verified with one update; gaps are recovered in between.
        let mut run_start = anchor + 1;
        for &slot in &pending {
            if slot != anchor + 1 {
                verify_range(&db, &statements, run_start, anchor).await?;
                recover_gap(&db, &statements, &mut rpc, anchor + 1, slot - 1).await?;
                run_start = slot;
            }
            anchor = slot;
        }
        verify_range(&db, &statements, run_start, anchor).await?;

        *backoff = INITIAL_BACKOFF;
        if pending.len() < PENDING_LIMIT as usize {
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }
}

/// The latest verified slot; with none, waits for a first block and verifies it.
async fn find_anchor(db: &Client, statements: &Statements) -> Result<u64> {
    let last: Option<i64> = db.query_one(&statements.last_verified, &[]).await?.get(0);
    if let Some(slot) = last {
        return Ok(slot as u64);
    }
    loop {
        if let Some(row) = db.query_opt(&statements.verify_first, &[]).await? {
            let slot = row.get::<_, i64>(0) as u64;
            println!("{} slot={slot} verified as first block", now());
            return Ok(slot);
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

async fn verify_range(db: &Client, statements: &Statements, first: u64, last: u64) -> Result<()> {
    if first <= last {
        db.execute(&statements.verify_range, &[&(first as i64), &(last as i64)])
            .await?;
    }
    Ok(())
}

/// Inserts the slots `first..=last` in order, so the gap shrinks from below if a call fails.
async fn recover_gap(
    db: &Client,
    statements: &Statements,
    rpc: &mut Rpc,
    first: u64,
    last: u64,
) -> Result<()> {
    println!(
        "{} gap slots={first}..={last} ({} slots)",
        now(),
        last - first + 1
    );
    let mut start = first;
    while start <= last {
        let end = (start + GET_BLOCKS_RANGE - 1).min(last);
        let mut existing = rpc.get_blocks(start, end).await?.into_iter().peekable();
        for slot in start..=end {
            let block = match existing.next_if_eq(&slot) {
                Some(_) => rpc.get_block(slot).await?,
                None => None,
            };
            let block_time = match &block {
                Some(block) => serde_json::from_str::<BlockMeta>(block.get())?.block_time,
                None => None,
            };
            db.execute(
                &statements.insert_recovered,
                &[
                    &(slot as i64),
                    &block.as_ref().map(|block| block.get().as_bytes()),
                    &block_time,
                ],
            )
            .await
            .with_context(|| format!("insert recovered slot {slot} failed"))?;
            let status = if block.is_some() {
                "recovered"
            } else {
                "skipped"
            };
            println!("{} slot={slot} {status}", now());
        }
        start = end + 1;
    }
    Ok(())
}

/// JSON-RPC over HTTP, rate limited to one request per [`RPC_INTERVAL`].
struct Rpc {
    client: reqwest::Client,
    url: String,
    ticker: Interval,
}

impl Rpc {
    fn new(url: &str) -> Result<Self> {
        let client = reqwest::Client::builder().timeout(RPC_TIMEOUT).build()?;
        let mut ticker = tokio::time::interval(RPC_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        Ok(Self {
            client,
            url: url.to_string(),
            ticker,
        })
    }

    /// Slots in `start..=end` that have a block, in ascending order.
    async fn get_blocks(&mut self, start: u64, end: u64) -> Result<Vec<u64>> {
        let params = json!([start, end, { "commitment": "finalized" }]);
        match self.call("getBlocks", params).await? {
            Ok(Some(slots)) => Ok(slots),
            Ok(None) => bail!("getBlocks {start}..={end} returned null"),
            Err(err) => Err(rpc_error("getBlocks", start, err)),
        }
    }

    /// The block as raw JSON, or `None` when the slot was skipped. Uses the same
    /// parameters as block-saver's subscription so the stored data has the same format.
    async fn get_block(&mut self, slot: u64) -> Result<Option<Box<RawValue>>> {
        let params = json!([
            slot,
            {
                "commitment": "finalized",
                "encoding": "json",
                "transactionDetails": "full",
                "maxSupportedTransactionVersion": 1,
                "rewards": false
            }
        ]);
        match self.call("getBlock", params).await? {
            Ok(Some(block)) => Ok(Some(block)),
            // Not available yet on this node; retried with the session backoff.
            Ok(None) => bail!("getBlock {slot} returned null"),
            Err(err) if SLOT_SKIPPED.contains(&err.code) => Ok(None),
            Err(err) => Err(rpc_error("getBlock", slot, err)),
        }
    }

    /// Transport and HTTP errors are returned as `Err`; JSON-RPC errors as `Ok(Err)`.
    async fn call<T: DeserializeOwned>(
        &mut self,
        method: &str,
        params: Value,
    ) -> Result<Result<Option<T>, RpcError>> {
        self.ticker.tick().await;
        let body = json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params });
        let bytes = self
            .client
            .post(&self.url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body.to_string())
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .with_context(|| format!("{method} request failed"))?
            .bytes()
            .await
            .with_context(|| format!("{method} response failed"))?;
        let response: RpcResponse<T> = serde_json::from_slice(&bytes)
            .with_context(|| format!("{method} response is not valid JSON-RPC"))?;
        Ok(match response.error {
            Some(err) => Err(err),
            None => Ok(response.result),
        })
    }
}

/// `-32001` is fatal; any other code (e.g. `-32004`, block not available yet) is retried.
fn rpc_error(method: &str, slot: u64, err: RpcError) -> anyhow::Error {
    let message = format!("{method} {slot} failed: {} {}", err.code, err.message);
    if err.code == BLOCK_CLEANED_UP {
        Fatal(message).into()
    } else {
        anyhow!(message)
    }
}

fn now() -> impl std::fmt::Display {
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ")
}
