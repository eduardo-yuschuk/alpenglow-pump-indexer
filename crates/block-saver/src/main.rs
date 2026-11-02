use std::time::Duration;

use anyhow::{Context, Result, bail};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{json, value::RawValue};
use tokio_postgres::{Client, NoTls, Statement, types::Type};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};

/// Full blocks can be several MB; raise tungstenite's default limits.
const MAX_MESSAGE_SIZE: usize = 256 << 20;

const INITIAL_BACKOFF: Duration = Duration::from_millis(500);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// A finalized block arrives every ~200ms; silence this long means a dead connection.
const READ_TIMEOUT: Duration = Duration::from_secs(30);

/// Duplicate notifications for a slot are dropped by the primary key.
const INSERT_BLOCK: &str = "INSERT INTO blocks (slot, data, block_time) \
     VALUES ($1, $2, to_timestamp($3)) ON CONFLICT (slot) DO NOTHING";

struct Config {
    wss_url: String,
    postgres_url: String,
}

/// JSON-RPC message: either the subscribe response or a block notification.
#[derive(Deserialize)]
struct RpcMessage<'a> {
    id: Option<u64>,
    #[serde(borrow)]
    result: Option<&'a RawValue>,
    #[serde(borrow)]
    error: Option<&'a RawValue>,
    method: Option<String>,
    #[serde(borrow)]
    params: Option<NotificationParams<'a>>,
}

#[derive(Deserialize)]
struct NotificationParams<'a> {
    #[serde(borrow)]
    result: NotificationResult<'a>,
}

#[derive(Deserialize)]
struct NotificationResult<'a> {
    #[serde(borrow)]
    value: BlockUpdate<'a>,
}

#[derive(Deserialize)]
struct BlockUpdate<'a> {
    slot: u64,
    /// Kept as the raw JSON slice so it is stored byte-for-byte.
    #[serde(borrow)]
    block: Option<&'a RawValue>,
    #[serde(borrow)]
    err: Option<&'a RawValue>,
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
        wss_url: std::env::var("WSS_URL").context("WSS_URL is not set")?,
        postgres_url: std::env::var("POSTGRES_URL").context("POSTGRES_URL is not set")?,
    };

    tokio::select! {
        _ = tokio::signal::ctrl_c() => Ok(()),
        _ = run_forever(&config) => unreachable!(),
    }
}

async fn run_forever(config: &Config) {
    let mut backoff = INITIAL_BACKOFF;
    // Survives reconnects so a new session does not resend already stored slots.
    let mut last_slot = None;
    loop {
        let err = run_session(config, &mut backoff, &mut last_slot)
            .await
            .unwrap_err();
        eprintln!("session ended: {err:#}; reconnecting in {backoff:?}");
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

/// Runs one database + websocket session until either fails. Resets `backoff` once the
/// subscription is confirmed.
async fn run_session(
    config: &Config,
    backoff: &mut Duration,
    last_slot: &mut Option<u64>,
) -> Result<()> {
    let (db, connection) = tokio_postgres::connect(&config.postgres_url, NoTls)
        .await
        .context("postgres connect failed")?;
    // Ends when `db` is dropped; a broken connection surfaces as a query error.
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            eprintln!("postgres connection error: {err}");
        }
    });
    let insert = db
        .prepare_typed(INSERT_BLOCK, &[Type::INT8, Type::BYTEA, Type::INT8])
        .await
        .context("prepare insert failed")?;

    let ws_config = WebSocketConfig::default()
        .max_message_size(Some(MAX_MESSAGE_SIZE))
        .max_frame_size(Some(MAX_MESSAGE_SIZE));
    let (ws, _) = connect_async_with_config(&config.wss_url, Some(ws_config), false)
        .await
        .context("websocket connect failed")?;
    let (mut write, mut read) = ws.split();

    let subscribe = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "blockSubscribe",
        "params": [
            "all",
            {
                "commitment": "finalized",
                "encoding": "json",
                "transactionDetails": "full",
                "maxSupportedTransactionVersion": 1,
                "showRewards": false
            }
        ]
    });
    write.send(Message::text(subscribe.to_string())).await?;

    loop {
        let msg = tokio::time::timeout(READ_TIMEOUT, read.next())
            .await
            .context("no message received within read timeout")?;
        let Some(msg) = msg else {
            bail!("websocket closed by server")
        };
        match msg? {
            Message::Text(text) => {
                if handle_message(&text, &db, &insert, last_slot).await? {
                    *backoff = INITIAL_BACKOFF;
                }
            }
            Message::Ping(payload) => write.send(Message::Pong(payload)).await?,
            Message::Close(frame) => bail!("websocket closed: {frame:?}"),
            _ => {}
        }
    }
}

/// Stores block notifications. Returns `true` when the message confirms the subscription.
///
/// Finalized blocks arrive in slot order, so any slot not above `last_slot` is a repeated
/// notification and is dropped without touching the database.
async fn handle_message(
    text: &str,
    db: &Client,
    insert: &Statement,
    last_slot: &mut Option<u64>,
) -> Result<bool> {
    let msg: RpcMessage = serde_json::from_str(text)?;

    if let Some(err) = msg.error {
        bail!("rpc error: {err}");
    }
    if let (Some(_), Some(id)) = (msg.id, msg.result) {
        println!("subscribed, subscription id {id}");
        return Ok(true);
    }
    let (Some("blockNotification"), Some(params)) = (msg.method.as_deref(), msg.params) else {
        return Ok(false);
    };

    let update = params.result.value;
    let slot = update.slot;
    if last_slot.is_some_and(|last| slot <= last) {
        return Ok(false);
    }
    let Some(block) = update.block else {
        let err = update.err.map_or("null", RawValue::get);
        println!(
            "{} slot={slot} no block, err={err}",
            timestamp(chrono::Utc::now())
        );
        return Ok(false);
    };

    let block_time = serde_json::from_str::<BlockMeta>(block.get())?.block_time;
    let rows = db
        .execute(
            insert,
            &[&(slot as i64), &block.get().as_bytes(), &block_time],
        )
        .await
        .with_context(|| format!("insert slot {slot} failed"))?;
    *last_slot = Some(slot);

    let now = chrono::Utc::now();
    let status = if rows == 0 { "duplicate" } else { "stored" };
    let (block_time, delay) = match block_time {
        Some(t) => (
            t.to_string(),
            format!(
                "{:.3}s",
                (now.timestamp_millis() - t * 1000) as f64 / 1000.0
            ),
        ),
        None => ("none".to_string(), "none".to_string()),
    };
    println!(
        "{} slot={slot} block_time={block_time} delay={delay} {status}",
        timestamp(now)
    );
    Ok(false)
}

fn timestamp(t: chrono::DateTime<chrono::Utc>) -> impl std::fmt::Display {
    t.format("%Y-%m-%dT%H:%M:%S%.3fZ")
}
