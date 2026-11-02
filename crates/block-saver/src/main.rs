use anyhow::{Context, Result, bail};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};

/// Full blocks can be several MB; raise tungstenite's default limits.
const MAX_MESSAGE_SIZE: usize = 256 << 20;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("rustls crypto provider already installed");
    let wss_url = std::env::var("WSS_URL").context("WSS_URL is not set")?;

    let config = WebSocketConfig::default()
        .max_message_size(Some(MAX_MESSAGE_SIZE))
        .max_frame_size(Some(MAX_MESSAGE_SIZE));
    let (ws, _) = connect_async_with_config(wss_url.as_str(), Some(config), false)
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
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            msg = read.next() => {
                let Some(msg) = msg else { bail!("websocket closed by server") };
                match msg? {
                    Message::Text(text) => handle_message(&text)?,
                    Message::Ping(payload) => write.send(Message::Pong(payload)).await?,
                    Message::Close(frame) => bail!("websocket closed: {frame:?}"),
                    _ => {}
                }
            }
        }
    }

    Ok(())
}

fn handle_message(text: &str) -> Result<()> {
    let msg: Value = serde_json::from_str(text)?;

    if let Some(err) = msg.get("error") {
        bail!("rpc error: {err}");
    }
    if let Some(id) = msg.get("result").filter(|_| msg.get("id").is_some()) {
        println!("subscribed, subscription id {id}");
        return Ok(());
    }
    if msg["method"] != "blockNotification" {
        return Ok(());
    }

    let value = &msg["params"]["result"]["value"];
    let slot = value["slot"].as_u64().unwrap_or_default();
    let block_time = value["block"]["blockTime"].as_i64();
    let received = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ");

    match block_time {
        Some(t) => println!("{received} slot={slot} block_time={t}"),
        None => println!("{received} slot={slot} block_time=none err={}", value["err"]),
    }
    Ok(())
}
