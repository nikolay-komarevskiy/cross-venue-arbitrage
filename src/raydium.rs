use std::{sync::Arc, time::Duration};

use arc_swap::ArcSwapOption;
use async_trait::async_trait;
use base64::{Engine, prelude::BASE64_STANDARD};
use futures::{SinkExt, StreamExt};
use rust_decimal::{Decimal, prelude::FromPrimitive};
use serde_json::{Value, json};
use thiserror::Error;
use tokio::{select, sync::Notify, time::sleep};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{self, Message},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use url::Url;

use crate::{
    raydium::program::raydium_clmm_idl::{accounts::PoolState, utils::Account},
    task::Task,
    trading_venue::TradingVenue,
};

#[allow(clippy::too_many_arguments)]
mod program {
    use anchor_lang::declare_program;
    declare_program!(raydium_clmm_idl);
}

const RECONNECT_INTERVAL: Duration = Duration::from_secs(1);
const Q64_FACTOR: u128 = 1u128 << 64;

#[derive(Clone, Debug)]
struct PoolSnapshot {
    sqrt_price: Decimal,
    liquidity: Decimal,
    base_scale: Decimal,
    quote_scale: Decimal,
}

impl PoolSnapshot {
    fn from_state(state: &PoolState) -> Option<Self> {
        if state.sqrt_price_x64 == 0 || state.liquidity == 0 {
            return None;
        }

        Some(Self {
            sqrt_price: Decimal::from_u128(state.sqrt_price_x64)? / Decimal::from_u128(Q64_FACTOR)?,
            liquidity: Decimal::from_u128(state.liquidity)?,
            base_scale: Decimal::from_u128(10u128.pow(state.mint_decimals_0 as u32))?,
            quote_scale: Decimal::from_u128(10u128.pow(state.mint_decimals_1 as u32))?,
        })
    }

    fn quote_sell(&self, base_in: Decimal) -> Option<Decimal> {
        if self.liquidity == Decimal::ZERO {
            return None;
        }
        if base_in.is_zero() {
            return Some(Decimal::ZERO);
        }

        let delta_x_raw = base_in * self.base_scale;

        let s2 = self.sqrt_price * self.sqrt_price;

        let denom = self.liquidity + delta_x_raw * self.sqrt_price;

        let delta_y_raw = delta_x_raw * self.liquidity * s2 / denom;

        Some(delta_y_raw / self.quote_scale)
    }

    fn quote_buy(&self, quote_in: Decimal) -> Option<Decimal> {
        if self.liquidity == Decimal::ZERO {
            return None;
        }
        if quote_in.is_zero() {
            return Some(Decimal::ZERO);
        }

        let delta_y_raw = quote_in * self.quote_scale;

        let s_prime = self.sqrt_price + delta_y_raw / self.liquidity;

        let delta_x_raw =
            self.liquidity * (Decimal::ONE / self.sqrt_price - Decimal::ONE / s_prime);

        Some(delta_x_raw / self.base_scale)
    }
}

#[derive(Error, Debug)]
enum RaydiumAmmMonitorError {
    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tungstenite::Error),
    #[error("Monitoring stopped")]
    Stopped,
    #[error("Channel closed")]
    ChannelClosed,
}

pub struct RaydiumAmmMonitor {
    ws_url: Url,
    pool_id: String,
    commitment: String,
    sender: Arc<Notify>,
    pool_snapshot: ArcSwapOption<PoolSnapshot>,
    token: CancellationToken,
}

impl RaydiumAmmMonitor {
    #[must_use]
    pub fn new(ws_url: Url, pool_id: String, commitment: String, token: CancellationToken) -> Self {
        let sender = Arc::new(Notify::new());
        Self {
            ws_url,
            pool_id,
            commitment,
            pool_snapshot: ArcSwapOption::from(None),
            sender,
            token,
        }
    }

    async fn run(&self) -> Result<(), RaydiumAmmMonitorError> {
        let (ws_stream, _) = connect_async(self.ws_url.as_str()).await?;
        info!(url = %self.ws_url, "Websocket connected");

        let (mut ws_write, mut ws_read) = ws_stream.split();

        // Subscribe to pool account events on solana node
        let subscribe_msg = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "accountSubscribe",
            "params": [
                self.pool_id,
                {
                    "encoding": "base64",
                    "commitment": self.commitment
                }
            ]
        });

        let msg = serde_json::to_string(&subscribe_msg).expect("serialization failed");

        ws_write.send(msg.into()).await?;

        info!(pool_id = %self.pool_id, "Subscribed to pool successfully");

        loop {
            select! {
                () = self.token.cancelled() => return Err(RaydiumAmmMonitorError::Stopped),

                msg = ws_read.next() => match msg {
                    None => return Err(RaydiumAmmMonitorError::ChannelClosed),
                    Some(Err(err)) => return Err(RaydiumAmmMonitorError::WebSocket(err)),
                    Some(Ok(msg)) => self.handle_message(msg),
                }
            }
        }
    }

    fn handle_message(&self, msg: Message) {
        let Message::Text(text) = msg else {
            return;
        };

        let value: Value = match serde_json::from_str(&text) {
            Ok(v) => v,
            Err(error) => {
                warn!(?error, raw=%text, "Invalid JSON");
                return;
            }
        };

        let data_str = value
            .pointer("/params/result/value/data/0")
            .and_then(|v| v.as_str());

        let Some(encoded) = data_str else {
            return;
        };

        let decoded = match BASE64_STANDARD.decode(encoded) {
            Ok(bytes) => bytes,
            Err(err) => {
                warn!(?err, "Base64 decode failed");
                return;
            }
        };

        match Account::try_from_bytes(&decoded) {
            Ok(Account::PoolState(state)) => {
                if let Some(snapshot) = PoolSnapshot::from_state(&state) {
                    debug!(pool_snapshot = ?snapshot);
                    self.pool_snapshot.store(Some(Arc::new(snapshot)));
                    self.sender.notify_waiters();
                } else {
                    warn!("Failed to build Raydium snapshot");
                }
            }
            Ok(_) => {}
            Err(error) => warn!(?error, "Account parse failed"),
        }
    }
}

#[allow(dead_code)]
fn compute_price(sqrt_price_x64: u128, decimals_0: u8, decimals_1: u8) -> Option<Decimal> {
    let sqrt_price = (sqrt_price_x64 as f64) / (Q64_FACTOR as f64);
    let decimals_diff = i32::from(decimals_0) - i32::from(decimals_1);
    let price = sqrt_price.powi(2) * 10f64.powi(decimals_diff);
    Decimal::from_f64(price)
}

#[async_trait]
impl Task for RaydiumAmmMonitor {
    async fn run(&self) {
        loop {
            match self.run().await {
                Ok(()) => warn!("Streaming stopped unexpectedly"),
                Err(RaydiumAmmMonitorError::Stopped) => {
                    warn!("Cancelled externally");
                    return;
                }
                Err(error) => warn!(?error, "Monitor error"),
            }
            info!(secs = RECONNECT_INTERVAL.as_secs(), "Reconnecting ...");
            sleep(RECONNECT_INTERVAL).await;
        }
    }
}

impl TradingVenue for RaydiumAmmMonitor {
    const NAME: &'static str = "RaydiumClmm";

    fn try_quote_sell(&self, base_in: Decimal) -> Option<Decimal> {
        let base_in = base_in.max(Decimal::ZERO); // set negative to zero
        if base_in.is_zero() {
            return Some(Decimal::ZERO);
        }
        let snapshot = self.pool_snapshot.load_full()?;
        snapshot.as_ref().quote_sell(base_in)
    }

    fn try_quote_buy(&self, quote_in: Decimal) -> Option<Decimal> {
        let quote_in = quote_in.max(Decimal::ZERO);
        if quote_in.is_zero() {
            return Some(Decimal::ZERO);
        }
        let snapshot = self.pool_snapshot.load_full()?;
        snapshot.as_ref().quote_buy(quote_in)
    }

    fn subscribe(&self) -> Arc<Notify> {
        self.sender.clone()
    }
}
