use arc_swap::ArcSwapOption;
use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use reqwest::Client;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
    time::Duration,
};
use thiserror::Error;
use tokio::{select, sync::Notify, time::sleep};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{self, Message},
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use url::Url;

use crate::{arbitrage::HasOrderBook, task::Task, trading_venue::TradingVenue};

type UpdateId = u64;
const RECONNECT_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Error, Debug)]
pub enum OrderBookError {
    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tungstenite::Error),
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Invalid state: {0}")]
    InvalidState(String),
    #[error("Channel closed")]
    ChannelClosed,
    #[error("Monitoring stopped externally")]
    Stopped,
}

// Binance orderbook update message
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DepthUpdate {
    // #[serde(rename = "e")]
    // event_type: String,
    #[serde(rename = "E")]
    event_time: u64,
    // #[serde(rename = "s")]
    // symbol: String,
    #[serde(rename = "U")]
    first_update_id: UpdateId,
    #[serde(rename = "u")]
    final_update_id: UpdateId,
    #[serde(rename = "b")]
    bids: Vec<(String, String)>,
    #[serde(rename = "a")]
    asks: Vec<(String, String)>,
}

// Binance orderbook snapshot message
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DepthSnapshot {
    #[serde(rename = "lastUpdateId")]
    last_update_id: UpdateId,
    bids: Vec<(String, String)>,
    asks: Vec<(String, String)>,
}

#[derive(Debug, Default, Clone)]
pub struct OrderBook {
    pub bids: BTreeMap<Decimal, Decimal>, // key - price, value - quantity
    pub asks: BTreeMap<Decimal, Decimal>,
    pub last_update_id: UpdateId,
    pub server_time_ms: u64,
}

impl OrderBook {
    #[must_use] 
    pub fn new() -> Self {
        Self::default()
    }

    fn apply_update(&mut self, depth: &DepthUpdate) -> Result<(), OrderBookError> {
        // Ignore if final_update_id is already processed
        if depth.final_update_id < self.last_update_id + 1 {
            return Ok(());
        }

        if depth.first_update_id > self.last_update_id + 1 {
            return Err(OrderBookError::InvalidState(format!(
                "Update gap: local={} update=[{},{}]",
                self.last_update_id, depth.first_update_id, depth.final_update_id
            )));
        }

        for (price_s, qty_s) in &depth.bids {
            let price = parse_decimal(price_s, "bid price")?;
            let qty = parse_decimal(qty_s, "bid qty")?;

            if qty.is_zero() {
                self.bids.remove(&price);
            } else {
                self.bids.insert(price, qty);
            }
        }

        for (price_s, qty_s) in &depth.asks {
            let price = parse_decimal(price_s, "ask price")?;
            let qty = parse_decimal(qty_s, "ask qty")?;

            if qty.is_zero() {
                self.asks.remove(&price);
            } else {
                self.asks.insert(price, qty);
            }
        }

        self.last_update_id = depth.final_update_id;
        self.server_time_ms = depth.event_time;

        Ok(())
    }

    fn apply_snapshot(&mut self, snapshot: &DepthSnapshot) -> Result<(), OrderBookError> {
        self.bids.clear();
        self.asks.clear();

        for (price_s, qty_s) in &snapshot.bids {
            let price = parse_decimal(price_s, "bid price")?;
            let qty = parse_decimal(qty_s, "bid qty")?;
            if !qty.is_zero() {
                self.bids.insert(price, qty);
            }
        }

        for (price_s, qty_s) in &snapshot.asks {
            let price = parse_decimal(price_s, "ask price")?;
            let qty = parse_decimal(qty_s, "ask qty")?;
            if !qty.is_zero() {
                self.asks.insert(price, qty);
            }
        }

        self.last_update_id = snapshot.last_update_id;
        self.server_time_ms = 0;
        Ok(())
    }
}

pub struct BinanceMonitor {
    ws_url: Url,
    rest_url: Url,
    orderbook: ArcSwapOption<OrderBook>,
    http_client: Client,
    sender: Arc<Notify>,
    token: CancellationToken,
}

impl BinanceMonitor {
    #[must_use]
    pub fn new(rest_url: Url, ws_url: Url, token: CancellationToken) -> Self {
        Self {
            ws_url,
            rest_url,
            orderbook: ArcSwapOption::new(None),
            http_client: Client::new(),
            sender: Arc::new(Notify::new()),
            token,
        }
    }

    #[inline]
    pub fn orderbook_snapshot(&self) -> Option<Arc<OrderBook>> {
        self.orderbook.load_full()
    }

    async fn run(&self) -> Result<(), OrderBookError> {
        let (ws_stream, _) = connect_async(self.ws_url.as_str()).await?;
        info!(url = %self.ws_url, "WebSocket connected");
        let (mut ws_write, mut ws_read) = ws_stream.split();

        // Buffer for depth updates
        let mut buffer: VecDeque<DepthUpdate> = VecDeque::new();

        // Receive first update
        let first_update = loop {
            match ws_read.next().await {
                Some(Ok(Message::Text(text))) => {
                    let update: DepthUpdate = serde_json::from_str(&text)?;
                    buffer.push_back(update.clone());
                    break update;
                }
                Some(Ok(Message::Ping(p))) => ws_write.send(Message::Pong(p)).await?,
                Some(Ok(_)) => {},
                Some(Err(e)) => return Err(OrderBookError::WebSocket(e)),
                None => {
                    return Err(OrderBookError::InvalidState(
                        "Closed before first event".into(),
                    ));
                }
            }
        };

        let first_u = first_update.first_update_id;
        info!("First update received U={}", first_u);

        // Fetch snapshot
        let snapshot: DepthSnapshot = self
            .http_client
            .get(self.rest_url.as_str())
            .send()
            .await?
            .json()
            .await?;

        if snapshot.last_update_id < first_u {
            return Err(OrderBookError::InvalidState("Snapshot too old".into()));
        }

        // Remove outdated buffered updates
        while buffer
            .front()
            .is_some_and(|u| u.final_update_id <= snapshot.last_update_id)
        {
            buffer.pop_front();
        }

        // Build initial book
        let mut book = OrderBook::new();
        book.apply_snapshot(&snapshot)?;
        info!("Applied snapshot lastUpdateId={}", snapshot.last_update_id);

        // Apply buffered updates
        for update in buffer {
            if update.first_update_id <= book.last_update_id + 1
                && update.final_update_id > book.last_update_id
            {
                book.apply_update(&update)?;
            }
        }

        self.orderbook.store(Some(Arc::new(book.clone())));

        info!(
            best_bid = ?book.bids.iter().next_back(),
            best_ask = ?book.asks.iter().next(),
            "Orderbook initialized"
        );

        loop {
            select! {
                () = self.token.cancelled() => return Err(OrderBookError::Stopped),

                msg = ws_read.next() => match msg {
                    Some(Ok(Message::Text(text))) => {
                        let update: DepthUpdate = serde_json::from_str(&text)?;

                        if let Some(current) = self.orderbook.load_full() {
                            let mut new_book = (*current).clone(); // clone incrementally
                            new_book.apply_update(&update)?;
                            self.orderbook.store(Some(Arc::new(new_book)));
                            self.sender.notify_waiters();
                        }
                    }
                    Some(Ok(Message::Ping(p))) => {
                        ws_write.send(Message::Pong(p)).await?;
                    }
                    Some(Ok(Message::Close(_))) => {
                        info!("WebSocket closed by server");
                        break;
                    }
                    Some(Err(e)) => return Err(OrderBookError::WebSocket(e)),
                    None => {
                        info!("WebSocket ended");
                        break;
                    }
                    _ => {}
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Task for BinanceMonitor {
    async fn run(&self) {
        loop {
            match self.run().await {
                Ok(()) => warn!("Streaming stopped unexpectedly"),
                Err(OrderBookError::Stopped) => {
                    warn!("Cancelled externally");
                    return;
                }
                Err(err) => warn!(error = %err, "Orderbook error"),
            }

            info!(secs = RECONNECT_INTERVAL.as_secs(), "Reconnecting ...");
            sleep(RECONNECT_INTERVAL).await;
        }
    }
}

#[inline]
fn parse_decimal(s: &str, field: &str) -> Result<Decimal, OrderBookError> {
    Decimal::from_str_exact(s)
        .map_err(|_| OrderBookError::InvalidState(format!("Failed to parse {field}: {s}")))
}

impl TradingVenue for BinanceMonitor {
    fn name(&self) -> &'static str {
        "Binance"
    }

    fn subscribe(&self) -> Arc<Notify> {
        self.sender.clone()
    }

    fn try_quote_sell(&self, mut base_in: Decimal) -> Option<Decimal> {
        let book = self.orderbook_snapshot()?;
        let mut quote_out = Decimal::ZERO;

        for (price, qty) in book.bids.iter().rev() {
            let take = base_in.min(*qty);
            quote_out += take * *price;
            base_in -= take;
            if base_in <= Decimal::ZERO {
                return Some(quote_out);
            }
        }

        None
    }

    fn try_quote_buy(&self, mut quote_out: Decimal) -> Option<Decimal> {
        let book = self.orderbook_snapshot()?;
        let mut base_out = Decimal::ZERO;

        for (price, qty) in &book.asks {
            let cost = *qty * *price;
            if cost >= quote_out {
                base_out += quote_out / *price;
                return Some(base_out);
            }
            quote_out -= cost;
            base_out += *qty;
        }

        None
    }
}

impl HasOrderBook for BinanceMonitor {
    fn orderbook_snapshot(&self) -> Option<Arc<OrderBook>> {
        self.orderbook_snapshot()
    }
}
