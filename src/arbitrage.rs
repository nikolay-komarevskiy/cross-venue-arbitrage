use async_trait::async_trait;
use rust_decimal::Decimal;
use std::sync::Arc;
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::binance::OrderBook;
use crate::task::Task;
use crate::trading_venue::TradingVenue;

pub trait HasOrderBook: Send + Sync {
    fn orderbook_snapshot(&self) -> Option<Arc<OrderBook>>;
}

pub trait ArbitrageCallback: Send + Sync {
    fn call(&self, opp: ArbitrageOpportunity);
}

// Implement trait for closure
impl<F> ArbitrageCallback for F
where
    F: Fn(ArbitrageOpportunity) + Send + Sync,
{
    fn call(&self, opp: ArbitrageOpportunity) {
        (self)(opp);
    }
}

#[derive(Debug)]
pub struct ArbitrageOpportunity {
    pub amount: Decimal,
    pub profit: Decimal,
    pub direction: String,
}

pub struct SimpleArbitrageDetector<T, U, C> {
    venue_a: Arc<T>,
    venue_b: Arc<U>,
    trade_amounts: Vec<Decimal>,
    callback: Arc<C>,
    token: CancellationToken,
}

impl<T, U, C> SimpleArbitrageDetector<T, U, C>
where
    T: TradingVenue,
    U: TradingVenue,
    C: ArbitrageCallback,
{
    pub const fn new(
        venue_a: Arc<T>,
        venue_b: Arc<U>,
        trade_amounts: Vec<Decimal>,
        callback: Arc<C>,
        token: CancellationToken,
    ) -> Self {
        Self {
            venue_a,
            venue_b,
            trade_amounts,
            callback,
            token,
        }
    }
}

#[async_trait]
impl<T, U, C> Task for SimpleArbitrageDetector<T, U, C>
where
    T: TradingVenue,
    U: TradingVenue,
    C: ArbitrageCallback,
{
    async fn run(&self) {
        let venue_a = self.venue_a.clone();
        let venue_b = self.venue_b.clone();

        let receiver_a = venue_a.subscribe();
        let receiver_b = venue_b.subscribe();

        let trade_amounts = &self.trade_amounts;
        let callback = self.callback.clone();
        let on_notify = || {
            for &amount in trade_amounts {
                // sell on A -> buy on B
                check_arbitrage(amount, &venue_a, &venue_b, callback.as_ref());
                // sell on B -> buy on A
                check_arbitrage(amount, &venue_b, &venue_a, callback.as_ref());
            }
        };

        loop {
            select! {
                () = self.token.cancelled() => {
                    warn!("Arbitraging cancelled");
                    break;
                }
                () = receiver_a.notified() => {
                    on_notify();
                }
                () = receiver_b.notified() => {
                    on_notify();
                }
            }
        }
    }
}

pub struct OrderBookArbitrageDetector<T, U, C> {
    cex: Arc<T>,
    dex: Arc<U>,
    depth: usize,
    callback: Arc<C>,
    token: CancellationToken,
}

impl<T, U, C> OrderBookArbitrageDetector<T, U, C>
where
    T: TradingVenue + HasOrderBook,
    U: TradingVenue,
    C: ArbitrageCallback,
{
    pub const fn new(
        cex: Arc<T>,
        dex: Arc<U>,
        depth: usize,
        callback: Arc<C>,
        token: CancellationToken,
    ) -> Self {
        Self {
            cex,
            dex,
            depth,
            callback,
            token,
        }
    }

    fn run_check(&self) {
        let Some(book) = self.cex.orderbook_snapshot() else {
            return;
        };

        let mut bid_amounts = Vec::new();
        let mut bid_cumulative = Decimal::ZERO;
        for (_, qty) in book.bids.iter().rev().take(self.depth) {
            if *qty <= Decimal::ZERO {
                continue;
            }
            bid_cumulative += *qty;
            if bid_cumulative > Decimal::ZERO {
                bid_amounts.push(bid_cumulative);
            }
        }

        let mut ask_amounts = Vec::new();
        let mut ask_cumulative = Decimal::ZERO;
        for (_, qty) in book.asks.iter().take(self.depth) {
            if *qty <= Decimal::ZERO {
                continue;
            }
            ask_cumulative += *qty;
            if ask_cumulative > Decimal::ZERO {
                ask_amounts.push(ask_cumulative);
            }
        }

        if bid_amounts.is_empty() && ask_amounts.is_empty() {
            return;
        }

        for amount in bid_amounts {
            // sell on CEX, buy on DEX
            check_arbitrage(amount, &self.cex, &self.dex, self.callback.as_ref());
        }

        for amount in ask_amounts {
            // NOTE: here we reuse Binance ask depth as the trade size for the DEX -> CEX leg,
            // assuming Raydium has constant price and infinite reserves.

            // sell on DEX, buy on CEX
            check_arbitrage(amount, &self.dex, &self.cex, self.callback.as_ref());
        }
    }
}

#[async_trait]
impl<T, U, C> Task for OrderBookArbitrageDetector<T, U, C>
where
    T: TradingVenue + HasOrderBook,
    U: TradingVenue,
    C: ArbitrageCallback,
{
    async fn run(&self) {
        let receiver_cex = self.cex.subscribe();
        let receiver_dex = self.dex.subscribe();

        loop {
            select! {
                () = self.token.cancelled() => {
                    warn!("Arbitraging cancelled");
                    break;
                }
                () = receiver_cex.notified() => {
                    self.run_check();
                }
                () = receiver_dex.notified() => {
                    self.run_check();
                }
            }
        }
    }
}

pub fn check_arbitrage<T, U, C>(
    amount: Decimal,
    venue_sell: &Arc<T>,
    venue_buy: &Arc<U>,
    callback: &C,
) where
    T: TradingVenue,
    U: TradingVenue,
    C: ArbitrageCallback,
{
    let profit_ab = venue_sell
        .try_quote_sell(amount)
        .and_then(|q| venue_buy.try_quote_buy(q))
        .map(|returned| returned - amount);

    if let Some(profit) = profit_ab
        && profit > Decimal::ZERO
    {
        callback.call(ArbitrageOpportunity {
            amount,
            profit,
            direction: format!("{} → {}", venue_sell.name(), venue_buy.name()),
        });
    }
}
