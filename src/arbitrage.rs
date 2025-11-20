use async_trait::async_trait;
use rust_decimal::Decimal;
use std::sync::Arc;
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::binance::OrderBook;
use crate::task::Task;
use crate::trading_venue::TradingVenue;

pub const TOKEN_PAIR: &str = "solusdc"; // SOLANA/USDC

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
    #[inline(always)]
    fn call(&self, opp: ArbitrageOpportunity) {
        (self)(opp)
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
    pub fn new(
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
            check_arbitrage(
                trade_amounts,
                trade_amounts,
                &venue_a,
                &venue_b,
                callback.as_ref(),
            );
        };

        loop {
            select! {
                _ = self.token.cancelled() => {
                    warn!("Arbitraging cancelled");
                    break;
                }
                _ = receiver_a.notified() => {
                    on_notify();
                }
                _ = receiver_b.notified() => {
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
    pub fn new(
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

        check_arbitrage(
            &bid_amounts,
            &ask_amounts,
            &self.cex,
            &self.dex,
            self.callback.as_ref(),
        );
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
        let venue_cex = self.cex.clone();
        let venue_dex = self.dex.clone();

        let receiver_cex = venue_cex.subscribe();
        let receiver_dex = venue_dex.subscribe();

        loop {
            select! {
                _ = self.token.cancelled() => {
                    warn!("Arbitraging cancelled");
                    break;
                }
                _ = receiver_cex.notified() => {
                    self.run_check();
                }
                _ = receiver_dex.notified() => {
                    self.run_check();
                }
            }
        }
    }
}

pub fn check_arbitrage<T, U, C>(
    forward_amounts: &[Decimal],
    reverse_amounts: &[Decimal],
    venue_a: &Arc<T>,
    venue_b: &Arc<U>,
    callback: &C,
) where
    T: TradingVenue,
    U: TradingVenue,
    C: ArbitrageCallback,
{
    for &amount in forward_amounts {
        let profit_ab = venue_a
            .try_quote_sell(amount)
            .and_then(|q| venue_b.try_quote_buy(q))
            .map(|returned| returned - amount);

        if let Some(profit) = profit_ab
            && profit > Decimal::ZERO
        {
            callback.call(ArbitrageOpportunity {
                amount,
                profit,
                direction: format!("{} → {}", venue_a.name(), venue_b.name()),
            });
        }
    }

    for &amount in reverse_amounts {
        let profit_ba = venue_b
            .try_quote_sell(amount)
            .and_then(|q| venue_a.try_quote_buy(q))
            .map(|returned| returned - amount);

        if let Some(profit) = profit_ba
            && profit > Decimal::ZERO
        {
            callback.call(ArbitrageOpportunity {
                amount,
                profit,
                direction: format!("{} → {}", venue_b.name(), venue_a.name()),
            });
        }
    }
}
