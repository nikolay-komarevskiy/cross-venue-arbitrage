use std::sync::Arc;

use rust_decimal::Decimal;
use tokio::sync::Notify;

pub trait TradingVenue: Send + Sync {
    /// Name of the venue.
    fn name(&self) -> &'static str;

    /// Given `base_in` (amount of base token to sell), returns amount of quote token received.
    fn try_quote_sell(&self, base_in: Decimal) -> Option<Decimal>;

    /// Given `quote_out` (amount of quote token you will pay), returns amount of base token bought.
    fn try_quote_buy(&self, quote_out: Decimal) -> Option<Decimal>;

    /// Subscribe to venue updates (orderbook, liquidity, etc.).
    fn subscribe(&self) -> Arc<Notify>;
}
