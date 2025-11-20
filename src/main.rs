use arbitrage_monitor::arbitrage::{OrderBookArbitrageDetector, TOKEN_PAIR};
use arbitrage_monitor::binance::BinanceMonitor;
use arbitrage_monitor::raydium::RaydiumAmmMonitor;
use arbitrage_monitor::task::TaskRunner;
use std::sync::Arc;
use tokio::spawn;
use tokio_util::sync::CancellationToken;
use tracing::{Level, info};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    info!("starting arbitrage monitoring for {TOKEN_PAIR} token pair...");

    // Cancellation token used for all tasks and graceful shutdown.
    let token = CancellationToken::new();

    let cex_monitor = Arc::new(BinanceMonitor::new(TOKEN_PAIR, token.clone()));
    let dex_monitor = Arc::new(RaydiumAmmMonitor::new(token.clone()));

    let callback = Arc::new(|arb_op| {
        info!("Arbitrage opp for pair {TOKEN_PAIR} detected: {:?}", arb_op);
        // run real execution model ...
    });

    // Use either OrderBookArbitrageDetector or SimpleArbitrageDetector
    let orderbook_depth = 10;
    let arb_detector = Arc::new(OrderBookArbitrageDetector::new(
        cex_monitor.clone(),
        dex_monitor.clone(),
        orderbook_depth,
        callback,
        token.clone(),
    ));

    let mut task_runner = TaskRunner::new(token.clone());

    task_runner.add(arb_detector); // if arbitrage is CPU-intensive run it on the blocking thread (spawn_blocking)
    task_runner.add(cex_monitor);
    task_runner.add(dex_monitor);

    info!("starting task runner");
    task_runner.start();

    // ctrl-c to cancel
    {
        let token = token.clone();
        spawn(async move {
            if let Err(e) = tokio::signal::ctrl_c().await {
                tracing::error!(msg = "failed to install Ctrl-C handler", %e);
                token.cancel();
                return;
            }
            tracing::info!(msg = "ctrl-c received, cancelling");
            token.cancel();
        });
    }

    token.cancelled().await;
    info!("shutdown");

    Ok(())
}
