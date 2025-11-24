use arbitrage_monitor::arbitrage::OrderBookArbitrageDetector;
use arbitrage_monitor::binance::BinanceMonitor;
use arbitrage_monitor::raydium::RaydiumAmmMonitor;
use arbitrage_monitor::task::TasksRunner;
use std::sync::Arc;
use tokio::{signal, spawn};
use tokio_util::sync::CancellationToken;
use tracing::{Level, error, info};
use url::Url;

pub const TOKEN_PAIR: &str = "solusdc"; // SOLANA/USDC

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    info!("starting arbitrage monitoring for {TOKEN_PAIR} token pair...");

    // Cancellation token used for all tasks and graceful shutdown.
    let token = CancellationToken::new();

    let cex_monitor: Arc<BinanceMonitor> = {
        let binance_ws_url = Url::parse(&format!(
            "wss://stream.binance.com:9443/ws/{}@depth",
            TOKEN_PAIR
        ))?;
        let binance_rest_url = Url::parse(&format!(
            "https://api.binance.com/api/v3/depth?symbol={}&limit=1000",
            TOKEN_PAIR.to_uppercase(),
        ))?;
        Arc::new(BinanceMonitor::new(
            binance_rest_url,
            binance_ws_url,
            token.child_token(),
        ))
    };

    let dex_monitor: Arc<RaydiumAmmMonitor> = {
        let raydium_ws_url = Url::parse("wss://api.mainnet-beta.solana.com")?;
        let pool_id = "3ucNos4NbumPLZNWztqGHNFFgkHeRMBQAVemeeomsUxv".to_string();
        let commitment = "confirmed".to_string();
        Arc::new(RaydiumAmmMonitor::new(
            raydium_ws_url,
            pool_id,
            commitment,
            token.child_token(),
        ))
    };

    let callback = Arc::new(|arb_op| {
        info!("Arbitrage opp for pair {TOKEN_PAIR} detected: {:?}", arb_op);
        // run real execution model ...
    });

    // Use either OrderBookArbitrageDetector or SimpleArbitrageDetector
    let orderbook_depth = 100;
    let arb_detector = Arc::new(OrderBookArbitrageDetector::new(
        cex_monitor.clone(),
        dex_monitor.clone(),
        orderbook_depth,
        callback,
        token.child_token(),
    ));

    let mut task_runner = TasksRunner::new(token.clone());

    task_runner.add(arb_detector); // if arbitrage is CPU-intensive run it on the blocking thread (spawn_blocking)
    task_runner.add(cex_monitor);
    task_runner.add(dex_monitor);

    info!("starting tasks runner");
    task_runner.start();

    await_ctrl_c(task_runner, token).await;

    Ok(())
}

async fn await_ctrl_c(tracker: TasksRunner, token: CancellationToken) {
    let token_cloned = token.clone();

    // Spawn Ctrl-C listener
    spawn(async move {
        match signal::ctrl_c().await {
            Ok(_) => {
                info!("ctrl-c received, cancelling tasks");
            }
            Err(e) => {
                error!(error = %e, "failed to install Ctrl-C handler; cancelling tasks");
            }
        }

        token_cloned.cancel();
    });

    token.cancelled().await;

    // Shutdown running tasks gracefully
    tracker.shutdown().await;

    info!("all tasks shutdown successfully");
}
