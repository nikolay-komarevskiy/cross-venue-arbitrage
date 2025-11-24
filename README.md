# An example of an arbitrage monitoring application between two venues

## Functional Requirements
- **Continuously stream price updates via WebSockets for a token pair (e.g., SOL/USDC):**
  - **CEX:** Binance [local order book](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams#how-to-manage-a-local-order-book-correctly) (bids/asks), with server-side timestamp
  - **DEX:** [Raydium AMM pool](https://github.com/raydium-io). Pool state is read via account subscriptions on a Solana node, emitted event does not contain the on-chain timestamp, a separate call is need to retrieve this timestamp

- **Detect arbitrage opportunities on every price update event on CEX/DEX:**
  - CEX data: `(bids, asks)`
  - DEX data: `(pool_state)`, see [PoolState](https://github.com/raydium-io/raydium-clmm/blob/d4ec101534724a20e1eb38a9b997f8b391c5100f/programs/amm/src/states/pool.rs#L59) definition
  - Check arbitrage in both directions:
    - `profit_cex_dex`: Buy on CEX → sell on DEX
    - `profit_dex_cex`: Buy on DEX → sell on CEX
  - Arbitrage condition:
    - `profit > 0` (fees ignored)

- **Callback on arbitrage opportunities**

- **Robustness:**
  - Automatic WebSocket reconnection
  - Graceful handling of all errors (stream failures, encoding/decoding/parsing, etc.)
  - Graceful termination of arbitraging/monitoring via cancellation

## Non-Functional Requirements
- Modular, flexible, and extensible architecture
- Clear interfaces supporting multiple implementations
- Abstractions for markets (CEX vs AMM, or other venue types)
- Aim for high throughput while minimizing latency too (benchmarking required)

![Diagram](diagram.svg)