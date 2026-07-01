# Taste (Continuously Learned by [CommandCode][cmd])

[cmd]: https://commandcode.ai/

# engine
- Use market price (markPrice) instead of lastPrice for unrealized PnL calculations. Confidence: 0.65
- Calculate SL/TP as percentages from entry price: TP = entry + 0.5%, SL = entry - 1.5% (don't use level-based SL/TP). Confidence: 0.85
- Use bid/ask prices for trade execution (entries, exits, SL, TP) instead of mark price to reduce slippage on actual exchange. Confidence: 0.70

# trading
- Use limit orders (not market orders) to open positions. Confidence: 0.65
- Attach TP and SL to the opening limit order so they are placed together as a single conditional order. Confidence: 0.65
- Cancel only stale position-opening limit orders (>3 min) — not TP/SL orders (TP/SL are linked to the entry order and auto-cancel when the entry order is cancelled). Confidence: 0.70
- Keep newly opened orders (<3 min) untouched — only cancel orders that have been open for more than 3 minutes. Confidence: 0.65
