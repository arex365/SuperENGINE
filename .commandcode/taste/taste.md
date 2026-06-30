# Taste (Continuously Learned by [CommandCode][cmd])

[cmd]: https://commandcode.ai/

# engine
- Use market price (markPrice) instead of lastPrice for unrealized PnL calculations. Confidence: 0.65
- Calculate SL/TP as percentages from entry price: TP = entry + 0.5%, SL = entry - 1.5% (don't use level-based SL/TP). Confidence: 0.85
