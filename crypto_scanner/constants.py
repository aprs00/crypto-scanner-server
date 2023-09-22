stats_select_options_htf = {
    "1w": 7 * 24,
    "2w": 14 * 24,
    "1m": 30 * 24,
    "3m": 90 * 24,
    "6m": 180 * 24,
}

stats_select_options_ltf = {
    "1h": 1,
    "2h": 2,
    "4h": 4,
    "6h": 6,
    "8h": 8,
    "12h": 12,
    "1d": 24,
}

stats_select_options_all = {**stats_select_options_ltf, **stats_select_options_htf}

tickers = [
    "BTCUSDT",
    "ETHUSDT",
    "XRPUSDT",
    "BNBUSDT",
    "SOLUSDT",
    "DOTUSDT",
    "DOGEUSDT",
    "LTCUSDT",
    "LINKUSDT",
    "BCHUSDT",
    "SHIBUSDT",
]

ticker_colors = [
    "#F0A500",
    "#54BAB9",
    "#4DFF4D",
    "#B9B4C7",
    "#6243B6",
    "#00B3B3",
    "#9A3B3B",
    "#ABC4AA",
    "#B30000",
    "#6B7F4F",
    "#C08261",
]
