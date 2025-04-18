stats_select_options_htf = {
    "1w": 7 * 24,
    "2w": 14 * 24,
    "1m": 30 * 24,
    "3M": 90 * 24,
    "6M": 180 * 24,
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

stats_select_options_all = stats_select_options_ltf | stats_select_options_htf
