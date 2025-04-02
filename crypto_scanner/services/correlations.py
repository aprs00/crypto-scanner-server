import redis
from itertools import product

from crypto_scanner.constants import (
    tickers,
)
from crypto_scanner.formulas.spearman import calculate_spearman_correlation
from crypto_scanner.formulas.pearson import calculate_pearson_correlation
from crypto_scanner.selectors.correlations import get_tickers_data


r = redis.Redis(host="redis", port=6379, decode_responses=True)

correlation_functions = {
    "pearson": calculate_pearson_correlation,
    "spearman": calculate_spearman_correlation,
}


def calculate_correlations(data, symbols, type):
    correlations = {}
    rank_cache = {}
    pearson_cache = {}

    for symbol1, symbol2 in product(symbols, repeat=2):
        correlations[f"{symbol1} - {symbol2}"] = correlation_functions[type](
            data[symbol1],
            data[symbol2],
            symbol1,
            symbol2,
            pearson_cache if type == "pearson" else rank_cache,
        )

    return correlations


def calculate_pearson_correlation_high_tf(duration):
    query_tickers_data = get_tickers_data(duration)
    correlations = calculate_correlations(query_tickers_data, tickers, "pearson")
    formatted_tickers = [ticker[:-4] for ticker in tickers]

    response = {
        "xAxis": formatted_tickers,
        "yAxis": formatted_tickers,
        "data": [
            [i, j, round(correlations[f"{tickers[i]} - {tickers[j]}"], 2)]
            for i in range(len(tickers))
            for j in range(i + 1, len(tickers))
        ],
    }

    return response
