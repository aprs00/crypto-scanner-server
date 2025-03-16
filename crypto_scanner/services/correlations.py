import redis

from crypto_scanner.constants import (
    tickers,
)
from crypto_scanner.formulas.spearman import calculate_spearman_correlation
from crypto_scanner.formulas.pearson import calculate_pearson_correlation
from crypto_scanner.selectors.correlations import get_tickers_data, extract_timeseries


r = redis.Redis(host="redis", port=6379, decode_responses=True)


def calculate_correlations(data, symbols, type):
    correlations = {}
    rank_cache = {}
    pearson_cache = {}

    for symbol1 in symbols:
        for symbol2 in symbols:
            if type == "pearson":
                correlations[f"{symbol1} - {symbol2}"] = calculate_pearson_correlation(
                    data[symbol1], data[symbol2], symbol1, symbol2, pearson_cache
                )
            elif type == "spearman":
                correlations[f"{symbol1} - {symbol2}"] = calculate_spearman_correlation(
                    data[symbol1], data[symbol2], rank_cache
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


def convert_array_to_matrix(symbols, correlations, is_matrix_upper_triangle=True):
    return [
        [
            i,
            j,
            round(
                correlations[f"{symbols[i]} - {symbols[j]}"],
                2,
            ),
        ]
        for i in range(len(symbols))
        for j in (range(i + 1, len(symbols)) if is_matrix_upper_triangle else range(i))
    ]


def format_large_pearson_response(
    tf, data_type, correlation_type, symbols, is_matrix_upper_triangle=True
):
    data = extract_timeseries(tf, symbols, data_type)
    correlations = calculate_correlations(data, symbols, correlation_type)

    return convert_array_to_matrix(symbols, correlations, is_matrix_upper_triangle)
