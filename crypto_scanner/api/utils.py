from crypto_scanner.constants import (
    tickers,
)


def get_min_length(query_tickers_data):
    min_length = min([len(data) for data in query_tickers_data.values()])
    for ticker in tickers:
        query_tickers_data[ticker] = query_tickers_data[ticker][:min_length]

    return query_tickers_data
