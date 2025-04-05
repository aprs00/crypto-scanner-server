def get_min_length(data, symbols):
    """Get the minimum length across all ticker data arrays to ensure equal lengths."""

    min_length = float("inf")
    for symbol in symbols:
        if symbol in data and len(data[symbol]) > 0:
            min_length = min(min_length, len(data[symbol]))

    if min_length == float("inf"):
        min_length = 0

    return {symbol: data[symbol][:min_length] for symbol in symbols if symbol in data}
