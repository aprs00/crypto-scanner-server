def get_min_length(symbols_data, symbols):
    min_length = min([len(data) for data in symbols_data.values()])
    for symbol in symbols:
        symbols_data[symbol] = symbols_data[symbol][:min_length]

    return symbols_data
