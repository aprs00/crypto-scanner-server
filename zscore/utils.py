def format_z_score_matrix_response(
    data: dict,
    x_axis: str,
    y_axis: str,
    z_axis: str | None = None,
    symbols: list[str] | None = None,
):
    symbols_to_include = symbols if symbols else list(data.keys())
    return [
        {
            "symbol": symbol,
            "data": [
                round(data[symbol][x_axis], 3),
                round(data[symbol][y_axis], 3),
            ]
            + ([round(data[symbol][z_axis], 3)] if z_axis else []),
        }
        for symbol in symbols_to_include
        if symbol in data
    ]
