def format_z_score_matrix_response(data: dict, x_axis: str, y_axis: str):
    return [
        {
            "symbol": symbol,
            "data": [
                round(data[symbol][x_axis], 2),
                round(data[symbol][y_axis], 2),
            ],
        }
        for symbol in list(data.keys())
    ]
