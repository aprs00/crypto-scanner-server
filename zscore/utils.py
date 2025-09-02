from core.constants import ticker_colors


def format_z_score_matrix_response(data: dict, x_axis: str, y_axis: str):
    return [
        {
            "type": "scatter",
            "name": symbol,
            "data": [
                [
                    round(data[symbol][x_axis], 2),
                    round(data[symbol][y_axis], 2),
                ]
            ],
            "color": ticker_colors[i],
            "symbolSize": 20,
            "emphasis": {"scale": 1.6},
        }
        for i, symbol in enumerate(list(data.keys()))
    ]
