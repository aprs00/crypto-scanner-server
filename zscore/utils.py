from exchange_connections.constants import ticker_colors
from zscore.selectors.zscore import get_tickers_data_z_score
from zscore.formulas import calculate_z_scores
from core.redis_config import get_redis_connection

r = get_redis_connection()


def calculate_z_score_history(duration):
    tickers_data_z_score = get_tickers_data_z_score(duration)
    z_scores = {}
    start_time_values = None

    for ticker, data in tickers_data_z_score.items():
        volume_values, price_values, trades_values, start_time_values = zip(*data)

        z_scores[ticker] = {
            "volume": calculate_z_scores(volume_values),
            "price": calculate_z_scores(price_values),
            "trades": calculate_z_scores(trades_values),
        }

    return {"data": z_scores, "start_time_values": start_time_values}


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


def format_z_score_history_response(data, data_type):
    return {
        "data": [
            {
                "name": ticker,
                "type": "line",
                "data": [float(item) for item in data[data_type]],
                "emphasis": {"focus": "self"},
            }
            for ticker, data in data["data"].items()
        ],
        "time": [item.strftime("%H:%M") for item in data["start_time_values"]],
    }
