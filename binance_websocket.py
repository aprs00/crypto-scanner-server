from binance import ThreadedWebsocketManager

import redis
import dotenv
import time
import json

dotenv.load_dotenv()

from crypto_scanner.constants import socket_symbols, timeseries_agg_types

r = redis.Redis(host="redis", port=6379, decode_responses=True)


def create_keys(retention):
    for symbol in socket_symbols:
        if not r.exists(f"1s:volume:{symbol}"):
            r.execute_command(
                f"TS.CREATE 1s:volume:{symbol} LABELS value_type volume type binance_1s_data symbol {symbol} RETENTION {retention}"
            )
        if not r.exists(f"1s:price:{symbol}"):
            r.execute_command(
                f"TS.CREATE 1s:price:{symbol} LABELS value_type price type binance_1s_data symbol {symbol} RETENTION {retention}"
            )
        if not r.exists(f"1s:trades:{symbol}"):
            r.execute_command(
                f"TS.CREATE 1s:trades:{symbol} LABELS value_type trades type binance_1s_data symbol {symbol} RETENTION {retention}"
            )


def get_timestamps():
    now = int(time.time() * 1000)
    thirty_seconds_ago = now - 30000
    one_minute_ago = now - 60000
    five_minutes_ago = now - 300000
    fifteen_minutes_ago = now - 900000
    one_hour_ago = now - 3600000

    ago_timestamps = [
        {"30s": thirty_seconds_ago},
        {"1m": one_minute_ago},
        {"5m": five_minutes_ago},
        {"15m": fifteen_minutes_ago},
        {"1h": one_hour_ago},
    ]

    return ago_timestamps


def extract_timeseries(data):
    value_type = data[1][0][1]
    symbol = data[1][2][1]
    agg_value = data[2][0][1]

    agg_value = float(agg_value)
    agg_value = round(agg_value, 4)
    agg_value = str(agg_value)

    value_type_abbr = value_type[0]

    return value_type_abbr, value_type, symbol, agg_value


def add_to_response(
    response, symbol, value_type_abbr, agg_type, timestamp_key, agg_value
):
    if symbol not in response:
        response[symbol] = {}
        response[symbol]["symbol"] = symbol

    key_name = f"{value_type_abbr}_{agg_type}_{timestamp_key}"
    response[symbol][key_name] = agg_value


def format_binance_1s_data():
    ago_timestamps = get_timestamps()
    response = {}

    for timestamp_dict in ago_timestamps:
        for timestamp_key, timestamp_value in timestamp_dict.items():
            for agg_type in timeseries_agg_types:
                timeseries_data = r.execute_command(
                    f"TS.MREVRANGE {timestamp_value} + AGGREGATION {agg_type} {timestamp_value} WITHLABELS FILTER type=binance_1s_data"
                )

                for data in timeseries_data:
                    (
                        value_type_abbr,
                        value_type,
                        symbol,
                        agg_value,
                    ) = extract_timeseries(data)

                    if value_type == "price" and agg_type not in [
                        "twa",
                    ]:
                        continue

                    add_to_response(
                        response,
                        symbol,
                        value_type_abbr,
                        agg_type,
                        timestamp_key,
                        agg_value,
                    )

    for symbol, aggregation_values in response.items():
        r.execute_command(f"HSET aggregation:timestamps:{symbol} {aggregation_values}")

    response = list(response.values())
    response = json.dumps(response)

    r.set("formatted_binance_1s_data", response)


def subscribe_to_redis_channel(channel):
    pubsub = r.pubsub()
    pubsub.subscribe(channel)

    for message in pubsub.listen():
        print(message)

        if message["type"] == "message":
            if message["data"] == "updated":
                format_binance_1s_data()
            if message["data"] == "reconnect_apis":
                print("RECONNECTING APIS")
                main()


def main():
    twm = ThreadedWebsocketManager()
    # asyncio.run_coroutine_threadsafe(twm.stop_client(), twm._loop)
    # await twm.stop_client()
    # await asyncio.sleep(0.1)
    twm.start()

    count = 0
    retention = "3600000"
    redis_channel = "binance_1s_data"
    symbols_len = len(socket_symbols)
    agg_redis_command = []

    create_keys(retention=retention)

    def handle_socket_message(msg):
        nonlocal count
        count += 1
        data = msg["data"]["k"]

        symbol = data["s"]
        quote_volume = float(data["q"])
        price = float(data["c"])
        num_of_trades = data["n"]
        timestamp = data["t"]

        agg_redis_command.append(f"1s:volume:{symbol} {timestamp} {quote_volume}")
        agg_redis_command.append(f"1s:price:{symbol} {timestamp} {price}")
        agg_redis_command.append(f"1s:trades:{symbol} {timestamp} {num_of_trades}")

        if count == symbols_len:
            r.publish(redis_channel, "updated")
            r.execute_command(f"ts.madd {' '.join(agg_redis_command)}")
            agg_redis_command.clear()
            count = 0

    streams = [f"{symbol.lower()}@kline_1s" for symbol in socket_symbols]
    twm.start_multiplex_socket(callback=handle_socket_message, streams=streams)
    # asyncio.run_coroutine_threadsafe(
    #     twm.start_multiplex_socket(callback=handle_socket_message, streams=streams),
    # )

    subscribe_to_redis_channel(redis_channel)

    twm.join()


"""
ts.range 1s:BTCUSDT:sum - + +
TS.MRANGE - + FILTER symbol=BTCUSDT
TS.MRANGE - + FILTER aggregation_type=sum

ts.REVRANGE 1s:BTCUSDT - + AGGREGATION sum 15000

ts.madd 1s:trades:BTCUSDT 10000 10
"""
if __name__ == "__main__":
    main()

"""
SHIBUSDT:aggregation = {
    "symbol": "SHIBUSDT",
    "t_avg_30s": "0.5714",
    "v_avg_30s": "11354066.1071",
    "t_sum_30s": "16.0",
    "v_sum_30s": "317913851.0",
    "t_std.p_30s": "0.8207",
    "v_std.p_30s": "26142057.7546",
    "t_std.s_30s": "0.8357",
    "v_std.s_30s": "26621768.5516",
    "t_var.p_30s": "0.6735",
    "v_var.p_30s": "683407183642274.9",
    "t_var.s_30s": "0.6984",
    "v_var.s_30s": "708718560814211.0",
    "p_twa_30s": "0.0",
    "t_twa_30s": "0.5379",
    "v_twa_30s": "10365747.5404",
    "t_avg_1m": "0.6552",
    "v_avg_1m": "11480523.431",
    "t_sum_1m": "38.0",
    "v_sum_1m": "665870359.0",
    "t_std.p_1m": "1.0595",
    "v_std.p_1m": "26605204.5958",
    "t_std.s_1m": "1.0687",
    "v_std.s_1m": "26837568.8731",
    "t_var.p_1m": "1.1225",
    "v_var.p_1m": "707836911584437.1",
    "t_var.s_1m": "1.1422",
    "v_var.s_1m": "720255103015743.0",
    "p_twa_1m": "0.0",
    "t_twa_1m": "0.6394",
    "v_twa_1m": "11005210.695",
    "t_avg_5m": "0.4899",
    "v_avg_5m": "14509014.1611",
    "t_sum_5m": "146.0",
    "v_sum_5m": "4323686220.0",
    "t_std.p_5m": "1.6633",
    "v_std.p_5m": "115523247.5874",
    "t_std.s_5m": "1.6661",
    "v_std.s_5m": "115717567.7375",
    "t_var.p_5m": "2.7667",
    "v_var.p_5m": "1.3345620733151078e+16",
    "t_var.s_5m": "2.776",
    "v_var.s_5m": "1.3390555483094346e+16",
    "p_twa_5m": "0.0",
    "t_twa_5m": "0.4868",
    "v_twa_5m": "14418015.3407",
    "t_avg_15m": "0.5935",
    "v_avg_15m": "36905206.8497",
    "t_sum_15m": "533.0",
    "v_sum_15m": "33140875751.0",
    "t_std.p_15m": "3.0897",
    "v_std.p_15m": "298322772.6909",
    "t_std.s_15m": "3.0914",
    "v_std.s_15m": "298489015.5418",
    "t_var.p_15m": "9.5464",
    "v_var.p_15m": "8.89964767059903e+16",
    "t_var.s_15m": "9.557",
    "v_var.s_15m": "8.909569239908506e+16",
    "p_twa_15m": "0.0",
    "t_twa_15m": "0.5925",
    "v_twa_15m": "36878310.5362",
    "t_avg_1h": "0.5331",
    "v_avg_1h": "33807346.8849",
    "t_sum_1h": "1918.0",
    "v_sum_1h": "121638834092.0",
    "t_std.p_1h": "2.5326",
    "v_std.p_1h": "262432593.1114",
    "t_std.s_1h": "2.5329",
    "v_std.s_1h": "262469069.9471",
    "t_var.p_1h": "6.414",
    "v_var.p_1h": "6.887086592719227e+16",
    "t_var.s_1h": "6.4158",
    "v_var.s_1h": "6.889001267890959e+16",
    "p_twa_1h": "0.0",
    "t_twa_1h": "0.5328",
    "v_twa_1h": "33800521.1046",
}
"""
