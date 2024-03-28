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
    # one_hour_ago = now - 3600000

    ago_timestamps = [
        {"30s": thirty_seconds_ago},
        {"1m": one_minute_ago},
        {"5m": five_minutes_ago},
        {"15m": fifteen_minutes_ago},
        # {"1h": one_hour_ago},
    ]

    return ago_timestamps


def extract_timeseries(data):
    value_type = data[1][0][1]
    symbol = data[1][2][1]
    agg_value = data[2][0][1]

    agg_value = float(agg_value)
    agg_value = round(agg_value, 2)
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

                    add_to_response(
                        response,
                        symbol,
                        value_type_abbr,
                        agg_type,
                        timestamp_key,
                        agg_value,
                    )

    for symbol, aggregation_values in response.items():
        key_values_agg = ""
        for agg_key, agg_value in aggregation_values.items():
            if "." in agg_key:
                agg_key = agg_key.replace(".", "_")
            key_values_agg += f"{agg_key} {agg_value} "

        r.execute_command(f"HMSET aggregation:timestamps:{symbol} {key_values_agg}")


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
    retention = "900000"
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
