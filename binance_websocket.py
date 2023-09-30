# from binance import ThreadedWebsocketManager
from django.core.cache import cache
import numpy as np

from django_redis import get_redis_connection

import redis

from crypto_scanner.constants import tickers

# set core settings
import os
import json
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "core.settings")
django.setup()

# r = redis.Redis(host="redis", port=6379, db=0)
con = get_redis_connection("default")


def main():
    # twm = ThreadedWebsocketManager()
    # twm.start()
    # to_trim = 1 * 60 * 15

    # def extract_key_values(data, key):
    #     return [item[key] for item in data]

    # def decode_redis_data(data):
    #     parsed_data = []
    #     for item in data:
    #         item_str = item.decode("utf-8")

    #         # Find the index of the last '}'
    #         end_index = item_str.rfind("}")

    #         if end_index != -1:
    #             # Trim the string to extract the JSON-like portion
    #             json_str = item_str[: end_index + 1]

    #             try:
    #                 # Parse the extracted JSON string
    #                 parsed_item = json.loads(json_str)
    #                 parsed_data.append(parsed_item)
    #             except json.JSONDecodeError as e:
    #                 print("Error decoding JSON:", str(e))
    #         else:
    #             print("No valid JSON-like portion found.")

    #     return parsed_data

    # def handle_socket_message(msg):
    #     data = json.dumps(msg["data"]["k"])

    #     redis_data_length = con.llen("kline_1s:btcusdt")

    #     if redis_data_length == to_trim:
    #         con.lpop("kline_1s:btcusdt")

    #     con.lpush("kline_1s:btcusdt", data)
    #     con.ltrim("kline_1s:btcusdt", 0, to_trim)

    #     # get value from redis
    #     klines_900 = con.lrange("kline_1s:btcusdt", 0, to_trim)

    #     volume_list = []

    #     parsed_data = decode_redis_data(klines_900)
    #     # parsed_data = []

    #     for item in parsed_data:
    #         volume_list.append(float(item["v"]))

    #     average_volume_900s = np.average(volume_list)

    #     con.hset(
    #         "btcusdt:stats",
    #         mapping={"average_volume_15s": average_volume_900s},
    #     )

    #     print(con.hgetall("btcusdt:stats"))

    #     # print(redis_data_length)

    # # streams = [f"{ticker.lower()}@kline_1s" for ticker in tickers]
    # streams = ["btcusdt@kline_1s"]
    # twm.start_multiplex_socket(callback=handle_socket_message, streams=streams)

    # twm.join()

    con.set(
        "APOIDFHPOEHWAPOFIEAWOPIFHEWOPIAWEFHPIOEFWHPIOAEHFIOAFHOEIHOPAWEFHOPAEFW",
        "bbbCON",
    )
    print(
        con.get(
            "APOIDFHPOEHWAPOFIEAWOPIFHEWOPIAWEFHPIOEFWHPIOAEHFIOAFHOEIHOPAWEFHOPAEFW"
        )
    )

    # r.set.set("aaaR", "bbbR")
    # print(r.set.get("aaaR"))


if __name__ == "__main__":
    main()

# {
#    "stream":"btcusdt@kline_1s",
#    "data":{
#       "e":"kline",
#       "E":1695934650002,
#       "s":"BTCUSDT",
#       "k":{
#          "t":1695934649000,
#          "T":1695934649999,
#          "s":"BTCUSDT",
#          "i":"1s",
#          "f":3222854516,
#          "L":3222854516,
#          "o":"27080.36000000",
#          "c":"27080.36000000",
#          "h":"27080.36000000",
#          "l":"27080.36000000",
#          "v":"0.00384000",
#          "n":1,
#          "x":true,
#          "q":"103.98858240",
#          "V":"0.00000000",
#          "Q":"0.00000000",
#          "B":"0"
#       }
#    }
# }
