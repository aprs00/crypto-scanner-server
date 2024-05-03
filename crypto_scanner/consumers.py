#
# NOT USED IN PRODUCTION
#

import json
import asyncio
import urllib.parse
import redis

from channels.generic.websocket import AsyncWebsocketConsumer

from .constants import test_socket_symbols

r = redis.Redis(host="redis", port=6379, decode_responses=True)


class TableConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        query_string = self.scope["query_string"].decode("utf-8")
        params = urllib.parse.parse_qs(query_string)
        agg_keys = params.get("aggregations", [""])[0].split(",")

        self.redis_channel = "binance_1s_data"
        self.pubsub = r.pubsub()
        await self.accept()

        self.listen_task = asyncio.create_task(self.listen_to_redis())

        async def handle_socket_message():
            try:
                while True:
                    joined_agg_keys = " ".join(agg_keys)

                    response = []

                    for symbol in test_socket_symbols:
                        redis_response = r.execute_command(
                            f"HMGET aggregation:timestamps:{symbol} {joined_agg_keys}"
                        )

                        new_obj = {"symbol": symbol}
                        for i, key in enumerate(agg_keys):
                            new_obj[key] = redis_response[i]

                        response.append(new_obj)

                    await self.send(json.dumps(response))

                    await asyncio.sleep(1)
            except asyncio.CancelledError:
                # Task was cancelled (e.g., on disconnect), clean up here if needed
                pass
            finally:
                self.listen_task.cancel()

        self.handle_socket_message = handle_socket_message

    async def listen_to_redis(self):
        self.pubsub.subscribe(self.redis_channel)

        try:
            while True:
                message = self.pubsub.get_message()
                if (
                    message
                    and message["type"] == "message"
                    and message["data"] == "updated"
                ):
                    try:
                        await self.handle_socket_message()
                    except Exception as e:
                        print("Error sending response:", str(e))
                        pass
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            # Task was cancelled (e.g., on disconnect), clean up here if needed
            self.pubsub.unsubscribe(self.redis_channel)
            pass

    async def disconnect(self, close_code):
        print("DISCONNECTED")
        self.pubsub.unsubscribe(self.redis_channel)
        pass

    async def receive(self, text_data):
        print("RECEIVED")
        pass

    # async def listen_to_redis(self):
    #     pubsub = r.pubsub()
    #     pubsub.subscribe("binance_1s_data")

    #     print("fewpoihfopwhiefhoipweiopfhwephoifwehpiofwehpiowehopi")

    #     for message in pubsub.listen():
    #         print(message)

    #         if message["data"] == "updated":
    #             # await self.handle_socket_message()

    #             try:
    #                 while True:
    #                     # response = r.hget("formatted_binance_1s_data")
    #                     # response = json.loads(response)
    #                     # response = json.dumps(response)

    #                     # response = r.execute_command(
    #                     #     f"HGET aggregation:timestamps:BTCUSDT v_twa_1m v_twa_1h"
    #                     # )

    #                     # await self.send(response)
    #                     await self.send({"message": "fewpohifeopwhifwepohi"})

    #                     await asyncio.sleep(1)
    #             except asyncio.CancelledError:
    #                 # Task was cancelled (e.g., on disconnect), clean up here if needed
    #                 pass
    #             finally:
    #                 self.listen_task.cancel()
