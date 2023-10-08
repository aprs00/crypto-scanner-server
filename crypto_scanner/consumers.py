import json
import asyncio
import time
import numpy as np
import urllib.parse

import redis
from channels.generic.websocket import AsyncWebsocketConsumer
from .constants import timeseries_agg_types

r = redis.Redis(host="redis", port=6379, decode_responses=True)


class TableConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        # query_string = self.scope["query_string"].decode("utf-8")
        # params = urllib.parse.parse_qs(query_string)
        # param1 = params.get("param1", [""])[0]
        # param2 = params.get("param2", [""])[0]
        # jsonObj = params.get("jsonObj", [""])[0]

        # convert jsonObj to dict
        # jsonObj = json.loads(jsonObj)

        # print(param1)
        # print(param2)
        # print(jsonObj["name"])

        await self.accept()

        listen_task = asyncio.create_task(self.listen_to_redis())
        self.listen_task = listen_task

        async def handle_socket_message():
            try:
                while True:
                    response = r.get("formatted_binance_1s_data")
                    response = json.loads(response)
                    response = json.dumps(response)

                    await self.send(response)

                    await asyncio.sleep(1)
            except asyncio.CancelledError:
                # Task was cancelled (e.g., on disconnect), clean up here if needed
                pass
            finally:
                self.listen_task.cancel()

        self.handle_socket_message = handle_socket_message

    async def disconnect(self, close_code):
        print("DISCONNECTED")
        pass

    async def receive(self, text_data):
        print("RECEIVED")
        pass

    async def listen_to_redis(self):
        pubsub = r.pubsub()
        pubsub.subscribe("binance_1s_data")

        for message in pubsub.listen():
            print(message)

            if message["type"] == "message":
                await self.handle_socket_message()
