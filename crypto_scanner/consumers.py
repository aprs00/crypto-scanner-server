import websockets
import json

# import redis
from channels.generic.websocket import AsyncWebsocketConsumer


class BinanceConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        await self.accept()

        # Binance WebSocket endpoint for BTC/USDT ticker
        # binance_ws_endpoint = "wss://stream.binance.com:9443/ws/btcusdt@ticker"

        # Initialize Redis connection
        # self.redis_client = redis.StrictRedis(host="localhost", port=6379, db=0)

        # async with websockets.connect(binance_ws_endpoint) as websocket:
        #     while True:
        #         data = await websocket.recv()
        #         parsed_data = json.loads(data)

        #         # Update Redis with the received data
        #         # self.update_redis(parsed_data)
        #         print(parsed_data)

        #         await self.send(text_data=json.dumps(parsed_data))

    # def update_redis(self, data):
    #     # Update Redis with the necessary data
    #     # For demonstration purposes, we'll set the data under a key 'binance_data'
    #     self.redis_client.set('binance_data', json.dumps(data))
