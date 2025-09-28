import json
import logging
from channels.generic.websocket import AsyncWebsocketConsumer

logger = logging.getLogger(__name__)


class NotificationConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        self.notification_group_name = "crypto_notifications"

        await self.channel_layer.group_add(
            self.notification_group_name, self.channel_name
        )

        await self.accept()
        logger.info(f"WebSocket connected: {self.channel_name}")

    async def disconnect(self, code):
        await self.channel_layer.group_discard(
            self.notification_group_name, self.channel_name
        )
        logger.info(f"WebSocket disconnected: {self.channel_name}")

    async def receive(self, text_data=None, bytes_data=None):
        if text_data:
            try:
                data = json.loads(text_data)
                logger.info(f"Received WebSocket message: {data}")
            except json.JSONDecodeError:
                logger.error(f"Invalid JSON received: {text_data}")
        elif bytes_data:
            logger.info(f"Received binary data: {len(bytes_data)} bytes")

    async def send_notification(self, event):
        message = event["message"]

        await self.send(text_data=json.dumps(message))
        logger.info(f"Sent notification: {message}")
