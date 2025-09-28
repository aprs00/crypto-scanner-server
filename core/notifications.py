import logging
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync

logger = logging.getLogger(__name__)


class NotificationService:
    def __init__(self):
        self.channel_layer = get_channel_layer()

    def send_correlation_update(self):
        """Send minimal notification that correlation data should be refetched"""
        if not self.channel_layer:
            logger.warning("Channel layer not configured - skipping notification")
            return

        message = {
            "type": "correlations_updated",
        }

        try:
            async_to_sync(self.channel_layer.group_send)(
                "crypto_notifications",
                {
                    "type": "send_notification",
                    "message": message,
                },
            )
            logger.info(f"Sent correlation update notification")
        except Exception as e:
            logger.error(f"Failed to send correlation notification: {str(e)}")

    def send_zscore_update(self):
        """Send minimal notification that zscore data should be refetched"""
        if not self.channel_layer:
            logger.warning("Channel layer not configured - skipping notification")
            return

        message = {
            "type": "zscore_updated",
        }

        try:
            async_to_sync(self.channel_layer.group_send)(
                "crypto_notifications",
                {
                    "type": "send_notification",
                    "message": message,
                },
            )
            logger.info(f"Sent zscore update notification")
        except Exception as e:
            logger.error(f"Failed to send zscore notification: {str(e)}")


# Global instance
notification_service = NotificationService()
