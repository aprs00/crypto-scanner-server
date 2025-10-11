import logging
from datetime import timedelta
from django.utils import timezone

from correlations.models import CorrelationPairHistory

logger = logging.getLogger(__name__)


def cleanup_old_correlation_data(retention_hours) -> int:
    """
    Delete correlation records older than the specified retention period.
    """
    try:
        cutoff_time = timezone.now() - timedelta(hours=retention_hours)
        deleted_count, _ = CorrelationPairHistory.objects.filter(
            calculated_at__lt=cutoff_time
        ).delete()

        if deleted_count > 0:
            logger.info(
                f"Cleaned up {deleted_count} correlation records older than {retention_hours} hours "
                f"(before {cutoff_time})"
            )

        return deleted_count

    except Exception as e:
        logger.error(f"Error cleaning up old correlation data: {e}", exc_info=True)
        return 0
