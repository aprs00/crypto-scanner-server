from datetime import timedelta
from django.utils import timezone

from correlations.models import CorrelationPairHistory


def cleanup_old_correlation_data(retention_hours) -> None:
    """
    Delete correlation records older than the specified retention period.
    """
    try:
        cutoff_time = timezone.now() - timedelta(hours=retention_hours)
        deleted_count, _ = CorrelationPairHistory.objects.filter(
            calculated_at__lt=cutoff_time
        ).delete()

        print(
            f"Cleaned up {deleted_count} correlation records older than {retention_hours} hours "
            f"(before {cutoff_time})"
        )

    except Exception as e:
        print(f"Error cleaning up old correlation data: {e}")
