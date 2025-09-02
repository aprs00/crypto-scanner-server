from django.db.models import F, CharField, Func, Value
from zscore.models import ZScoreHistory
from django.utils import timezone


def get_zscore_history_data(hours):
    last_hours = timezone.now() - timezone.timedelta(hours=hours)

    zscore_data = (
        ZScoreHistory.objects.select_related("symbol")
        .filter(calculated_at__gte=last_hours)
        .annotate(
            time=Func(
                F("calculated_at"),
                Value("HH24:MI:SS"),
                function="to_char",
                output_field=CharField(),
            )
        )
        .values(
            "price",
            "volume",
            "trades",
            "time",
            "hours",
            "symbol__name",
        )
        .order_by("calculated_at")
    )

    return list(zscore_data)
