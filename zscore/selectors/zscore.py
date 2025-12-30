from django.db.models import F, CharField, Func, Value
from zscore.models import ZScoreHistory
from django.utils import timezone


def get_zscore_history_data(hours, exchange: str, contract_type: str):
    last_hours = timezone.now() - timezone.timedelta(hours=hours)

    zscore_data = (
        ZScoreHistory.objects.select_related("symbol", "symbol__exchange", "symbol__contract_type")
        .filter(
            calculated_at__gte=last_hours,
            symbol__exchange__name=exchange,
            symbol__contract_type__name=contract_type,
        )
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
