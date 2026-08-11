from __future__ import annotations

import json
import logging
import threading

from django.core.exceptions import ValidationError
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.utils import timezone
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET, require_POST

from footprints.api import _error, market_by_key
from footprints.chart_serialization import serialize_active_bar, serialize_chart_bar
from footprints.chart_service import (
    ChartDensityLimitError,
    active_bar_for_series,
    request_chart_series,
    series_lease_cutoff,
)
from footprints.constants import (
    CHART_MAX_LEVELS_PER_BAR,
    CHART_REQUEST_MAX_BYTES,
    CHART_SNAPSHOT_BARS,
    CHART_SNAPSHOT_MAX_LEVELS,
    TICK_MAX_TRADES_PER_BAR,
    TREV_MAX_THRESHOLD_TICKS,
    TREV_MIN_THRESHOLD_TICKS,
)
from footprints.models import ChartSeries, ChartSeriesStatus, ChartType, InstrumentSpec
from footprints.storage.charts import ClickHouseChartStore
from footprints.storage.clickhouse import (
    ClickHouseAccess,
    ClickHouseSettings,
    create_clickhouse_client,
)


logger = logging.getLogger(__name__)

_STORE_LOCK = threading.Lock()
_BAR_STORE: ClickHouseChartStore | None = None
_BAR_CLIENT = None


@csrf_exempt
@require_POST
def chart_series(request: HttpRequest) -> JsonResponse:
    content_length = request.META.get("CONTENT_LENGTH")
    if content_length:
        try:
            declared_length = int(content_length)
        except (TypeError, ValueError):
            return _error("Content-Length must be an integer", "INVALID_CONTENT_LENGTH")
        if declared_length < 0:
            return _error("Content-Length cannot be negative", "INVALID_CONTENT_LENGTH")
        if declared_length > CHART_REQUEST_MAX_BYTES:
            return _error("Request body is too large", "REQUEST_TOO_LARGE", 413)
    if len(request.body) > CHART_REQUEST_MAX_BYTES:
        return _error("Request body is too large", "REQUEST_TOO_LARGE", 413)
    try:
        payload = json.loads(request.body or b"{}")
    except (TypeError, ValueError, json.JSONDecodeError):
        return _error("Request body must be valid JSON", "INVALID_JSON")
    if not isinstance(payload, dict):
        return _error("Request body must be a JSON object", "INVALID_JSON")

    market = market_by_key(str(payload.get("marketKey", "")))
    if market is None:
        return _error("Unknown or disabled chart market", "UNKNOWN_MARKET", 404)
    chart_type = str(payload.get("chartType") or "").lower()
    if chart_type not in (ChartType.TICK, ChartType.TREV):
        return _error("chartType must be tick or trev", "INVALID_CHART_TYPE")
    parameters = _chart_parameters(chart_type, payload)
    if parameters is None:
        if chart_type == ChartType.TICK:
            message = f"tradesPerBar must be between 1 and {TICK_MAX_TRADES_PER_BAR}"
        else:
            message = (
                "continuationTicks and reversalTicks must be between "
                f"{TREV_MIN_THRESHOLD_TICKS} and {TREV_MAX_THRESHOLD_TICKS}"
            )
        return _error(message, "INVALID_CHART_PARAMETERS")
    parameter_a, parameter_b = parameters

    now = timezone.now()
    series = request_chart_series(
        market=market,
        chart_type=chart_type,
        parameter_a=parameter_a,
        parameter_b=parameter_b,
        now=now,
    )
    if series is None:
        response = _error(
            "This market already has the maximum number of live chart series",
            "SERIES_CAPACITY_REACHED",
            429,
        )
        response["Retry-After"] = "60"
        return response
    return _series_response(series)


@require_GET
def chart_bars(request: HttpRequest) -> HttpResponse:
    series_key = request.GET.get("seriesKey", "")
    try:
        series = ChartSeries.objects.get(
            public_key=series_key,
            last_requested_at__gte=series_lease_cutoff(timezone.now()),
        )
    except (ChartSeries.DoesNotExist, ValidationError, ValueError):
        return _error("Unknown chart series", "UNKNOWN_CHART_SERIES", 404)
    if series.status != ChartSeriesStatus.READY:
        return _error(
            "The chart series is not ready",
            "CHART_SERIES_NOT_READY",
            409,
            seriesKey=str(series.public_key),
            status=series.status,
            errorCode=series.error_code or None,
        )

    try:
        active_bar = active_bar_for_series(series)
        active_level_count = len(active_bar.levels) if active_bar is not None else 0
        newest = _chart_bar_store().read_detailed(
            series_id=series.pk,
            limit=CHART_SNAPSHOT_BARS,
            max_levels=CHART_SNAPSHOT_MAX_LEVELS - active_level_count,
        )
        if any(len(bar.levels) > CHART_MAX_LEVELS_PER_BAR for bar in newest):
            raise ChartDensityLimitError(
                f"closed chart bar exceeds {CHART_MAX_LEVELS_PER_BAR} native levels"
            )
        if active_bar is not None and any(
            bar.bar_index >= active_bar.bar_index for bar in newest
        ):
            active_bar = None
        selected = list(reversed(newest))
        specs = {
            spec.pk: spec
            for spec in InstrumentSpec.objects.filter(
                pk__in={bar.instrument_spec_id for bar in selected}
            )
        }
        bars = [serialize_chart_bar(bar, specs) for bar in selected]
        active = None
        if active_bar is not None:
            if active_bar.spec_id not in specs:
                spec = InstrumentSpec.objects.get(pk=active_bar.spec_id)
                specs[spec.pk] = spec
            active = serialize_active_bar(active_bar, specs)
    except ChartDensityLimitError as exc:
        ChartSeries.objects.filter(pk=series.pk).update(
            status=ChartSeriesStatus.FAILED,
            error_code="CHART_BAR_TOO_DENSE",
            checkpoint_state=None,
        )
        logger.warning("Oversized chart data for %s: %s", series.public_key, exc)
        return _error("Chart footprint is too dense", "CHART_BAR_TOO_DENSE", 422)
    except (KeyError, OverflowError, ValueError) as exc:
        logger.warning("Invalid cached chart data for %s: %s", series.public_key, exc)
        return _error("Chart history is invalid", "INVALID_CHART_DATA", 422)
    except Exception:
        logger.exception("ClickHouse chart read failed for %s", series.public_key)
        return _error(
            "Chart storage is temporarily unavailable", "CHART_UNAVAILABLE", 503
        )

    return JsonResponse(
        {
            "seriesKey": str(series.public_key),
            "bars": bars,
            "activeBar": active,
        }
    )


def _chart_parameters(chart_type: str, payload: dict) -> tuple[int, int] | None:
    if chart_type == ChartType.TICK:
        trades_per_bar = _strict_int(payload.get("tradesPerBar"))
        if trades_per_bar is None or not 1 <= trades_per_bar <= TICK_MAX_TRADES_PER_BAR:
            return None
        return trades_per_bar, 0
    continuation = _strict_int(payload.get("continuationTicks"))
    reversal = _strict_int(payload.get("reversalTicks"))
    if continuation is None or reversal is None:
        return None
    if not (
        TREV_MIN_THRESHOLD_TICKS <= continuation <= TREV_MAX_THRESHOLD_TICKS
        and TREV_MIN_THRESHOLD_TICKS <= reversal <= TREV_MAX_THRESHOLD_TICKS
    ):
        return None
    return continuation, reversal


def _series_response(series: ChartSeries) -> JsonResponse:
    status = (
        202
        if series.status in (ChartSeriesStatus.QUEUED, ChartSeriesStatus.BUILDING)
        else 200
    )
    response = JsonResponse(
        {
            "seriesKey": str(series.public_key),
            "status": series.status,
            "errorCode": series.error_code or None,
        },
        status=status,
    )
    response["Cache-Control"] = "no-store"
    if status == 202:
        response["Retry-After"] = "1"
    return response


def _chart_bar_store() -> ClickHouseChartStore:
    global _BAR_CLIENT, _BAR_STORE
    if _BAR_STORE is not None:
        return _BAR_STORE
    with _STORE_LOCK:
        if _BAR_STORE is None:
            configuration = ClickHouseSettings.from_environment()
            _BAR_CLIENT = create_clickhouse_client(configuration, role="chart-api")
            _BAR_STORE = ClickHouseChartStore(
                ClickHouseAccess(_BAR_CLIENT),
                database=configuration.database,
            )
    return _BAR_STORE


def _strict_int(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None
