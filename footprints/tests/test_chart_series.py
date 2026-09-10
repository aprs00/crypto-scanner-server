from django.db import IntegrityError, transaction
from django.test import TestCase
from django.utils import timezone

from footprints.constants import (
    TICK_MAX_TRADES_PER_BAR,
    TREV_MAX_THRESHOLD_TICKS,
    TREV_MIN_THRESHOLD_TICKS,
)
from footprints.models import ChartSeries, ChartSeriesStatus, ChartType, FootprintMarket


class ChartSeriesModelTests(TestCase):
    def setUp(self):
        self.market = FootprintMarket.objects.get(exchange="binance", symbol="BTCUSDT")

    def test_minimal_tick_series_defaults_to_queued(self):
        series = self._create()

        self.assertEqual(series.chart_type, ChartType.TICK)
        self.assertEqual(series.parameter_b, 0)
        self.assertEqual(series.status, ChartSeriesStatus.QUEUED)
        self.assertIsNone(series.checkpoint_state)

    def test_chart_configuration_is_unique(self):
        self._create()

        with self.assertRaises(IntegrityError), transaction.atomic():
            self._create()

    def test_trev_has_one_continuous_configuration_without_a_lookback(self):
        values = dict(
            chart_type=ChartType.TREV, parameter_a=3, parameter_b=2, lookback_days=None
        )
        self._create(**values)
        with self.assertRaises(IntegrityError), transaction.atomic():
            self._create(**values)
        with self.assertRaises(IntegrityError), transaction.atomic():
            self._create(**{**values, "parameter_a": 4, "lookback_days": 5})
        with self.assertRaises(IntegrityError), transaction.atomic():
            self._create(lookback_days=None)

    def test_database_rejects_invalid_chart_parameters(self):
        invalid = [
            {"parameter_a": 0},
            {"parameter_a": TICK_MAX_TRADES_PER_BAR + 1},
            {"parameter_b": 1},
            {
                "chart_type": ChartType.TREV,
                "parameter_a": TREV_MIN_THRESHOLD_TICKS - 1,
                "parameter_b": TREV_MIN_THRESHOLD_TICKS,
            },
            {
                "chart_type": ChartType.TREV,
                "parameter_a": TREV_MIN_THRESHOLD_TICKS,
                "parameter_b": TREV_MAX_THRESHOLD_TICKS + 1,
            },
        ]
        for overrides in invalid:
            with self.subTest(overrides=overrides):
                with self.assertRaises(IntegrityError), transaction.atomic():
                    self._create(**overrides)

    def _create(self, **overrides) -> ChartSeries:
        values = {
            "market": self.market,
            "chart_type": ChartType.TICK,
            "parameter_a": 500,
            "anchor_at": timezone.now(),
        }
        values.update(overrides)
        return ChartSeries.objects.create(**values)
