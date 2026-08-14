from datetime import timedelta
from decimal import Decimal

from django.test import TestCase
from django.utils import timezone

from exchange_connections.db_utils import cleanup_old_kline_data
from exchange_connections.models import ContractType, Exchange, Kline1m, Symbol


class KlineCleanupTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.exchange = Exchange.objects.create(name="cleanup-test")
        contract_type = ContractType.objects.create(name="perpetual")
        cls.symbol = Symbol.objects.create(
            name="BTCUSDT",
            exchange=cls.exchange,
            contract_type=contract_type,
        )

    def _kline(self, start_time):
        return Kline1m(
            start_time=start_time,
            close_time=start_time + timedelta(minutes=1),
            symbol=self.symbol,
            open=Decimal("100"),
            close=Decimal("101"),
            high=Decimal("102"),
            low=Decimal("99"),
            base_volume=Decimal("1"),
            quote_volume=Decimal("100"),
            taker_buy_base_volume=Decimal("0.5"),
            taker_buy_quote_volume=Decimal("50"),
            number_of_trades=10,
            exchange=self.exchange,
        )

    def test_cleanup_is_bounded_and_preserves_retained_rows(self):
        now = timezone.now()
        expired = [
            self._kline(now - timedelta(days=31, minutes=offset))
            for offset in range(5)
        ]
        retained = [
            self._kline(now - timedelta(days=29, minutes=offset))
            for offset in range(2)
        ]
        Kline1m.objects.bulk_create([*expired, *retained])

        first_deleted = cleanup_old_kline_data(
            retention_days=30,
            batch_size=2,
            max_batches=2,
        )
        second_deleted = cleanup_old_kline_data(
            retention_days=30,
            batch_size=2,
            max_batches=2,
        )

        self.assertEqual(first_deleted, 4)
        self.assertEqual(second_deleted, 1)
        self.assertEqual(Kline1m.objects.count(), 2)
        self.assertFalse(
            Kline1m.objects.filter(start_time__lt=now - timedelta(days=30)).exists()
        )
