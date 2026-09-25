from datetime import datetime, timedelta, timezone
from decimal import Decimal

import numpy as np
from django.test import TransactionTestCase

from core.constants import Exchange as ExchangeName
from correlations.selectors import get_historical_correlation_matrices
from correlations.services.incremental_correlations import (
    CorrelationCalculator,
    CorrelationTracker,
)
from exchange_connections.models import ContractType, Exchange, Kline1m, Symbol
from exchange_connections.selectors import get_historical_kline_data


class HistoricalCorrelationMatricesTests(TransactionTestCase):
    def setUp(self):
        self.bybit = Exchange.objects.create(name="bybit")
        self.binance = Exchange.objects.create(name="binance")
        self.perpetual = ContractType.objects.create(name="perpetual")
        self.spot = ContractType.objects.create(name="spot")
        self.end = datetime(2026, 9, 25, 12, 2, 30, tzinfo=timezone.utc)
        self.start = self.end.replace(second=0) - timedelta(hours=1)
        self.symbols = [
            Symbol.objects.create(
                name=name, exchange=self.bybit, contract_type=self.perpetual
            )
            for name in ("AUSDT", "BUSDT", "CUSDT")
        ]

    def add_candle(self, symbol, minute, price, volume, exchange=None):
        at = self.start + timedelta(minutes=minute)
        Kline1m.objects.create(
            start_time=at,
            close_time=at + timedelta(minutes=1),
            symbol=symbol,
            exchange=exchange or symbol.exchange,
            open=Decimal(str(price)),
            high=Decimal(str(price)),
            low=Decimal(str(price)),
            close=Decimal(str(price)),
            base_volume=Decimal(str(volume)),
            number_of_trades=0,
        )

    def test_streamed_result_matches_existing_selector_with_gaps_and_boundaries(self):
        a, b, _ = self.symbols
        self.add_candle(a, -1, 999, 999)
        self.add_candle(a, 0, 10, 1)
        self.add_candle(a, 2, 20, 2)
        self.add_candle(a, 3, 30, 3)
        self.add_candle(a, 4, 25, 4)
        self.add_candle(b, 1, 4, 8)
        self.add_candle(b, 2, 8, 16)
        self.add_candle(b, 3, 12, 24)
        self.add_candle(b, 4, 13, 5)
        self.add_candle(a, 60, 999, 999)

        other_exchange = Symbol.objects.create(
            name="AUSDT", exchange=self.binance, contract_type=self.perpetual
        )
        other_contract = Symbol.objects.create(
            name="AUSDT", exchange=self.bybit, contract_type=self.spot
        )
        self.add_candle(other_exchange, 2, 777, 777)
        self.add_candle(other_contract, 2, 888, 888)

        names = [symbol.name for symbol in self.symbols]
        matrices = get_historical_correlation_matrices(
            hours=1,
            symbols=names,
            symbol_to_idx={name: i for i, name in enumerate(names)},
            exchange="bybit",
            contract_type="perpetual",
            data_types=("price", "volume"),
            end_time=self.end,
        )
        old = get_historical_kline_data(
            hours=1,
            symbols=names,
            exchange="bybit",
            contract_type="perpetual",
            end_time=self.end,
        )

        self.assertEqual(set(matrices), {"price", "volume"})
        for data_type in matrices:
            self.assertEqual(matrices[data_type].shape, (3, 60))
            for i, name in enumerate(names):
                np.testing.assert_array_equal(
                    matrices[data_type][i], old[name][data_type], strict=True
                )

        self.assertEqual(matrices["price"][0, 2], 20.0)
        self.assertEqual(matrices["volume"][1, 2], 16.0)
        self.assertTrue(np.isnan(matrices["price"][2]).all())
        self.assertTrue(np.isnan(matrices["price"][0, 1]))

        tracker = CorrelationTracker(window_size=60, n_symbols=3)
        tracker.initialize_from_matrix(matrices["price"])
        expected = float(np.corrcoef([20, 30, 25], [8, 12, 13])[0, 1])
        self.assertAlmostEqual(tracker.get_pair_correlation(0, 1), expected)
        self.assertEqual(tracker.get_pair_correlation(0, 2), 0.0)

        calculator = CorrelationCalculator(exchange=ExchangeName.BYBIT)
        calculator.symbols = names
        calculator._rebuild_indices()
        calculator.hours_options = [1]
        calculator._init_trackers(end_time=self.end)
        self.assertEqual(set(calculator.trackers), {(1, "price"), (1, "volume")})
        self.assertAlmostEqual(
            calculator.trackers[(1, "price")].get_pair_correlation(0, 1), expected
        )
        volume_expected = float(np.corrcoef([2, 3, 4], [16, 24, 5])[0, 1])
        self.assertAlmostEqual(
            calculator.trackers[(1, "volume")].get_pair_correlation(0, 1),
            volume_expected,
        )

    def test_bybit_uses_only_supported_correlation_types(self):
        calculator = CorrelationCalculator(exchange=ExchangeName.BYBIT)
        self.assertEqual(calculator._data_types, ("price", "volume"))

    def test_large_symbol_set_keeps_every_symbol(self):
        extra = [
            Symbol.objects.create(
                name=f"EXTRA{i}USDT",
                exchange=self.bybit,
                contract_type=self.perpetual,
            )
            for i in range(62)
        ]
        all_symbols = [*self.symbols, *extra]
        for i, symbol in enumerate(all_symbols):
            self.add_candle(symbol, 0, i + 1, (i + 1) * 2)

        names = [symbol.name for symbol in all_symbols]
        matrices = get_historical_correlation_matrices(
            hours=1,
            symbols=names,
            symbol_to_idx={name: i for i, name in enumerate(names)},
            exchange="bybit",
            contract_type="perpetual",
            data_types=("price", "volume"),
            end_time=self.end,
        )

        np.testing.assert_array_equal(matrices["price"][:, 0], range(1, 66))
        np.testing.assert_array_equal(matrices["volume"][:, 0], range(2, 131, 2))
        self.assertTrue(np.isnan(matrices["price"][:, 1:]).all())
