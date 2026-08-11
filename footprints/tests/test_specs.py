from decimal import Decimal
from unittest.mock import patch

from django.test import SimpleTestCase

from footprints.specs import _hyperliquid_spec


class InstrumentSpecificationTests(SimpleTestCase):
    def test_hyperliquid_uses_lossless_decimal_quantum_not_mark_price_spacing(self):
        with patch(
            "footprints.specs._json_request",
            return_value={"universe": [{"name": "BTC", "szDecimals": 5}]},
        ):
            tick_size, quantity_step = _hyperliquid_spec("BTC")

        self.assertEqual(tick_size, Decimal("0.1"))
        self.assertEqual(quantity_step, Decimal("0.00001"))
