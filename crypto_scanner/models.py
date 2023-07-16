from django.db import models
from pygments.lexers import get_all_lexers
from pygments.styles import get_all_styles

LEXERS = [item for item in get_all_lexers() if item[1]]
LANGUAGE_CHOICES = sorted([(item[1][0], item[0]) for item in LEXERS])
STYLE_CHOICES = sorted([(item, item) for item in get_all_styles()])


class Snippet(models.Model):
    created = models.DateTimeField(auto_now_add=True)
    title = models.CharField(max_length=100, blank=True, default="")
    code = models.TextField()
    linenos = models.BooleanField(default=False)
    language = models.CharField(
        choices=LANGUAGE_CHOICES, default="python", max_length=100
    )
    style = models.CharField(choices=STYLE_CHOICES, default="friendly", max_length=100)

    class Meta:
        ordering = ["created"]


class BtcPrice(models.Model):
    id = models.AutoField(primary_key=True)
    created = models.DateTimeField(auto_now_add=True)
    price = models.DecimalField(max_digits=10, decimal_places=2)

    class Meta:
        ordering = ["created"]


attrs = {
    "start_time": models.DateTimeField(),
    "end_time": models.DateTimeField(),
    "open": models.DecimalField(max_digits=24, decimal_places=10),
    "close": models.DecimalField(max_digits=24, decimal_places=10),
    "high": models.DecimalField(max_digits=24, decimal_places=10),
    "low": models.DecimalField(max_digits=24, decimal_places=10),
    "base_volume": models.DecimalField(max_digits=24, decimal_places=10),
    "number_of_trades": models.IntegerField(),
    "quote_asset_volume": models.DecimalField(max_digits=24, decimal_places=10),
    "taker_buy_base_asset_volume": models.DecimalField(
        max_digits=24, decimal_places=10
    ),
    "taker_buy_quote_asset_volume": models.DecimalField(
        max_digits=24, decimal_places=10
    ),
    "__module__": "crypto_scanner.models",
}


BtcKline1m = type("binance_btcusdt_kline_1m", (models.Model,), attrs.copy())
EthKline1m = type("binance_ethusdt_kline_1m", (models.Model,), attrs.copy())
XrpKline1m = type("binance_xrpusdt_kline_1m", (models.Model,), attrs.copy())
BnbKline1m = type("binance_bnbusdt_kline_1m", (models.Model,), attrs.copy())
SolKline1m = type("binance_solusdt_kline_1m", (models.Model,), attrs.copy())
AdaKline1m = type("binance_adausdt_kline_1m", (models.Model,), attrs.copy())
DotKline1m = type("binance_dotusdt_kline_1m", (models.Model,), attrs.copy())
DogeKline1m = type("binance_dogeusdt_kline_1m", (models.Model,), attrs.copy())
UniKline1m = type("binance_uniusdt_kline_1m", (models.Model,), attrs.copy())
LtcKline1m = type("binance_ltcusdt_kline_1m", (models.Model,), attrs.copy())
LinkKline1m = type("binance_linkusdt_kline_1m", (models.Model,), attrs.copy())
BchKline1m = type("binance_bchusdt_kline_1m", (models.Model,), attrs.copy())
MaticKline1m = type("binance_maticusdt_kline_1m", (models.Model,), attrs.copy())
AvaxKline1m = type("binance_avaxusdt_kline_1m", (models.Model,), attrs.copy())
ShibKline1m = type("binance_shibusdt_kline_1m", (models.Model,), attrs.copy())
