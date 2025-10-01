from django.db import models
from django.db.models import UniqueConstraint


class Kline1m(models.Model):
    start_time = models.DateTimeField(db_index=True)
    close_time = models.DateTimeField()
    symbol = models.ForeignKey("Symbol", on_delete=models.CASCADE, db_index=True)
    open = models.DecimalField(max_digits=18, decimal_places=8)
    close = models.DecimalField(max_digits=18, decimal_places=8)
    high = models.DecimalField(max_digits=18, decimal_places=8)
    low = models.DecimalField(max_digits=18, decimal_places=8)
    base_volume = models.DecimalField(max_digits=24, decimal_places=8)
    quote_volume = models.DecimalField(max_digits=24, decimal_places=8)
    taker_buy_base_volume = models.DecimalField(max_digits=24, decimal_places=8)
    taker_buy_quote_volume = models.DecimalField(max_digits=24, decimal_places=8)
    number_of_trades = models.IntegerField()
    exchange = models.ForeignKey("Exchange", on_delete=models.CASCADE, db_index=True)

    class Meta:
        db_table = "cs_klines_1m"
        ordering = ["-start_time", "symbol"]
        constraints = [
            UniqueConstraint(
                fields=["start_time", "symbol", "exchange"],
                name="unique_klines_1m_fields",
            )
        ]
        indexes = [
            models.Index(
                fields=["exchange", "symbol", "-start_time"],
                name="klines_main_query_idx",
            ),
        ]

    def __str__(self):
        return f"{self.symbol.name} @ {self.start_time} | C: {self.close}"


class Exchange(models.Model):
    name = models.CharField(max_length=20, db_index=True)

    class Meta:
        db_table = "cs_exchanges"

    def __str__(self):
        return self.name


class ContractType(models.Model):
    name = models.CharField(max_length=20, db_index=True)

    class Meta:
        db_table = "cs_contract_types"

    def __str__(self):
        return self.name


class Symbol(models.Model):
    name = models.CharField(max_length=20)
    exchange = models.ForeignKey(Exchange, on_delete=models.CASCADE)
    contract_type = models.ForeignKey(
        "ContractType", on_delete=models.CASCADE, null=True, db_index=True
    )

    class Meta:
        db_table = "cs_symbols"
        constraints = [
            UniqueConstraint(
                fields=["name", "exchange", "contract_type"],
                name="unique_symbol_fields",
            )
        ]
        indexes = [
            models.Index(
                fields=["exchange", "contract_type", "name"],
                name="symbol_lookup_idx",
            ),
        ]

    def __str__(self):
        return self.name
