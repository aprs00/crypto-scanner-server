# Generated by Django 4.2.2 on 2023-07-30 16:27

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('crypto_scanner', '0010_alter_binancespotkline1m_table'),
    ]

    operations = [
        migrations.CreateModel(
            name='BinanceSpotKline5m',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('ticker', models.CharField(max_length=10)),
                ('start_time', models.DateTimeField()),
                ('end_time', models.DateTimeField()),
                ('open', models.DecimalField(decimal_places=10, max_digits=24)),
                ('close', models.DecimalField(decimal_places=10, max_digits=24)),
                ('high', models.DecimalField(decimal_places=10, max_digits=24)),
                ('low', models.DecimalField(decimal_places=10, max_digits=24)),
                ('base_volume', models.DecimalField(decimal_places=10, max_digits=24)),
                ('number_of_trades', models.IntegerField()),
                ('quote_asset_volume', models.DecimalField(decimal_places=10, max_digits=24)),
                ('taker_buy_base_asset_volume', models.DecimalField(decimal_places=10, max_digits=24)),
                ('taker_buy_quote_asset_volume', models.DecimalField(decimal_places=10, max_digits=24)),
            ],
            options={
                'db_table': 'crypto_scanner_binance_spot_kline_5m',
                'ordering': ['start_time'],
            },
        ),
    ]