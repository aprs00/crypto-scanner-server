# Generated by Django 4.2.2 on 2023-07-18 22:05

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('crypto_scanner', '0009_delete_binance_adausdt_kline_1m_and_more'),
    ]

    operations = [
        migrations.AlterModelTable(
            name='binancespotkline1m',
            table='crypto_scanner_binance_spot_kline_1m',
        ),
    ]