# Generated by Django 4.2.2 on 2023-10-23 17:06

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('crypto_scanner', '0001_initial'),
    ]

    operations = [
        migrations.AddConstraint(
            model_name='binancespotkline5m',
            constraint=models.UniqueConstraint(fields=('ticker', 'start_time'), name='unique_ticker_start_time'),
        ),
    ]