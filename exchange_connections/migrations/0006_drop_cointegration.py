# The cointegration app was removed, so its tables can no longer be dropped by
# migrations of its own. This drops them (history partitions included, which
# returns their disk space to the OS immediately) together with the rows the
# app left in Django's bookkeeping tables. Databases that never had the app are
# unaffected. The data is not recoverable, so there is no reverse operation.

from django.db import migrations


def delete_content_types(apps, schema_editor):
    # Cascades to the app's permissions and any admin log entries.
    ContentType = apps.get_model("contenttypes", "ContentType")
    ContentType.objects.filter(app_label="cointegration").delete()


class Migration(migrations.Migration):

    dependencies = [
        ("exchange_connections", "0005_alter_kline1m_exchange_alter_kline1m_start_time"),
        ("admin", "0003_logentry_add_action_flag_choices"),
        ("auth", "0012_alter_user_first_name_max_length"),
        ("contenttypes", "0002_remove_content_type_name"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            DROP TABLE IF EXISTS cs_cointegration_pair_history CASCADE;
            DROP TABLE IF EXISTS cs_cointegration_pair CASCADE;
            DELETE FROM django_migrations WHERE app = 'cointegration';
            """,
        ),
        migrations.RunPython(delete_content_types),
    ]
