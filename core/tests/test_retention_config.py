from pathlib import Path
from xml.etree import ElementTree

from django.conf import settings
from django.test import SimpleTestCase


class ClickHouseSystemLogConfigTests(SimpleTestCase):
    def test_diagnostic_logs_have_bounded_retention(self):
        config_path = (
            Path(settings.BASE_DIR) / "clickhouse" / "config.d" / "system-logs.xml"
        )
        root = ElementTree.parse(config_path).getroot()

        self.assertEqual(root.findtext("logger/level"), "warning")
        self.assertEqual(root.findtext("text_log/level"), "warning")
        for name in (
            "query_log",
            "trace_log",
            "part_log",
            "text_log",
            "metric_log",
            "error_log",
            "query_metric_log",
            "asynchronous_metric_log",
            "processors_profile_log",
            "asynchronous_insert_log",
            "background_schedule_pool_log",
        ):
            self.assertEqual(
                root.findtext(f"{name}/ttl"),
                "event_date + INTERVAL 3 DAY DELETE",
                name,
            )
