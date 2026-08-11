import signal

from django.core.management.base import BaseCommand

from footprints.collector import ADAPTER_MODULES, FootprintCollector


class Command(BaseCommand):
    help = "Collect public executions directly into ClickHouse for an exchange"

    def add_arguments(self, parser):
        parser.add_argument(
            "--exchange",
            required=True,
            choices=sorted(ADAPTER_MODULES),
        )

    def handle(self, *args, **options):
        collector = FootprintCollector(options["exchange"])

        def stop_collector(_signum, _frame):
            collector.stop()

        previous_sigterm = signal.signal(signal.SIGTERM, stop_collector)
        try:
            collector.run()
        except KeyboardInterrupt:
            collector.stop()
            self.stdout.write(self.style.WARNING("Footprint collector stopped"))
        finally:
            signal.signal(signal.SIGTERM, previous_sigterm)
