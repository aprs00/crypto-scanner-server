import signal

from django.core.management.base import BaseCommand, CommandError

from footprints.collector import ADAPTER_MODULES, FootprintCollector


class Command(BaseCommand):
    help = "Collect public executions directly into ClickHouse for an exchange"

    def add_arguments(self, parser):
        parser.add_argument(
            "--exchange",
            required=True,
            choices=sorted(ADAPTER_MODULES),
        )
        parser.add_argument(
            "--standby",
            action="store_true",
            help="Open execution feeds in standby until promoted with SIGUSR1",
        )

    def handle(self, *args, **options):
        standby = options["standby"]
        sigusr1 = getattr(signal, "SIGUSR1", None)
        if standby and sigusr1 is None:
            raise CommandError("--standby requires SIGUSR1 support")

        collector = FootprintCollector(
            options["exchange"],
            standby=standby,
        )

        def stop_collector(_signum, _frame):
            collector.stop()

        def promote_collector(_signum, _frame):
            collector.promote()

        previous_handlers = {}
        try:
            previous_handlers[signal.SIGTERM] = signal.signal(
                signal.SIGTERM,
                stop_collector,
            )
            if standby:
                previous_handlers[sigusr1] = signal.signal(
                    sigusr1,
                    promote_collector,
                )
            try:
                collector.run()
            except KeyboardInterrupt:
                collector.stop()
                self.stdout.write(self.style.WARNING("Footprint collector stopped"))
        finally:
            for signum, handler in reversed(tuple(previous_handlers.items())):
                signal.signal(signum, handler)
