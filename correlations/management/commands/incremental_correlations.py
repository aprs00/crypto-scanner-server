from django.core.management.base import BaseCommand
from correlations.services.incremental_correlations import (
    IncrementalCorrelationCalculator,
)


class Command(BaseCommand):
    help = "Runs the incremental correlations calculations"

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS("Starting correlations calculations..."))
        calculator = IncrementalCorrelationCalculator()
        calculator.run()
