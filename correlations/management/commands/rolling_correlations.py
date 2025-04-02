from django.core.management.base import BaseCommand
from correlations.services.rolling_correlations import initialize_rolling_correlations


class Command(BaseCommand):
    help = "Runs the rolling correlations calculations"

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS("Starting correlations calculations..."))
        initialize_rolling_correlations()
