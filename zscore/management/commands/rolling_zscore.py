from django.core.management.base import BaseCommand
from zscore.services.rolling_zscore import initialize_rolling_z_score


class Command(BaseCommand):
    help = "Runs the rolling zscore calculations"

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS("Starting zscore calculations..."))
        initialize_rolling_z_score()
