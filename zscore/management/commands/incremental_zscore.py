from django.core.management.base import BaseCommand
from zscore.services.incremental_zscore import ZScoreProcessor


class Command(BaseCommand):
    help = "Runs the incremental zscore calculations"

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS("Starting zscore calculations..."))
        processor = ZScoreProcessor()
        processor.run()
