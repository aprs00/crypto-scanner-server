from django.core.management.base import BaseCommand
from zscore.services.incremental_zscore import initialize_incremental_zscore


class Command(BaseCommand):
    help = "Runs the incremental zscore calculations"

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS("Starting zscore calculations..."))
        initialize_incremental_zscore()
