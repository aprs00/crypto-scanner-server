from django.core.management.base import BaseCommand
from correlations.services.incremental_correlations import (
    initialize_incremental_correlations,
)


class Command(BaseCommand):
    help = "Runs the incremental correlations calculations"

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS("Starting correlations calculations..."))
        initialize_incremental_correlations()
