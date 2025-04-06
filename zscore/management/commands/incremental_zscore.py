from django.core.management.base import BaseCommand
import threading
from zscore.services.incremental_zscore import initialize_incremental_zscore
from zscore.tasks import subscribe_to_klines_updates


class Command(BaseCommand):
    help = "Runs the incremental zscore calculations"

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS("Starting zscore calculations..."))

        init_thread = threading.Thread(target=initialize_incremental_zscore)
        subscribe_thread = threading.Thread(target=subscribe_to_klines_updates)

        init_thread.start()
        subscribe_thread.start()

        init_thread.join()
        subscribe_thread.join()
