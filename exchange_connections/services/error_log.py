from django.utils import timezone
from crypto_scanner.models import ErrorLog


def log_error(message):
    print("LOG ERROR")
    error_log = ErrorLog()
    error_log.message = message
    error_log.created = timezone.now()
    error_log.save()
