from django.core.management.base import BaseCommand, CommandError
from django_async_job_pipelines.models import JobDBModel


class Command(BaseCommand):
    def handle(self, *args, **kwargs):
        JobDBModel.objects.all().delete()
