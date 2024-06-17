from asgiref.sync import async_to_sync
from django.core.management.base import BaseCommand, CommandError
from django_async_job_pipelines.job import abulk_create_new
from django_async_job_pipelines.models import JobDBModel

from myjobs.jobs import JobForTests, JobMissingRunMethod, JobWithSleep


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            "create",
            default=10,
            type=int,
        )

    def handle(self, *args, **kwargs):
        to_create = kwargs["create"]
        async_to_sync(abulk_create_new)([JobForTests() for _ in range(to_create)])
