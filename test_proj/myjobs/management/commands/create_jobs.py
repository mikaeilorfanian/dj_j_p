from asgiref.sync import async_to_sync
from django.core.management.base import BaseCommand, CommandError
from django_async_job_pipelines.job import abulk_create_new
from django_async_job_pipelines.models import JobDBModel

from myjobs.jobs import JobForTests, JobMissingRunMethod, JobWithSleep


class Command(BaseCommand):
    def handle(self, *args, **kwargs):
        num_jobs_to_create = 100
        async_to_sync(abulk_create_new)(
            [JobForTests() for _ in range(num_jobs_to_create)]
        )
