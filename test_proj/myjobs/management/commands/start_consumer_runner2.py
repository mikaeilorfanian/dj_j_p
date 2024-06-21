import time

from asgiref.sync import async_to_sync
from django.core.management import call_command
from django.core.management.base import BaseCommand
from django.utils import timezone
from django_async_job_pipelines.job import abulk_create_new
from django_async_job_pipelines.job_runner import run_num_jobs
from django_async_job_pipelines.models import JobDBModel

from myjobs.jobs import JobForTests, JobMissingRunMethod, JobWithSleep


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            "--max_num_workers",
            default=10,
            type=int,
        )
        parser.add_argument(
            "--num_jobs_to_consume",
            default=10,
            type=int,
        )

    def handle(self, *args, **kwargs):
        time.sleep(2)
        num_jobs_to_consume = kwargs["num_jobs_to_consume"]
        num_workers = kwargs["max_num_workers"]
        async_to_sync(run_num_jobs)(
            max_num_workers=num_workers, num_jobs=num_jobs_to_consume
        )
