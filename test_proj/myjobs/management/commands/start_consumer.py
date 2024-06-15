import time

from asgiref.sync import async_to_sync
from django.core.management import call_command
from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone
from django_async_job_pipelines.job import abulk_create_new
from django_async_job_pipelines.models import JobDBModel
from django_async_job_pipelines.runner import run_num_jobs, run_one_job

from myjobs.jobs import JobForTests, JobMissingRunMethod, JobWithSleep


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            "--create",
            default=10,
            type=int,
        )
        parser.add_argument(
            "--consume",
            default=10,
            type=int,
        )
        parser.add_argument(
            "--fail",
            default=0,
            type=int,
        )
        parser.add_argument(
            "--job_with_sleep",
            default=1,
            type=int,
        )

    def handle(self, *args, **kwargs):
        time.sleep(2)
        num_jobs_to_consume = 5
        async_to_sync(run_num_jobs)(num_jobs_to_consume)
