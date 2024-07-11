import asyncio

from django.core.management.base import BaseCommand, CommandError

from django_async_job_pipelines.job_runner import run_num_jobs
from django_async_job_pipelines.models import JobDBModel


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            "--max_num_workers",
            default=10,
            type=int,
        )

    def handle(self, *args, **options):
        asyncio.run(run_num_jobs(max_num_workers=int(options["max_num_workers"])))
