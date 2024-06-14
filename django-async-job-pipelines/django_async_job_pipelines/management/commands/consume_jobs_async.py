import asyncio

from django.core.management.base import BaseCommand, CommandError
from django_async_job_pipelines.models import JobDBModel


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            "--num_jobs_to_consume",
            default=None,
            help="Type: int, max number of jobs to process and then quit.",
        )

    def handle(self, *args, **options):
        if options["num_jobs_to_consume"]:
            asyncio.run(run_num_jobs(int(options["num_jobs_to_consume"])), debug=True)
