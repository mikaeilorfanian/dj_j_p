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
            help="Maximum number of `async` workers (not OS processes) which will be consuming jobs concurrently",
        )
        parser.add_argument(
            "--exclude",
            default="",
            type=str,
            help="Comma separated job names (a job name is the name of the job `class`) to exclude from this job runner",
        )
        parser.add_argument(
            "--timeout",
            default=0,
            type=int,
            help="This is used for testing purposes mainly. The jobs runner stops after this many seconds.",
        )

    def handle(self, *args, **options):
        timeout = options["timeout"]
        if options["exclude"]:
            jobs_to_skip = options["exclude"].split(",")
            asyncio.run(
                run_num_jobs(
                    max_num_workers=int(options["max_num_workers"]),
                    skip_jobs=jobs_to_skip,
                    timeout=timeout,
                ),
            )
        else:
            asyncio.run(
                run_num_jobs(
                    max_num_workers=int(options["max_num_workers"]), timeout=timeout
                )
            )
