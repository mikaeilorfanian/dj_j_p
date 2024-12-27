import csv
import os
from datetime import datetime

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "test_proj.settings")
django.setup()
import httpx
from django.core.management.base import BaseCommand, CommandError

from django_async_job_pipelines.models import JobDBModel as Job


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            "--output_path",
            default=".",
            type=str,
            help="The path to the directory where the outputs files should be written",
        )

    def handle(self, *args, **options):
        output_path = options["output_path"]
        error_jobs = Job.objects.filter(status=Job.JobStatus.ERROR).all()
        with open(f"{output_path}/error_jobs.csv", "w") as f:
            writer = csv.writer(f)
            writer.writerow(["count", "date"])
            for j in error_jobs:
                writer.writerow([1, j.date_updated.isoformat(timespec="seconds")])
            writer.writerow(
                [1, datetime.now().isoformat(timespec="seconds") + "+00:00"]
            )

        done_jobs = Job.objects.filter(status=Job.JobStatus.DONE).all()
        with open(f"{output_path}/done_jobs.csv", "w") as f:
            writer = csv.writer(f)
            writer.writerow(["count", "date"])
            for j in done_jobs:
                writer.writerow([1, j.date_updated.isoformat(timespec="seconds")])
            writer.writerow(
                [1, datetime.now().isoformat(timespec="seconds") + "+00:00"]
            )

        in_progress = Job.objects.filter(status=Job.JobStatus.IN_PROGRESS).all()
        with open(f"{output_path}/in_progress_jobs.csv", "w") as f:
            writer = csv.writer(f)
            writer.writerow(["count", "date"])
            for j in in_progress:
                writer.writerow([1, j.date_updated.isoformat(timespec="seconds")])
            writer.writerow(
                [1, datetime.now().isoformat(timespec="seconds") + "+00:00"]
            )

        all = Job.objects.all()
        with open(f"{output_path}/all_jobs.csv", "w") as f:
            writer = csv.writer(f)
            writer.writerow(["count", "date"])
            for j in all:
                writer.writerow([j.pk, j.date_updated.isoformat(timespec="seconds")])
            writer.writerow(
                [1, datetime.now().isoformat(timespec="seconds") + "+00:00"]
            )
