import time

from asgiref.sync import async_to_sync
from django.core.management import call_command
from django.core.management.base import BaseCommand, CommandError
from django_async_job_pipelines.job import abulk_create_new
from django_async_job_pipelines.models import JobDBModel
from django_async_job_pipelines.runner2 import run_num_jobs

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
        parser.add_argument(
            "--max_num_workers",
            default=0,
            type=int,
        )
        parser.add_argument(
            "--timeout",
            default=0,
            type=int,
        )

    def handle(self, *args, **kwargs):
        JobDBModel.objects.all().delete()

        timeout = kwargs["timeout"]
        create = kwargs["create"]
        consume = kwargs["consume"]
        fail = kwargs["fail"]

        if consume > create:
            raise CommandError(
                f"Create param bigger than consume param: {create} > {consume}"
            )
        if fail > create:
            raise CommandError(
                f"Fail param bigger than create param: {fail} > {create}"
            )

        job_with_sleep = kwargs["job_with_sleep"]
        if job_with_sleep == 0:
            job_with_sleep = False
        else:
            job_with_sleep = True
        if job_with_sleep:
            job_klass = JobWithSleep
        else:
            job_klass = JobForTests

        if kwargs["max_num_workers"] == 0:
            limit = consume
        else:
            limit = kwargs["max_num_workers"]

        start = time.perf_counter()
        fail_jobs = [JobMissingRunMethod() for _ in range(fail)]
        async_to_sync(abulk_create_new)(fail_jobs)
        jobs = [job_klass() for _ in range(create - fail)]
        async_to_sync(abulk_create_new)(jobs)
        duration = time.perf_counter() - start
        self.stdout.write(
            self.style.SUCCESS(
                f"Created {create-fail} regular jobs and {fail} failing jobs in {duration} seconds."
            )
        )

        assert JobDBModel.new_jobs_count() == create

        start = time.perf_counter()
        async_to_sync(run_num_jobs)(
            max_num_workers=limit, num_jobs=consume, timeout=timeout
        )
        duration = time.perf_counter() - start
        self.stdout.write(
            self.style.SUCCESS(
                f"Processed {consume} {job_klass.__name__} jobs in {duration} seconds."
            )
        )

        error_msg = f"New jobs count mismatch: expected {create-consume} got {JobDBModel.new_jobs_count()}"
        assert JobDBModel.new_jobs_count() == create - consume, error_msg

        error_msg = f"Done jobs count mismatch: expected {consume - fail} got {JobDBModel.done_jobs_count()}"
        assert JobDBModel.done_jobs_count() == consume - fail, error_msg

        error_msg = f"Failed jobs count mismatch: expected {fail} got {JobDBModel.failed_jobs_count()}"
        assert JobDBModel.failed_jobs_count() == fail, error_msg
