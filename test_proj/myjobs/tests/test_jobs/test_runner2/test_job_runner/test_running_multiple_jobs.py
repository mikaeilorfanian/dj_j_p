import asyncio

import pytest
from asgiref.sync import async_to_sync, sync_to_async
from django_async_job_pipelines.job import abulk_create_new, acreate_new
from django_async_job_pipelines.models import JobDBModel
from django_async_job_pipelines.test_utils import run_jobs

from myjobs.jobs import JobForTests, JobMissingRunMethod


class TestMoreThanOneJob:
    def test_no_jobs_to_consume_so_it_times_out(self, db):
        JobDBModel.objects.all().delete()
        assert JobDBModel.new_jobs_count() == 0

        run_jobs(1, 1)

        assert JobDBModel.done_jobs_count() == 0
        assert JobDBModel.failed_jobs_count() == 0

    def test_two_jobs_available_consume_both(self, new_job, new_job2):
        assert JobDBModel.new_jobs_count() == 2

        initial_done_jobs_count = JobDBModel.done_jobs_count()

        run_jobs(2, num_workers=2)

        assert JobDBModel.new_jobs_count() == 0
        assert JobDBModel.done_jobs_count() == initial_done_jobs_count + 2

    def test_process_a_subset_of_jobs(self, db):
        JobDBModel.objects.all().delete()
        total_num_jobs = 100
        num_jobs_to_consume = 5
        async_to_sync(abulk_create_new)([JobForTests() for _ in range(total_num_jobs)])
        assert JobDBModel.new_jobs_count() == total_num_jobs

        run_jobs(num_jobs_to_consume, num_workers=num_jobs_to_consume)

        assert JobDBModel.new_jobs_count() == total_num_jobs - num_jobs_to_consume
        assert JobDBModel.done_jobs_count() == num_jobs_to_consume

    def test_process_all_jobs_where_some_fail(self, db):
        JobDBModel.objects.all().delete()
        num_successful_jobs = 5
        num_failing_jobs = 5
        total = num_failing_jobs + num_successful_jobs
        async_to_sync(abulk_create_new)(
            [JobForTests() for _ in range(num_successful_jobs)]
        )
        async_to_sync(abulk_create_new)(
            [JobMissingRunMethod() for _ in range(num_failing_jobs)]
        )
        assert JobDBModel.new_jobs_count() == total

        run_jobs(total, num_workers=total)

        assert JobDBModel.new_jobs_count() == 0
        assert JobDBModel.done_jobs_count() == num_successful_jobs
        assert JobDBModel.failed_jobs_count() == num_failing_jobs

    def test_process_all_jobs_where_all_fail(self, db):
        JobDBModel.objects.all().delete()
        total_jobs = 10
        num_to_consume = 5
        async_to_sync(abulk_create_new)(
            [JobMissingRunMethod() for _ in range(total_jobs)]
        )
        assert JobDBModel.new_jobs_count() == total_jobs

        run_jobs(num_to_consume, num_workers=num_to_consume)

        assert JobDBModel.new_jobs_count() == total_jobs - num_to_consume
        assert JobDBModel.failed_jobs_count() == num_to_consume
