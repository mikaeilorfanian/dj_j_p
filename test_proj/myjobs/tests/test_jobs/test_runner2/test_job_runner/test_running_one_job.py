import asyncio

import pytest
from asgiref.sync import async_to_sync, sync_to_async
from django_async_job_pipelines.job import abulk_create_new, acreate_new
from django_async_job_pipelines.job_runner import run_num_jobs
from django_async_job_pipelines.models import JobDBModel

from myjobs.jobs import JobForTests, JobMissingRunMethod


class TestRunOneJob:
    def test_job_gets_processed_successfully(self, new_job):
        assert JobDBModel.new_jobs_count() == 1

        async_to_sync(run_num_jobs)(1, 1)

        assert JobDBModel.new_jobs_count() == 0
        assert JobDBModel.done_jobs_count() == 1

        assert not JobDBModel.get(new_job.pk).error

    def test_job_processing_fails(self, new_job_missing_run_method):
        assert JobDBModel.new_jobs_count() == 1

        async_to_sync(run_num_jobs)(1, 1)

        assert JobDBModel.new_jobs_count() == 0
        assert JobDBModel.failed_jobs_count() == 1

        assert "Traceback" in JobDBModel.get(new_job_missing_run_method.pk).error
        assert (
            "raise NotImplementedError"
            in JobDBModel.get(new_job_missing_run_method.pk).error
        )

    def test_two_jobs_available_but_one_only_to_consume(self, new_job, new_job2):
        assert JobDBModel.new_jobs_count() == 2

        async_to_sync(run_num_jobs)(1, 1)

        assert JobDBModel.done_jobs_count() == 1
        assert JobDBModel.new_jobs_count() == 1
