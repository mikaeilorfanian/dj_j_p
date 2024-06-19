import asyncio

import pytest
from asgiref.sync import async_to_sync
from django_async_job_pipelines.job import abulk_create_new, acreate_new
from django_async_job_pipelines.models import JobDBModel
from django_async_job_pipelines.runner2 import run_num_jobs

from myjobs.jobs import JobForTests, JobMissingRunMethod


class TestExludingJobs:
    def test_one_job_to_exclude_exists(self, new_job):
        assert JobDBModel.new_jobs_count() == 1

        async_to_sync(run_num_jobs)(
            max_num_workers=1,
            timeout=3,
            skip_jobs=[new_job.name],
        )

        assert JobDBModel.new_jobs_count() == 1

    def test_job_to_exclude_doesnt_exists(self, new_job):
        assert JobDBModel.new_jobs_count() == 1

        async_to_sync(run_num_jobs)(
            max_num_workers=1,
            timeout=3,
            skip_jobs=["random-name"],
        )

        assert JobDBModel.new_jobs_count() == 0

    def test_multiple_job_names_exist_exclude_only_one(
        self, new_job, new_job_missing_run_method
    ):
        assert JobDBModel.new_jobs_count() == 2

        name_to_exclude = new_job.name

        async_to_sync(run_num_jobs)(
            max_num_workers=1,
            timeout=3,
            skip_jobs=[name_to_exclude],
        )

        assert JobDBModel.new_jobs_count() == 1
        assert (
            JobDBModel.objects.filter(status=JobDBModel.JobStatus.NEW).first().name
            == name_to_exclude
        )
