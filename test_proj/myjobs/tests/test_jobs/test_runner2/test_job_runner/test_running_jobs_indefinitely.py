import asyncio

import pytest
from asgiref.sync import async_to_sync, sync_to_async
from django_async_job_pipelines.job import abulk_create_new, acreate_new
from django_async_job_pipelines.job_runner import run_num_jobs
from django_async_job_pipelines.models import JobDBModel

from myjobs.jobs import JobForTests, JobMissingRunMethod


class TestRunningJobsIndefinitely:
    async def test_consume_until_timeout(self, db):
        total_num_jobs = 100
        await abulk_create_new([JobForTests() for _ in range(total_num_jobs)])
        await asyncio.sleep(0.1)
        assert await sync_to_async(JobDBModel.new_jobs_count)() == total_num_jobs

        with pytest.raises(TimeoutError):
            async with asyncio.timeout(2):
                await run_num_jobs(max_num_workers=10)

        assert await sync_to_async(JobDBModel.done_jobs_count)() > 0
        # TODO check what the number of DONE jobs actually is
