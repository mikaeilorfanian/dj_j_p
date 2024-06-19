import asyncio

import pytest
from asgiref.sync import async_to_sync
from django_async_job_pipelines.models import JobDBModel


class TestGetOneNewJob:
    def test_one_new_job_in_db(self, new_job, db):
        res = JobDBModel.get_new_jobs_for_processing()[0][0]
        assert res == new_job.pk

    def test_no_new_job_in_db(self, db):
        res = JobDBModel.get_new_jobs_for_processing()
        assert not res

    def test_multiple_new_jobs_in_db(self, new_job, new_job2, db):
        res = JobDBModel.get_new_jobs_for_processing()[0][0]
        assert isinstance(res, int)
        assert res in (new_job.pk, new_job2.pk)


class TestAsyncGetOneNewJob:
    def test_one_new_job_in_db(self, new_job, db):
        res = async_to_sync(JobDBModel.aget_new_jobs_for_processing)(10)
        assert len(res) == 1
        assert res[0] == new_job.pk

        res = async_to_sync(JobDBModel.aget_new_jobs_for_processing)(1)
        assert len(res) == 1
        assert res[0] == new_job.pk

    def test_no_new_job_in_db(self, db):
        res = async_to_sync(JobDBModel.aget_new_jobs_for_processing)(10)
        assert not res

        res = async_to_sync(JobDBModel.aget_new_jobs_for_processing)(1)
        assert not res

    def test_multiple_new_jobs_in_db(self, new_job, new_job2, db):
        res = async_to_sync(JobDBModel.aget_new_jobs_for_processing)(10)
        assert len(res) == 2
        assert res[0] in (new_job.pk, new_job2.pk)
        assert res[1] in (new_job.pk, new_job2.pk)

        res = async_to_sync(JobDBModel.aget_new_jobs_for_processing)(1)
        assert len(res) == 1

    def test_get_one_job_using_async_method(self, new_job):
        res = async_to_sync(JobDBModel.aget_job_for_processing)()
        assert res

    async def test_get_one_job_async_excluding_one_job_class(
        self, new_job, new_job_missing_run_method
    ):
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(1):
                await JobDBModel.aget_job_for_processing(exclude=[new_job.name])

        with pytest.raises(TimeoutError):
            async with asyncio.timeout(1):
                await JobDBModel.aget_job_for_processing(
                    exclude=[new_job.name, new_job_missing_run_method.name]
                )


class TestUpdateNewJobToInProgress:
    def test_new_job_exists_in_db(self, new_job):
        res = async_to_sync(JobDBModel.aupdate_new_to_in_progress_by_id)(new_job.pk)
        assert res == 1

    def test_job_doesnt_exists_in_db(self, db):
        res = async_to_sync(JobDBModel.aupdate_new_to_in_progress_by_id)(1)
        assert res == 0

    def test_job_status_is_not_new(self, job_in_progress, db):
        res = async_to_sync(JobDBModel.aupdate_new_to_in_progress_by_id)(
            job_in_progress.pk
        )
        assert res == 0
