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
