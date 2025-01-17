# A test here creates 100 jobs and processes them so other tests end up with 100 "DONE" jobs
from asgiref.sync import async_to_sync
from django_async_job_pipelines.job import abulk_create_new
from django_async_job_pipelines.models import JobDBModel
from django_async_job_pipelines.old_runner import run_one_job
from django_async_job_pipelines.test_utils import run_jobs

from myjobs.jobs import JobForTests, JobMissingRunMethod


class TestRunOneJobFunction:
    # TODO this can be deleted
    def test_new_job_gets_processed_successfully(self, new_job):
        assert JobDBModel.new_jobs_count() == 1

        async_to_sync(run_one_job)(new_job.pk)

        assert JobDBModel.new_jobs_count() == 0
        assert JobDBModel.done_jobs_count() == 1

        assert not JobDBModel.get(new_job.pk).error

    def test_new_job_processing_fails(self, new_job_missing_run_method):
        assert JobDBModel.new_jobs_count() == 1

        async_to_sync(run_one_job)(new_job_missing_run_method.pk)

        assert JobDBModel.new_jobs_count() == 0
        assert JobDBModel.failed_jobs_count() == 1

        assert "Traceback" in JobDBModel.get(new_job_missing_run_method.pk).error
        assert (
            "raise NotImplementedError"
            in JobDBModel.get(new_job_missing_run_method.pk).error
        )


class TestRunningMoreThanOneJob:
    def test_new_job_exists(self, new_job):
        assert JobDBModel.new_jobs_count() == 1

        run_jobs(1)

        assert JobDBModel.new_jobs_count() == 0
        assert JobDBModel.done_jobs_count() == 1

        assert not JobDBModel.get(new_job.pk).error

    def test_no_jobs_to_consume_so_it_times_out(self, db):
        assert JobDBModel.new_jobs_count() == 0

        run_jobs(1, timeout_seconds=1)

        assert JobDBModel.done_jobs_count() == 0
        assert JobDBModel.failed_jobs_count() == 0

    def test_two_jobs_available_but_one_only_to_consume(self, new_job, new_job2):
        assert JobDBModel.new_jobs_count() == 2

        run_jobs(1)

        assert JobDBModel.done_jobs_count() == 1
        assert JobDBModel.new_jobs_count() == 1

    def test_two_jobs_available_consume_both(self, new_job, new_job2):
        assert JobDBModel.new_jobs_count() == 2

        run_jobs(1)

        assert JobDBModel.new_jobs_count() == 0
        assert JobDBModel.done_jobs_count() == 2

    def test_process_jobs_by_chunks(self, db):
        total_num_jobs = 100
        num_jobs_to_consume = 5
        async_to_sync(abulk_create_new)([JobForTests() for _ in range(total_num_jobs)])
        assert JobDBModel.new_jobs_count() == total_num_jobs

        run_jobs(num_jobs_to_consume)

        assert JobDBModel.new_jobs_count() == total_num_jobs - num_jobs_to_consume
        assert JobDBModel.done_jobs_count() == num_jobs_to_consume

    def test_process_jobs_where_some_fail(self, db):
        num_successful_jobs = 5
        num_failing_jobs = 5
        async_to_sync(abulk_create_new)(
            [JobForTests() for _ in range(num_successful_jobs)]
        )
        async_to_sync(abulk_create_new)(
            [JobMissingRunMethod() for _ in range(num_failing_jobs)]
        )
        assert JobDBModel.new_jobs_count() == num_successful_jobs + num_failing_jobs

        run_jobs(num_failing_jobs + num_successful_jobs)

        assert JobDBModel.new_jobs_count() == 0
        assert JobDBModel.done_jobs_count() == num_successful_jobs
        assert JobDBModel.failed_jobs_count() == num_failing_jobs

    def test_process_jobs_where_all_fail(self, db):
        total_jobs = 10
        num_to_consume = 5
        async_to_sync(abulk_create_new)(
            [JobMissingRunMethod() for _ in range(total_jobs)]
        )
        assert JobDBModel.new_jobs_count() == total_jobs

        run_jobs(num_to_consume)

        assert JobDBModel.new_jobs_count() == total_jobs - num_to_consume
        assert JobDBModel.failed_jobs_count() == num_to_consume
