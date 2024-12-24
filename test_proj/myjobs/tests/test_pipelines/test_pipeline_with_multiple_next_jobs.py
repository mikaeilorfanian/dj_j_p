import pytest
from asgiref.sync import async_to_sync
from django_async_job_pipelines.models import JobDBModel
from django_async_job_pipelines.test_utils import run_jobs
from myjobs.jobs import JobWithInputsForMultipleNextJobsWithWait
from myjobs.pipelines import PipelineWithMultipleNextJobs


@pytest.mark.django_db(transaction=True)
class TestPipelineWithSameJobsRunningInParallel:
    def test_multiple_jobs_creater_does_not_run_yet(self):
        JOBS_TO_MAKE = 10
        async_to_sync(PipelineWithMultipleNextJobs.trigger)(
            inputs=JobWithInputsForMultipleNextJobsWithWait.Inputs(
                jobs_to_make=JOBS_TO_MAKE
            )
        )
        run_jobs(2)
        assert JobDBModel.objects.count() == 4  # 4 == trigger job + num pipeline jobs
        assert JobDBModel.done_jobs_count() == 2

        assert JobDBModel.new_jobs_count() == 1
        assert JobDBModel.not_ready_jobs_count() == 1

        assert (
            JobDBModel.objects.filter(
                name="JobWithInputsForMultipleNextJobsWithWait"
            ).count()
            == 1
        )
        assert JobDBModel.objects.filter(name="RunMultipleJobs").count() == 1
        assert (
            JobDBModel.objects.filter(name="JobWithLongSleep").count() == 1
        )  # this one was created by the pipeline trigger job

    def test_multiple_jobs_creater_runs_and_waits(self):
        JOBS_TO_MAKE = 10
        async_to_sync(PipelineWithMultipleNextJobs.trigger)(
            inputs=JobWithInputsForMultipleNextJobsWithWait.Inputs(
                jobs_to_make=JOBS_TO_MAKE
            )
        )
        run_jobs(13, num_workers=2)
        assert (
            JobDBModel.objects.count() == 4 + JOBS_TO_MAKE - 1
        ), "-1 == job created by pipeline trigger ,4 == trigger job + num pipeline jobs"
        assert (
            JobDBModel.done_jobs_count() == 2
        ), "trigger + first job, parallel job runner is still running"
        assert JobDBModel.failed_jobs_count() == 0

        in_progress_jobs_count = JobDBModel.in_progress_jobs_count()
        assert in_progress_jobs_count > 2
        assert (
            JobDBModel.new_jobs_count() == JOBS_TO_MAKE - in_progress_jobs_count + 1
        ), "+1 = multiple job runner"

        assert (
            JobDBModel.objects.filter(
                name="JobWithInputsForMultipleNextJobsWithWait"
            ).count()
            == 1
        )
        assert JobDBModel.objects.filter(name="RunMultipleJobs").count() == 1
        assert (
            JobDBModel.objects.filter(name="JobWithLongSleep").count() == 10
        ), "one job was already created by the pipeline trigger job"

        for job in JobDBModel.objects.filter(name="JobWithLongSleep").all():
            assert job.inputs
