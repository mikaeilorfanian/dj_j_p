import pytest
from asgiref.sync import async_to_sync
from django_async_job_pipelines.models import JobDBModel, PipelineDBModel
from django_async_job_pipelines.test_utils import run_jobs

from myjobs.pipelines import PipelineMultipleJobsOneInMiddleFails


class TestAJobFails:
    def test_pipeline_halts_when_job_fails(self, db):
        job = async_to_sync(PipelineMultipleJobsOneInMiddleFails.trigger)()
        run_jobs(num_jobs_to_run=3, timeout_seconds=1)

        pipeline_job = JobDBModel.get(pk=job.pk)
        assert pipeline_job
        assert pipeline_job.is_done
        assert pipeline_job.name == "StartPipeline"
        assert pipeline_job.inputs == {
            "first_job_inputs": None,
            "pipeline_name": "PipelineMultipleJobsOneInMiddleFails",
        }
        assert JobDBModel.new_jobs_count() == 0  # there are no jobs ready to process
        assert (
            JobDBModel.done_jobs_count() == 2
        )  # first job and trigger job ran successfully
        assert (
            JobDBModel.failed_jobs_count() == 1
        )  # second job failed because its inputs is missing
        assert (
            JobDBModel.not_ready_jobs_count() == 1
        )  # this is the last job in the pipeline

        failed_jobs = JobDBModel.objects.filter(status=JobDBModel.JobStatus.ERROR).all()
        assert len(failed_jobs) == 1

        first_job_pk = failed_jobs[0].pk
        first_job = JobDBModel.get(pk=first_job_pk)
        assert first_job.errored

        assert PipelineDBModel.objects.count() == 1
        pipeline = PipelineDBModel.objects.first()
        assert pipeline.errored
        assert len(pipeline.jobs.all()) == 3
