import pytest
from asgiref.sync import async_to_sync
from django_async_job_pipelines.models import JobDBModel, PipelineDBModel
from django_async_job_pipelines.test_utils import run_jobs

from myjobs.pipelines import OneJobPipeline


class TestRunningAllJobsInPipeline:
    def test_pipeline_with_one_job(self, db):
        job = async_to_sync(OneJobPipeline.trigger)()
        run_jobs(2)  # one trigger job, one pipeline job

        pipeline_job = JobDBModel.get(pk=job.pk)
        assert pipeline_job
        assert pipeline_job.is_done
        assert pipeline_job.name == "StartPipeline"
        assert pipeline_job.inputs == {
            "first_job_inputs": None,
            "pipeline_name": "OneJobPipeline",
        }
        assert JobDBModel.new_jobs_count() == 0
        assert JobDBModel.done_jobs_count() == 2

        assert PipelineDBModel.objects.count() == 1
        pipeline = PipelineDBModel.objects.first()
        assert pipeline.is_done
        assert len(pipeline.jobs.all()) == 1
