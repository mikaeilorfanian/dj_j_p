import pytest
from asgiref.sync import async_to_sync
from django_async_job_pipelines.models import JobDBModel
from django_async_job_pipelines.pipeline import BasePipeline

from myjobs.jobs import JobForTests, JobWithInputs
from myjobs.pipelines import (
    OneJobPipelineWithInputs,
    OneJobPipelineWithoutInputs,
    PipelineWithoutJobs,
    TwoJobsPipeline,
)


class PipelineInWrongPlace(BasePipeline):
    jobs = [JobForTests]


def test_pipeline_is_not_defined_in_pipelines_module():
    with pytest.raises(ValueError):
        async_to_sync(PipelineInWrongPlace.trigger)()


def test_no_jobs_defined_raises_error():
    with pytest.raises(ValueError):
        async_to_sync(PipelineWithoutJobs.trigger)()


class TestFirstJobTakesInputs:
    def test_inputs_given(self, db):
        job = async_to_sync(OneJobPipelineWithInputs.trigger)(
            inputs=JobWithInputs.Inputs(id=1)
        )
        new_job = JobDBModel.get(pk=job.pk)
        assert new_job
        assert new_job.is_new
        assert new_job.name == "StartPipeline"
        assert new_job.inputs == {
            "first_job_inputs": {"id": 1},
            "pipeline_name": "OneJobPipelineWithInputs",
        }

    def test_inputs_not_given(self):
        with pytest.raises(ValueError):
            async_to_sync(OneJobPipelineWithInputs.trigger)()


class TestFirstJobDoesNotTakeInputs:
    def test_inputs_given(self):
        with pytest.raises(ValueError):
            async_to_sync(OneJobPipelineWithoutInputs.trigger)(
                inputs=JobWithInputs.Inputs(id=1)
            )

    def test_inputs_not_given(self, db):
        job = async_to_sync(OneJobPipelineWithoutInputs.trigger)()
        new_job = JobDBModel.get(pk=job.pk)
        assert new_job
        assert new_job.is_new
        assert new_job.name == "StartPipeline"
        assert new_job.inputs == {
            "first_job_inputs": None,
            "pipeline_name": "OneJobPipelineWithoutInputs",
        }
