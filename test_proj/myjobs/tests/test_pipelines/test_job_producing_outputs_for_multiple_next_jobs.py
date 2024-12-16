import pytest
from asgiref.sync import async_to_sync
from django_async_job_pipelines.models import JobDBModel
from django_async_job_pipelines.test_utils import run_jobs
from myjobs.jobs import JobWithInputsForMultipleNextJobs
from myjobs.pipelines import PipelineWithOneJobProducingInputsForMultipleNextJobs


@pytest.mark.django_db(transaction=True)
class TestPipelineWithJobsProducingInputsForMultipleNextJobs:
    def test_one_job_create_inputs_for_multiple_next_jobs(self):
        JOBS_TO_MAKE = 10
        async_to_sync(PipelineWithOneJobProducingInputsForMultipleNextJobs.trigger)(
            inputs=JobWithInputsForMultipleNextJobs.Inputs(jobs_to_make=JOBS_TO_MAKE)
        )
        run_jobs(2)
        assert (
            JobDBModel.objects.count() == JOBS_TO_MAKE + 2
        )  # 2 == trigger job + first job
        assert JobDBModel.done_jobs_count() == 2
        assert JobDBModel.new_jobs_count() == 10
