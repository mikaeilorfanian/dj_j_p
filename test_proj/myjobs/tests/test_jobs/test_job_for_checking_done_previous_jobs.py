import pytest
from asgiref.sync import async_to_sync
from django_async_job_pipelines.job import acreate_new
from django_async_job_pipelines.jobs import RunMultipleJobs


@pytest.mark.run
@pytest.mark.django_db(transaction=True)
class TestJobWhichWaitForPreviousJobsToFinish:
    def test_inputs_not_given(self, db):
        with pytest.raises(ValueError):
            j = RunMultipleJobs()
            async_to_sync(acreate_new)(j)
