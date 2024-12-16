import pytest
from asgiref.sync import async_to_sync
from django_async_job_pipelines.job import acreate_new
from django_async_job_pipelines.jobs import CheckPreviousJobsFinished
from django_async_job_pipelines.models import JobDBModel, PipelineDBModel
from django_async_job_pipelines.test_utils import run_jobs
from myjobs.jobs import JobProducingOutputs, JobWithInputsAndOutputs, JobWithLongSleep


@pytest.mark.run
@pytest.mark.django_db(transaction=True)
class TestJobWhichWaitForPreviousJobsToFinish:
    def test_inputs_not_given(self, db):
        with pytest.raises(ValueError):
            j = CheckPreviousJobsFinished()
            async_to_sync(acreate_new)(j)

    def test_previous_job_with_given_id_does_not_exist(self, db):
        non_existing_job_id = 10
        j = CheckPreviousJobsFinished(
            inputs=CheckPreviousJobsFinished.Inputs(
                previous_jobs_ids=[non_existing_job_id]
            )
        )
        job = async_to_sync(acreate_new)(j)

        run_jobs(1)

        job_in_db = JobDBModel.get(pk=job.pk)
        assert job_in_db
        assert job_in_db.errored
        assert str(non_existing_job_id) in job_in_db.error
        assert "does not exist" in job_in_db.error
        assert job_in_db.inputs == {
            "previous_jobs_ids": [non_existing_job_id],
        }

    def test_previous_job_id_points_to_same_job(self, db):
        wait_job_id = 2  # hardcoded job ID, just guessing the PK for the new job
        j = CheckPreviousJobsFinished(
            inputs=CheckPreviousJobsFinished.Inputs(previous_jobs_ids=[wait_job_id])
        )
        job = async_to_sync(acreate_new)(j)

        assert job.pk == wait_job_id

        run_jobs(1)

        job_in_db = JobDBModel.get(pk=job.pk)
        assert job_in_db
        assert job_in_db.is_done
        assert job_in_db.inputs == {
            "previous_jobs_ids": [wait_job_id],
        }
        assert job_in_db.outputs == {
            "finished_jobs_outputs": [],
        }

    def test_previous_job_is_not_done(self, db):
        j_with_sleep = JobWithLongSleep()
        job_with_sleep = async_to_sync(acreate_new)(j_with_sleep)

        j = CheckPreviousJobsFinished(
            inputs=CheckPreviousJobsFinished.Inputs(
                previous_jobs_ids=[job_with_sleep.pk]
            )
        )
        job = async_to_sync(acreate_new)(j)

        run_jobs(2, timeout_seconds=2)

        job_in_db = JobDBModel.get(pk=job.pk)
        assert job_in_db
        assert job_in_db.is_in_progress  # it's in progress because `run_jobs` timed out
        assert job_in_db.inputs == {
            "previous_jobs_ids": [job_with_sleep.pk],
        }
        assert (
            job_in_db.outputs == {}
        )  # should have no outputs because it's in progress

    def test_one_previous_job_errors_out(self, db):
        j_with_sleep = JobWithInputsAndOutputs(
            inputs=JobWithInputsAndOutputs.Inputs(id=1)
        )
        job_with_sleep = async_to_sync(acreate_new)(j_with_sleep)

        j = CheckPreviousJobsFinished(
            inputs=CheckPreviousJobsFinished.Inputs(
                previous_jobs_ids=[job_with_sleep.pk]
            )
        )
        job = async_to_sync(acreate_new)(j)

        run_jobs(2, timeout_seconds=1)

        job_in_db = JobDBModel.get(pk=job.pk)
        assert job_in_db
        assert (
            job_in_db.is_in_progress
        )  # it's in progress because `job_with_sleep` failed
        assert job_in_db.inputs == {
            "previous_jobs_ids": [job_with_sleep.pk],
        }

    def test_one_previous_job_is_done(self, db):
        j_producting_outputs = JobProducingOutputs(
            inputs=JobProducingOutputs.Inputs(id=1)
        )
        job_producting_outputs = async_to_sync(acreate_new)(j_producting_outputs)

        j = CheckPreviousJobsFinished(
            inputs=CheckPreviousJobsFinished.Inputs(
                previous_jobs_ids=[job_producting_outputs.pk]
            )
        )
        job = async_to_sync(acreate_new)(j)

        run_jobs(2, timeout_seconds=1)

        job_in_db = JobDBModel.get(pk=job.pk)
        assert job_in_db
        assert job_in_db.is_done
        assert job_in_db.inputs == {
            "previous_jobs_ids": [job_producting_outputs.pk],
        }
        assert job_in_db.outputs["finished_jobs_outputs"][0]["id"]

    def test_one_previous_job_is_done_the_other_errors_out(self, db):
        j_with_sleep = JobProducingOutputs(inputs=JobProducingOutputs.Inputs(id=1))
        job_with_sleep = async_to_sync(acreate_new)(j_with_sleep)
        j_with_sleep = JobWithInputsAndOutputs(
            inputs=JobWithInputsAndOutputs.Inputs(id=1)
        )
        job_with_sleep = async_to_sync(acreate_new)(j_with_sleep)

        j = CheckPreviousJobsFinished(
            inputs=CheckPreviousJobsFinished.Inputs(
                previous_jobs_ids=[job_with_sleep.pk]
            )
        )
        job = async_to_sync(acreate_new)(j)

        run_jobs(3, timeout_seconds=1)

        job_in_db = JobDBModel.get(pk=job.pk)
        assert job_in_db
        assert job_in_db.is_in_progress
        assert job_in_db.inputs == {
            "previous_jobs_ids": [job_with_sleep.pk],
        }
        assert not job_in_db.outputs
