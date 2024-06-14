import pytest
from asgiref.sync import async_to_sync
from django_async_job_pipelines.job import acreate_new
from django_async_job_pipelines.models import JobDBModel
from myjobs.jobs import (
    JobForTests,
    JobWithCustomAsdict,
    JobWithInputs,
    JobWithInputsAndOutputs,
)


class TestAsyncCreateNewJobFunction:
    def test_job_with_no_inputs_and_outputs(self, db, new_job):

        assert JobDBModel.new_jobs_count() == 1
        job = async_to_sync(JobDBModel.aget_by_id)(new_job.pk)
        assert job.is_new
        assert isinstance(job, JobForTests)
        assert not job.inputs
        assert not job.outputs

        job = JobDBModel.get(new_job.pk)
        assert job.status == JobDBModel.JobStatus.NEW

    def test_job_with_inputs_missing_inputs(self, db):
        with pytest.raises(ValueError):
            j = JobWithInputs()
            async_to_sync(acreate_new)(j)

        assert JobDBModel.new_jobs_count() == 0

    def test_job_with_inputs(self, db):
        j = JobWithInputs.create(inputs=JobWithInputs.Inputs(id=1))
        new_job = async_to_sync(acreate_new)(j)

        assert JobDBModel.new_jobs_count() == 1
        job = async_to_sync(JobDBModel.aget_by_id)(new_job.pk)
        assert job
        assert isinstance(job, JobWithInputs)
        assert job.inputs
        assert isinstance(job.inputs, JobWithInputs.Inputs)

        job = JobDBModel.get(new_job.pk)
        assert job.status == JobDBModel.JobStatus.NEW

    def test_job_with_outputs_missing_outputs(self, db):
        j = JobWithInputsAndOutputs.create(inputs=JobWithInputsAndOutputs.Inputs(id=1))
        new_job = async_to_sync(acreate_new)(j)

        assert JobDBModel.new_jobs_count() == 1
        job = async_to_sync(JobDBModel.aget_by_id)(new_job.pk)
        assert job
        assert isinstance(job, JobWithInputsAndOutputs)
        assert job.inputs
        assert isinstance(job.inputs, JobWithInputsAndOutputs.Inputs)
        assert not job.outputs

        job = JobDBModel.get(new_job.pk)
        assert job.status == JobDBModel.JobStatus.NEW

    def test_job_with_inputs_and_outputs(self, db):
        j = JobWithInputsAndOutputs.create(
            inputs=JobWithInputsAndOutputs.Inputs(id=1),
            outputs=JobWithInputsAndOutputs.Outputs(id=1),
        )
        new_job = async_to_sync(acreate_new)(j)

        assert JobDBModel.new_jobs_count() == 1
        job = async_to_sync(JobDBModel.aget_by_id)(new_job.pk)
        assert job
        assert isinstance(job, JobWithInputsAndOutputs)
        assert job.inputs
        assert isinstance(job.inputs, JobWithInputsAndOutputs.Inputs)
        assert job.outputs
        assert isinstance(job.outputs, JobWithInputsAndOutputs.Outputs)

        job = JobDBModel.get(new_job.pk)
        assert job.status == JobDBModel.JobStatus.NEW

    def test_job_with_previous_job(self):
        pass

    def test_job_with_custom_inputs_and_ouputs_asdict_method(self, db):
        j = JobWithCustomAsdict.create(
            JobWithCustomAsdict.Inputs(id=2), JobWithCustomAsdict.Outputs(id=1)
        )
        new_job = async_to_sync(acreate_new)(j)

        assert JobDBModel.new_jobs_count() == 1
        job = async_to_sync(JobDBModel.aget_by_id)(new_job.pk)
        assert job
        assert isinstance(job, JobWithCustomAsdict)
        assert job.inputs
        assert isinstance(job.inputs, JobWithCustomAsdict.Inputs)
        assert job.outputs
        assert isinstance(job.outputs, JobWithCustomAsdict.Outputs)

        job = JobDBModel.get(new_job.pk)
        assert job.status == JobDBModel.JobStatus.NEW


class TestSyncCreateNewJobFunction:
    # TODO fill this out
    pass
