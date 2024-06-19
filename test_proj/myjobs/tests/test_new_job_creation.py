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
    def test_job_with_no_inputs_and_outputs_classes(self, db, new_job):
        assert JobDBModel.new_jobs_count() == 1
        job = async_to_sync(JobDBModel.aget_by_id)(new_job.pk)
        assert job.is_new
        assert isinstance(job, JobForTests)
        assert not job.inputs
        assert not job.outputs

        job = JobDBModel.get(new_job.pk)
        assert job.status == JobDBModel.JobStatus.NEW

    def test_job_with_inputs_class_missing_inputs_data(self, db):
        with pytest.raises(ValueError):
            j = JobWithInputs()
            async_to_sync(acreate_new)(j)

        assert JobDBModel.new_jobs_count() == 0

    def test_job_with_inputs_class_and_inputs_data(self, db):
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

    def test_job_with_outputs_class_missing_outputs_data(self, db):
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

    def test_job_with_inputs_class_and_outputs_class(self, db):
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

    def test_job_with_custom_asdict_method_of_inputs_class_and_ouputs_class(self, db):
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
