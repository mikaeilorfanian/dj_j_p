import pytest
from asgiref.sync import async_to_sync
from django_async_job_pipelines.job import acreate_new
from django_async_job_pipelines.models import JobDBModel

from myjobs.jobs import (
    JobForTests,
    JobMissingRunMethod,
    JobProducingOutputs,
    JobWithCustomAsdict,
    JobWithoutOutputClass,
)


@pytest.fixture
def new_job(db) -> JobDBModel:
    job_to_create = JobForTests()
    return async_to_sync(acreate_new)(job_to_create)


@pytest.fixture
def new_job2(db) -> JobDBModel:
    job_to_create = JobForTests()
    return async_to_sync(acreate_new)(job_to_create)


@pytest.fixture
def job_in_progress(new_job) -> JobDBModel:
    async_to_sync(JobDBModel.aupdate_new_to_in_progress_by_id)(new_job.pk)
    return new_job


@pytest.fixture
def new_job_missing_run_method(db) -> JobDBModel:
    job = JobMissingRunMethod()
    return async_to_sync(acreate_new)(job)


@pytest.fixture
def job_with_inputs_outputs(db) -> JobDBModel:
    job = JobWithCustomAsdict(inputs=JobWithCustomAsdict.Inputs(id=1))
    return async_to_sync(acreate_new)(job)


@pytest.fixture
def job_producing_outputs(db) -> JobDBModel:
    job = JobProducingOutputs(inputs=JobProducingOutputs.Inputs(id=1))
    return async_to_sync(acreate_new)(job)


@pytest.fixture
def job_without_outputs_class(db) -> JobDBModel:
    job = JobWithoutOutputClass(inputs=JobWithoutOutputClass.Inputs(id=1))
    return async_to_sync(acreate_new)(job)
