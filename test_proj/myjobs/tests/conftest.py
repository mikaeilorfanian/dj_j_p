import pytest
from asgiref.sync import async_to_sync
from django_async_job_pipelines.job import acreate_new
from django_async_job_pipelines.models import JobDBModel

from myjobs.jobs import JobForTests, JobMissingRunMethod


@pytest.fixture
def new_job(db):
    job_to_create = JobForTests()
    return async_to_sync(acreate_new)(job_to_create)


@pytest.fixture
def new_job2(db):
    job_to_create = JobForTests()
    return async_to_sync(acreate_new)(job_to_create)


@pytest.fixture
def job_in_progress(new_job):
    async_to_sync(JobDBModel.aupdate_new_to_in_progress_by_id)(new_job.pk)
    return new_job


@pytest.fixture
def new_job_missing_run_method(db):
    job = JobMissingRunMethod()
    return async_to_sync(acreate_new)(job)
