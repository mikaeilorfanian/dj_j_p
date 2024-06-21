import asyncio

import pytest
from asgiref.sync import async_to_sync, sync_to_async
from django_async_job_pipelines.job import abulk_create_new, acreate_new
from django_async_job_pipelines.models import JobDBModel
from django_async_job_pipelines.job_runner import run_num_jobs

from myjobs.jobs import JobForTests, JobMissingRunMethod


class TestJobRunnerPersistsOutputToDB:

    def test_job_with_outputs_class_but_outputs_data_not_set(
        self, job_with_inputs_outputs
    ):
        async_to_sync(run_num_jobs)(1, 1)

        job = JobDBModel.objects.get(pk=job_with_inputs_outputs.pk)
        assert job.status == JobDBModel.JobStatus.DONE
        assert not job.outputs
        assert not job.error

    def test_outputs_data_set_for_job_with_outputs_class(self, job_producing_outputs):
        async_to_sync(run_num_jobs)(1, 1)

        job = JobDBModel.objects.get(pk=job_producing_outputs.pk)
        assert job.status == JobDBModel.JobStatus.DONE
        assert job.outputs

    def test_job_without_outputs_class_tries_to_set_its_outputs(
        self, job_without_outputs_class
    ):
        async_to_sync(run_num_jobs)(1, 1)

        job = JobDBModel.objects.get(pk=job_without_outputs_class.pk)
        assert job.status == JobDBModel.JobStatus.ERROR
        assert "AttributeError" in job.error
        assert "Traceback" in job.error
