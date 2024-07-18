import asyncio

from asgiref.sync import async_to_sync
from django.core.management.base import BaseCommand
from django_async_job_pipelines.job_runner import run_num_jobs
from django_async_job_pipelines.models import (
    JobDBModel,
    PipelineDBModel,
    PipelineJobsDBModel,
)

from myjobs.jobs import DeleteExistingJobs
from myjobs.pipelines import TestPipelineWith10KJobs


class Command(BaseCommand):
    async def delete_all_rows(self):
        await JobDBModel.objects.all().adelete()
        await PipelineDBModel.objects.all().adelete()
        await PipelineJobsDBModel.objects.all().adelete()

    def handle(self, *args, **kwargs):
        num_jobs_to_process = 1000_000
        num_pipeline_jobs = len(TestPipelineWith10KJobs.jobs)
        timeout = 30
        num_workers = 10
        async_to_sync(self.delete_all_rows)()
        async_to_sync(TestPipelineWith10KJobs.trigger)(
            DeleteExistingJobs.Inputs(
                num_jobs_to_create=num_jobs_to_process,
                num_workers_to_spawn=num_workers,
                worker_timeout=timeout,
                num_done_jobs=num_jobs_to_process + num_pipeline_jobs,
            )
        )
        asyncio.run(
            run_num_jobs(
                max_num_workers=1,
                num_jobs=10,
                timeout=timeout,
                skip_jobs=["JobProducingOutputs"],
            )
        )
