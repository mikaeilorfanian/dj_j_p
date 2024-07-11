import asyncio
import logging
import os
from typing import Iterable, Optional

from django.db import models
from django.utils.module_loading import import_module

from .job import BaseJob
from .registry import job_registery


def logs_filename():
    return f"{os.getgid()}_job_runner.log"


logger = logging.getLogger(__name__)
FORMAT = "%(asctime)s %(process)d %(taskName)s %(filename)s:%(funcName)s:%(lineno)d %(message)s"
logging.basicConfig(format=FORMAT, filename=logs_filename(), level=logging.DEBUG)


class JobDBModel(models.Model):
    class JobStatus(models.TextChoices):
        NOT_READY = "NOT_READY"
        NEW = "NEW"
        IN_PROGRESS = "IN_PROGRESS"
        DONE = "DONE"
        ERROR = "ERROR"

    previous_job = models.ForeignKey(
        "self",
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        related_name="next_jobs",
    )
    name = models.TextField(max_length=200)
    inputs = models.JSONField(null=True)
    outputs = models.JSONField(null=True)
    date_created = models.DateTimeField(auto_now_add=True)
    date_updated = models.DateTimeField(auto_now=True)
    status = models.CharField(
        max_length=20, choices=JobStatus.choices, default=JobStatus.NEW
    )
    error = models.TextField(null=True)

    class Meta:
        db_table = "async_job"

    def __str__(self) -> str:
        return f"{self.id}: {self.status}"

    @classmethod
    def get(cls, pk) -> "JobDBModel":
        return cls.objects.get(pk=pk)

    @property
    def is_not_ready(self) -> bool:
        return self.status == self.JobStatus.NOT_READY

    @property
    def is_new(self) -> bool:
        return self.status == self.JobStatus.NEW

    @property
    def is_done(self) -> bool:
        return self.status == self.JobStatus.DONE

    @property
    def errored(self) -> bool:
        return self.status == self.JobStatus.ERROR

    @classmethod
    def not_ready_jobs_count(cls) -> int:
        return JobDBModel.objects.filter(status=JobDBModel.JobStatus.NOT_READY).count()

    @classmethod
    def new_jobs_count(cls) -> int:
        return JobDBModel.objects.filter(status=JobDBModel.JobStatus.NEW).count()

    @classmethod
    def done_jobs_count(cls) -> int:
        return cls.objects.filter(status=cls.JobStatus.DONE).count()

    @classmethod
    def failed_jobs_count(cls) -> int:
        return cls.objects.filter(status=cls.JobStatus.ERROR).count()

    @classmethod
    async def amark_as_failed(cls, pk: int, error_msg: str = ""):
        await JobDBModel.objects.filter(pk=pk).aupdate(
            status=cls.JobStatus.ERROR, error=error_msg
        )

    @classmethod
    def get_new_jobs_for_processing(
        cls,
    ) -> list[tuple[int]]:
        res = cls.objects.filter(status=cls.JobStatus.NEW).values_list("pk").all()
        if not res:
            return []
        return list(res)

    @classmethod
    def get_not_ready_jobs(cls) -> models.QuerySet:
        return cls.objects.filter(status=cls.JobStatus.NOT_READY).all()

    @classmethod
    async def aget_job_for_processing(cls, exclude: Optional[list[str]] = None) -> int:
        """
        Picks one job which is in `new` status. Updates its status to `in progress`.
        Returns the job.
        If it cannot pick a job and update, it blocks until it finds one.
        """
        while True:
            found_new: bool = False
            pk: tuple[int]
            if not exclude:
                async for pk in cls.objects.filter(
                    status=cls.JobStatus.NEW
                ).values_list("pk"):
                    found_new = True
                    res = await cls.objects.filter(
                        pk=pk[0], status=cls.JobStatus.NEW
                    ).aupdate(status=cls.JobStatus.IN_PROGRESS)
                    if not res:
                        continue
                    else:
                        return pk[0]
                if not found_new:
                    await asyncio.sleep(0.1)
            else:
                logger.info(f"Fetching from db excluding {exclude} jobs")
                async for pk in (
                    cls.objects.filter(status=cls.JobStatus.NEW)
                    .exclude(name__in=exclude)
                    .values_list("pk")
                ):
                    found_new = True
                    res = (
                        await cls.objects.filter(pk=pk[0], status=cls.JobStatus.NEW)
                        .exclude(name__in=exclude)
                        .aupdate(status=cls.JobStatus.IN_PROGRESS)
                    )
                    if not res:
                        continue
                    else:
                        return pk[0]
                if not found_new:
                    await asyncio.sleep(0.1)

    @classmethod
    async def aget_new_jobs_for_processing(cls, limit: int) -> list[int]:
        if limit == 0:
            raise ValueError("Limit for getting new jobs must be greater than zero!")
        return [
            res[0]
            async for res in cls.objects.filter(status=cls.JobStatus.NEW).values_list(
                "pk"
            )[:limit]
        ]

    @classmethod
    async def aget_by_id(cls, _id: int) -> BaseJob:
        job = await cls.objects.select_related("previous_job").aget(id=_id)
        module = import_module(job_registery.get_import_path_for_class_name(job.name))
        klass = getattr(module, job.name)
        if hasattr(klass, "Inputs"):
            if not job.inputs:
                job.status = cls.JobStatus.ERROR
                await job.asave()
                raise ValueError(
                    "If job class has a `Inputs` class then its inputs should be given!"
                )
            inputs = klass.Inputs(**job.inputs)
        else:
            inputs = None
        if hasattr(klass, "Outputs"):
            if not job.outputs:
                outputs = None
            else:
                outputs = klass.Outputs(**job.outputs)
        else:
            outputs = None

        return klass.create(
            inputs=inputs,
            outputs=outputs,
            status=job.status,
            db_model=job,
            previous_job=job.previous_job,
        )

    @classmethod
    async def aupdate_new_to_in_progress_by_id(cls, pk: int) -> int:
        return await cls.objects.filter(pk=pk, status=cls.JobStatus.NEW).aupdate(
            status=cls.JobStatus.IN_PROGRESS
        )

    @classmethod
    async def aupdate_in_progress_to_done_by_id(
        cls, pk: int, outputs: Optional[dict | list] = None
    ) -> int:
        if not outputs:
            return await cls.objects.filter(
                pk=pk, status=cls.JobStatus.IN_PROGRESS
            ).aupdate(status=cls.JobStatus.DONE)
        else:
            return await cls.objects.filter(
                pk=pk, status=cls.JobStatus.IN_PROGRESS
            ).aupdate(status=cls.JobStatus.DONE, outputs=outputs)

    @classmethod
    async def acreate_new_in_db(
        cls,
        job,
        previous_job: Optional["JobDBModel"] = None,
    ) -> "JobDBModel":
        j = await cls.objects.acreate(
            name=type(job).__name__,
            previous_job=previous_job,
            status=cls.JobStatus.NEW,
            inputs=job.inputs_asdict(),
            outputs=job.outputs_asdict(),
        )

        return j

    @classmethod
    async def abulk_create_new_in_db(
        cls,
        jobs,
    ) -> "JobDBModel":
        to_create = [
            cls(
                name=type(j).__name__,
                status=cls.JobStatus.NEW,
                inputs=j.inputs_asdict(),
                outputs=j.outputs_asdict(),
            )
            for j in jobs
        ]
        await cls.objects.abulk_create(to_create)
        # TODO do we want to chunk this `bulk_create`

    @classmethod
    def create_not_ready_in_db(
        cls, job, previous_job: Optional["JobDBModel"] = None
    ) -> "JobDBModel":
        j = cls.objects.create(
            name=type(job).__name__,
            previous_job=previous_job,
            status=cls.JobStatus.NOT_READY,
            inputs=job.inputs_asdict(),
            outputs=job.outputs_asdict(),
        )

        return j

    @classmethod
    async def ainit_next_job(
        cls, current_job: "JobDBModel", next_job_inputs: Optional[dict] = None
    ) -> bool:
        # TODO create index for look up by `previous_job`?
        try:
            next_job = await cls.objects.aget(previous_job=current_job)
            next_job.status = cls.JobStatus.NEW
            if next_job_inputs:
                next_job.inputs = next_job_inputs
            await next_job.asave()
            return True
        except cls.DoesNotExist:
            return False


class PipelineJobsDBModel(models.Model):
    pipeline = models.ForeignKey(
        "PipelineDBModel", on_delete=models.CASCADE, related_name="jobs"
    )
    job = models.ForeignKey(JobDBModel, on_delete=models.CASCADE)

    class Meta:
        db_table = "async_pipeline_jobs"

    @classmethod
    def create(cls, pipeline: "PipelineDBModel", job: JobDBModel):
        return cls.objects.create(pipeline=pipeline, job=job)

    @classmethod
    def get_jobs_for_pipeline(cls, pipeline_id: int) -> Iterable[str]:
        return cls.objects.filter(pipeline__pk=pipeline_id).values_list(
            "job__status", flat=True
        )


class PipelineDBModel(models.Model):
    class Status(models.TextChoices):
        NEW = "NEW"
        IN_PROGRESS = "IN_PROGRESS"
        DONE = "DONE"
        ERROR = "ERROR"

    name = models.TextField(max_length=200)
    status = models.CharField(max_length=20, choices=Status.choices, default=Status.NEW)

    class Meta:
        db_table = "async_pipeline"

    @classmethod
    def create_new_in_db(
        cls,
        pipeline,
    ) -> "PipelineDBModel":
        j = cls.objects.create(
            name=type(pipeline).__name__,
            status=cls.Status.NEW,
        )

        return j

    @property
    def is_new(self) -> bool:
        return self.status == PipelineDBModel.Status.NEW

    @property
    def is_done(self) -> bool:
        pipeline_jobs = PipelineJobsDBModel.get_jobs_for_pipeline(self.pk)
        for pj_status in pipeline_jobs:
            if pj_status != JobDBModel.JobStatus.DONE:
                return False
        return True

    @property
    def errored(self) -> bool:
        pipeline_jobs = PipelineJobsDBModel.get_jobs_for_pipeline(self.pk)
        for pj_status in pipeline_jobs:
            if pj_status == JobDBModel.JobStatus.ERROR:
                return True
        return False

    def add_job(self, job: JobDBModel) -> PipelineJobsDBModel:
        return PipelineJobsDBModel.create(self, job)
