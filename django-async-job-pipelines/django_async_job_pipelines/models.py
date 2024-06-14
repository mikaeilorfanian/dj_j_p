import inspect
from dataclasses import asdict
from typing import Iterable, Optional

from asgiref.sync import sync_to_async
from django.db import models
from django.utils.module_loading import import_module

from .job import BaseJob
from .registry import job_registery


class JobDBModel(models.Model):
    class JobStatus(models.TextChoices):
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
    def is_done(self) -> bool:
        return self.status == self.JobStatus.DONE

    @classmethod
    def done_jobs_count(cls) -> int:
        return cls.objects.filter(status=cls.JobStatus.DONE).count()

    @classmethod
    def failed_jobs_count(cls) -> int:
        return cls.objects.filter(status=cls.JobStatus.ERROR).count()

    @property
    def is_new(self) -> bool:
        return self.status == self.JobStatus.NEW

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
    async def aget_by_id(cls, _id: int) -> BaseJob:
        job = await cls.objects.aget(id=_id)
        module = import_module(job_registery.get_import_path_for_class_name(job.name))
        klass = getattr(module, job.name)
        if hasattr(klass, "Inputs"):
            if not job.inputs:
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

        return klass.create(inputs=inputs, outputs=outputs, status=job.status)

    @classmethod
    async def aupdate_new_to_in_progress_by_id(cls, pk: int) -> int:
        return await cls.objects.filter(pk=pk, status=cls.JobStatus.NEW).aupdate(
            status=cls.JobStatus.IN_PROGRESS
        )

    @classmethod
    async def aupdate_in_progress_to_done_by_id(cls, pk: int) -> int:
        return await cls.objects.filter(
            pk=pk, status=cls.JobStatus.IN_PROGRESS
        ).aupdate(status=cls.JobStatus.DONE)

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
        j = await cls.objects.abulk_create(to_create)
        # TODO do we want to chunk this `bulk_create`

        return j

    @classmethod
    def new_jobs_count(cls) -> int:
        return JobDBModel.objects.filter(status=JobDBModel.JobStatus.NEW).count()
