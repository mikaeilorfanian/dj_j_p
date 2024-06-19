from dataclasses import asdict, dataclass
from typing import Optional

from .registry import job_registery


class BaseJob:
    name: Optional[str] = None

    def __init__(
        self,
        status: Optional[str] = "",
        inputs=None,
        outputs=None,
    ) -> None:
        from django_async_job_pipelines.models import JobDBModel

        if not status:
            status = JobDBModel.JobStatus.NEW

        self.inputs = inputs
        self.outputs = outputs
        self.status = status

    @property
    def is_new(self) -> bool:
        from django_async_job_pipelines.models import JobDBModel

        return self.status == JobDBModel.JobStatus.NEW

    @classmethod
    def create(
        cls,
        inputs=Optional[dict | list],
        outputs=Optional[dict | list],
        status: str = "",
    ) -> "BaseJob":  # TODO fix type hint, make it to work with user defined classes
        if hasattr(cls, "Inputs"):
            if not inputs:
                raise ValueError(
                    "`inputs` parameter missing but `Inputs` class is given for this job."
                )

        return cls(inputs=inputs, outputs=outputs, status=status)

    def inputs_asdict(self) -> dict:
        # TODO rename this method to "serialize"
        if not self.inputs:
            return {}

        if not hasattr(self, "Inputs"):
            raise ValueError(
                "`Inputs` class missing for this job, but `self.inputs` is not `None`!"
            )

        if hasattr(self.inputs, "asdict"):
            return self.inputs.asdict()

        return asdict(self.inputs)

    def outputs_asdict(self) -> dict:
        # TODO rename this method to "deserialize"
        if not self.outputs:
            return {}

        if not hasattr(self, "Outputs"):
            raise ValueError(
                "`Outputs` class missing for this job, but `self.outputs` is not `None`!"
            )

        if hasattr(self.outputs, "asdict"):
            return self.outputs.asdict()

        return asdict(self.outputs)

    async def run(self):
        raise NotImplementedError()

    @property
    def name(self) -> str:
        return type(self).__name__


async def acreate_new(job):
    from .models import JobDBModel

    if job.name not in job_registery.job_class_to_name_map:
        raise ValueError(
            f'Job with name "{job.name}" was not found. It should be a subclass of the "BaseJob" class and located in a `jobs.py` of a registered Django app.'
        )

    if hasattr(job, "Inputs") and not job.inputs:
        raise ValueError(
            "`inputs` parameter missing but `Inputs` class is given for this job."
        )

    j = await JobDBModel.acreate_new_in_db(job)
    return j


async def abulk_create_new(jobs):
    from .models import JobDBModel

    for job in jobs:
        if hasattr(job, "Inputs") and not job.inputs:
            raise ValueError(
                "`inputs` parameter missing but `Inputs` class is given for this job."
            )

    await JobDBModel.abulk_create_new_in_db(jobs)
