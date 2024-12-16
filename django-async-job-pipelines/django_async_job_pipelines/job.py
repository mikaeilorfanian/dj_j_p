from dataclasses import asdict
from typing import Any, Iterable, Optional

from .registry import job_registery


class BaseJob:
    """
    `inputs`, `outputs`, and `next_job_inputs` are dataclasses instances
    and/or they provide a `asdict` method for custom behavior.
    """

    def __init__(
        self,
        status: Optional[str] = "",
        inputs=None,
        outputs=None,
        previous_job: Optional["JobDBModel"] = None,
        db_model: Optional["JobDBModel"] = None,
        next_job_inputs: Optional[Any] = None,
    ) -> None:
        from django_async_job_pipelines.models import JobDBModel

        if not status:
            status = JobDBModel.JobStatus.NEW

        self.inputs = inputs
        self.outputs = outputs
        self.status = status
        self.db_model = db_model
        self.previous_job = previous_job
        self.next_job_inputs = next_job_inputs

    @property
    def is_done(self) -> bool:
        from django_async_job_pipelines.models import JobDBModel

        return self.status == JobDBModel.JobStatus.DONE

    @property
    def is_new(self) -> bool:
        from django_async_job_pipelines.models import JobDBModel

        return self.status == JobDBModel.JobStatus.NEW

    @classmethod
    def create(
        cls,
        inputs: Optional[Any] = None,
        outputs: Optional[Any] = None,
        status: str = "",
        previous_job: Optional["BaseJob"] = None,
        db_model: Optional["JobDBModel"] = None,
        check_inputs: Optional[bool] = True,
    ) -> "BaseJob":  # TODO fix type hint, make it to work with user defined classes
        # TODO Add to README: this is how you create job instances
        if check_inputs:
            if hasattr(cls, "Inputs"):
                if not inputs:
                    raise ValueError(
                        "`inputs` parameter missing but `Inputs` class is given for this job."
                    )

        return cls(
            inputs=inputs,
            outputs=outputs,
            status=status,
            previous_job=previous_job,
            db_model=db_model,
        )

    def inputs_asdict(self) -> dict:
        if not self.inputs:
            return {}

        if not hasattr(self, "Inputs"):
            raise ValueError(
                f"`Inputs` class missing for this job, but `self.inputs` is not `None`: {self.inputs}"
            )

        if hasattr(self.inputs, "asdict"):
            return self.inputs.asdict()

        return asdict(self.inputs)

    def outputs_asdict(self) -> dict:
        if not self.outputs:
            return {}

        if not hasattr(self, "Outputs"):
            raise ValueError(
                f"`Outputs` class missing for this job, but `self.outputs` is not `None`: {self.outputs}"
            )

        if hasattr(self.outputs, "asdict"):
            return self.outputs.asdict()

        return asdict(self.outputs)

    def next_job_inputs_asdict(self) -> dict | list:
        if not self.next_job_inputs:
            return {}

        if isinstance(self.next_job_inputs, list) and len(self.next_job_inputs) == 0:
            return []

        if isinstance(self.next_job_inputs, list):
            next_jobs_inputs = list()
            for next_j_inputs in self.next_job_inputs:
                if hasattr(next_j_inputs, "asdict"):
                    next_jobs_inputs.append(next_j_inputs.asdict())
                else:
                    next_jobs_inputs.append(asdict(next_j_inputs))
            return next_jobs_inputs
        else:
            if hasattr(self.next_job_inputs, "asdict"):
                return self.next_job_inputs.asdict()

            return asdict(self.next_job_inputs)

    async def run(self):
        raise NotImplementedError()

    @property
    def name(self) -> str:
        return type(self).__name__


def create_new(job) -> "JobDBModel":
    from .models import JobDBModel

    if job.name not in job_registery.job_class_to_name_map:
        raise ValueError(
            f'Job with name "{job.name}" was not found. It should be a subclass \
            of the "BaseJob" class and located in a `jobs.py` of a registered Django app.'
        )

    if hasattr(job, "Inputs") and not job.inputs:
        raise ValueError(
            "`inputs` parameter missing but `Inputs` class is given for this job."
        )

    j = JobDBModel.create_new_in_db(job)
    return j


async def acreate_new(job) -> "JobDBModel":
    from .models import JobDBModel

    if job.name not in job_registery.job_class_to_name_map:
        raise ValueError(
            f'Job with name "{job.name}" was not found. It should be a subclass \
            of the "BaseJob" class and located in a `jobs.py` of a registered Django app.'
        )

    if hasattr(job, "Inputs") and not job.inputs:
        raise ValueError(
            "`inputs` parameter missing but `Inputs` class is given for this job."
        )

    j = await JobDBModel.acreate_new_in_db(job)
    return j


async def abulk_create_new(jobs: Iterable[BaseJob]):
    from .models import JobDBModel

    for job in jobs:
        if hasattr(job, "Inputs") and not job.inputs:
            raise ValueError(
                "`inputs` parameter missing but `Inputs` class is given for this job."
            )

    await JobDBModel.abulk_create_new_in_db(jobs)


def create_not_ready(
    job: BaseJob, previous_job: Optional["JobDBModel"] = None
) -> "JobDBModel":
    from .models import JobDBModel

    if job.name not in job_registery.job_class_to_name_map:
        raise ValueError(
            f'Job with name "{job.name}" was not found. It should be a subclass \
            of the "BaseJob" class and located in a `jobs.py` of a registered Django app.'
        )

    return JobDBModel.create_not_ready_in_db(job, previous_job)
