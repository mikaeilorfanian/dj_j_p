from typing import Optional

from .job import acreate_new
from .jobs import StartPipeline
from .registry import pipeline_registery


class BasePipeline:
    jobs: list = []

    def __init__(self, inputs=None) -> None:
        self.inputs = inputs

    @classmethod
    async def trigger(cls, inputs=None):
        if cls.__name__ not in pipeline_registery.pipeline_class_to_name_map:
            raise ValueError(
                f"Pipeline class {cls.__name__} is not a registered pipeline probably because it is not defined in any `pipelines.py` module of a registerefd Django app."
            )
        if len(cls.jobs) == 0:
            raise ValueError("Pipeline has not jobs defined int it!")

        first_job = cls.jobs[0]
        if hasattr(first_job, "Inputs"):
            if not inputs:
                raise ValueError(
                    f"The first job ({type(first_job)}) takes `inputs`, but no `inputs` were given to `trigger`."
                )
        else:
            if inputs:
                raise ValueError(
                    f"`inputs` were passed to `trigger`, but the first job ({type(first_job)}) does not take any `inputs`."
                )

        return await acreate_new(
            StartPipeline(
                inputs=StartPipeline.Inputs(
                    pipeline_name=cls.__name__, first_job_inputs=inputs
                )
            )
        )
