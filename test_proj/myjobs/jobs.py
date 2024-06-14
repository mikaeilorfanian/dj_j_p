from dataclasses import dataclass

from django.core.handlers.asgi import asyncio
from django_async_job_pipelines.job import BaseJob


class BaseTestJob(BaseJob):
    async def run(self):
        pass


class JobForTests(BaseTestJob):
    name = "test job"


class JobMissingRunMethod(BaseJob):
    pass


class JobWithSleep(BaseTestJob):
    async def run(self):
        await asyncio.sleep(0.1)


class JobWithInputs(BaseJob):
    name = "job with inputs"

    @dataclass
    class Inputs:
        id: int


class JobWithInputsAndOutputs(BaseJob):
    name = "job with inputs and outputs"

    @dataclass
    class Inputs:
        id: int

    @dataclass
    class Outputs:
        id: int


class JobWithCustomAsdict(BaseJob):
    @dataclass
    class Inputs:
        id: int

        def asdict(self):
            return {"id": self.id}

    @dataclass
    class Outputs:
        id: int

        def asdict(self):
            return {"id": self.id}
