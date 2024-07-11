from dataclasses import dataclass

from django.core.handlers.asgi import asyncio
from django_async_job_pipelines.job import BaseJob, abulk_create_new
from django_async_job_pipelines.models import JobDBModel


class BaseTestJob(BaseJob):
    async def run(self):
        pass


class JobForTests(BaseTestJob):
    pass


class JobMissingRunMethod(BaseJob):
    pass


class JobWithSleep(BaseTestJob):
    async def run(self):
        await asyncio.sleep(0.1)


class JobWithInputs(BaseJob):
    @dataclass
    class Inputs:
        id: int


class JobWithInputsAndOutputs(BaseJob):
    @dataclass
    class Inputs:
        id: int

    @dataclass
    class Outputs:
        id: int


class JobWithCustomAsdict(BaseJob):
    async def run(self):
        pass

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


class JobWithoutOutputClass(BaseJob):
    async def run(self):
        self.outputs = self.Outputs(id=20)

    @dataclass
    class Inputs:
        id: int

        def asdict(self):
            return {"id": self.id}


class JobProducingOutputs(BaseJob):
    async def run(self):
        self.outputs = self.Outputs(id=20)

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


class SpawnConsumerProcesses(BaseJob):
    @dataclass
    class Inputs:
        num_workers_to_spawn: int

    async def run(self):
        cmd = f"python manage.py consume_jobs_async --max_num_workers={self.inputs.num_workers_to_spawn}"
        worker_procs = list()
        for _ in range(self.inputs.num_workers_to_spawn):
            worker_procs.append(await asyncio.create_subprocess_shell(cmd))

        while True:
            new_jobs_count = JobDBModel.new_jobs_count()
            if new_jobs_count == 0:
                for w in worker_procs:
                    w.terminate()
                break
            else:
                await asyncio.sleep(1)


class CreateJobs(BaseJob):
    @dataclass
    class Inputs:
        num_jobs_to_create: int
        num_workers_to_spawn: int

    async def run(self):
        jobs = [
            JobProducingOutputs(inputs=JobProducingOutputs.Inputs(id=i))
            for i in range(self.inputs.num_jobs_to_create)
        ]
        await abulk_create_new(jobs)

        self.next_job_inputs = SpawnConsumerProcesses.Inputs(
            num_workers_to_spawn=self.inputs.num_workers_to_spawn
        )


class DeleteExistingJobs(BaseJob):
    @dataclass
    class Inputs:
        num_jobs_to_create: int
        num_workers_to_spawn: int

    async def run(self):
        await JobDBModel.objects.adelete()
        self.next_job_inputs = CreateJobs.Inputs(
            num_jobs_to_create=self.inputs.num_jobs_to_create,
            num_workers_to_spawn=self.inputs.num_workers_to_spawn,
        )
