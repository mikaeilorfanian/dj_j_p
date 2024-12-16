import time
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


class AssertPipelieWorkedProperly(BaseJob):
    @dataclass
    class Inputs:
        num_done_jobs: int

    @dataclass
    class Outputs:
        actual_num_done_jobs: int

    async def run(self):
        actual_done_jobs = await JobDBModel.objects.filter(
            status=JobDBModel.JobStatus.DONE
        ).acount()
        self.outputs = self.Outputs(actual_num_done_jobs=actual_done_jobs)
        assert actual_done_jobs == self.inputs.num_done_jobs


class SpawnConsumerProcesses(BaseJob):
    @dataclass
    class Inputs:
        num_workers_to_spawn: int
        worker_timeout: int
        num_done_jobs: int
        num_processes_to_spawn: int

    async def run(self):
        cmd = f"python manage.py consume_jobs_async --max_num_workers={self.inputs.num_workers_to_spawn} --timeout={str(self.inputs.worker_timeout)}"
        worker_procs = list()
        for _ in range(self.inputs.num_processes_to_spawn):
            worker_procs.append(await asyncio.create_subprocess_shell(cmd))

        while True:
            new_jobs_count = await JobDBModel.objects.filter(
                status=JobDBModel.JobStatus.NEW
            ).acount()
            if new_jobs_count == 0:
                for w in worker_procs:
                    w.kill()
                for w in worker_procs:
                    while True:
                        rc = w.returncode
                        if rc is None:
                            await asyncio.sleep(1)
                        else:
                            break
                break
            else:
                await asyncio.sleep(1)

        self.next_job_inputs = AssertPipelieWorkedProperly.Inputs(
            num_done_jobs=self.inputs.num_done_jobs
        )

        time.sleep(10)


class CreateJobs(BaseJob):
    @dataclass
    class Inputs:
        num_jobs_to_create: int
        num_workers_to_spawn: int
        worker_timeout: int
        num_done_jobs: int
        num_processes_to_spawn: int

    async def run(self):
        async with asyncio.TaskGroup() as tg:
            num_jobs = self.inputs.num_jobs_to_create
            batch_size = 10_000
            if num_jobs > batch_size:
                created_so_far = 0
                while created_so_far < num_jobs:
                    batch_size = 10_000
                    if num_jobs - created_so_far < batch_size:
                        batch_size = num_jobs - created_so_far
                    jobs = [
                        JobProducingOutputs(inputs=JobProducingOutputs.Inputs(id=i))
                        for i in range(batch_size)
                    ]
                    tg.create_task(abulk_create_new(jobs))
                    created_so_far += batch_size
            else:
                jobs = [
                    JobProducingOutputs(inputs=JobProducingOutputs.Inputs(id=i))
                    for i in range(num_jobs)
                ]
                await abulk_create_new(jobs)

        self.next_job_inputs = SpawnConsumerProcesses.Inputs(
            num_workers_to_spawn=self.inputs.num_workers_to_spawn,
            worker_timeout=self.inputs.worker_timeout,
            num_done_jobs=self.inputs.num_done_jobs,
            num_processes_to_spawn=self.inputs.num_processes_to_spawn,
        )


class DeleteExistingJobs(BaseJob):
    @dataclass
    class Inputs:
        num_jobs_to_create: int
        num_workers_to_spawn: int
        worker_timeout: int
        num_done_jobs: int
        num_processes_to_spawn: int

    async def run(self):
        self.next_job_inputs = CreateJobs.Inputs(
            num_jobs_to_create=self.inputs.num_jobs_to_create,
            num_workers_to_spawn=self.inputs.num_workers_to_spawn,
            worker_timeout=self.inputs.worker_timeout,
            num_done_jobs=self.inputs.num_done_jobs,
            num_processes_to_spawn=self.inputs.num_processes_to_spawn,
        )


class JobWithLongSleep(BaseJob):
    async def run(self):
        await asyncio.sleep(1_000)


class JobWithInputsForMultipleNextJobs(BaseJob):
    async def run(self):
        self.outputs = self.Outputs(id=20)
        self.next_job_inputs = [
            JobWithInputs.Inputs(id=i) for i in range(self.inputs.jobs_to_make)
        ]

    @dataclass
    class Inputs:
        jobs_to_make: int

        def asdict(self):
            return {"jobs_to_make": self.jobs_to_make}

    @dataclass
    class Outputs:
        id: int

        def asdict(self):
            return {"id": self.id}
