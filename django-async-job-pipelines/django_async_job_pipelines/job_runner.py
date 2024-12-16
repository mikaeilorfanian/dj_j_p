import asyncio
import logging
import os
import traceback
from dataclasses import dataclass
from typing import Optional

from django_async_job_pipelines.job import BaseJob
from django_async_job_pipelines.models import JobDBModel


def logs_filename():
    return f"{os.getgid()}_job_runner.log"


logger = logging.getLogger(__name__)
FORMAT = "%(asctime)s %(process)d %(levelname)s %(process)d %(taskName)s %(filename)s:%(funcName)s:%(lineno)d %(message)s"
logging.basicConfig(format=FORMAT, filename=logs_filename(), level=logging.DEBUG)


class LimitReachedError(Exception):
    pass


LOG_TO_FILE = False


class Logger:
    def info(self, msg):
        if not LOG_TO_FILE:
            return
        logger.info(msg)

    def exception(self, msg):
        if not LOG_TO_FILE:
            return
        logger.exception(msg)


_logger = Logger()


@dataclass
class Runner:
    max_num_workers: int
    timeout_seconds: int = 0
    num_jobs_to_run: int = 0
    total_jobs_enqueued: int = 0
    total_jobs_processed: int = 0
    get_job_to_process_timeout: float = 0.4
    get_job_from_queue_timeout: float = 0.1
    wait_seconds_between_queries: float = 0.2
    job_queue: Optional[asyncio.Queue] = None
    exclude_jobs: Optional[list[str]] = None

    def __post_init__(self):
        """
        This job queue controls the maximum number of concurrent jobs to be run
        using `asyncio.Queue`.
        """
        self.job_queue = asyncio.Queue(maxsize=self.max_num_workers)
        if self.get_job_to_process_timeout <= self.wait_seconds_between_queries:
            self.get_job_to_process_timeout = self.wait_seconds_between_queries + 0.2

    async def add_jobs_to_queue(self):
        """
        This function enforces the total number of jobs to run. This number is passed to the
        initializer of this class and is optional. Once this number is reached
        this function doesn't enqueue any more jobs to be run and returns.
        The max number of jobs enqueued is always limited to the max number of workers.
        If this number is reached this function blocks until the queue has empty slots
        as result of a worker calling `get` on this queue.
        """
        if self.num_jobs_to_run > 0:
            if self.total_jobs_enqueued > self.num_jobs_to_run:
                return

        # TODO What if this return no PKs?
        # TODO Do manual/autoamted QA for this
        while True:
            if self.num_jobs_to_run > 0:
                if self.total_jobs_enqueued == self.num_jobs_to_run:
                    _logger.info("No more enqueues since enough have been enqueued")
                    return
            _logger.info(f"Total jobs enqueued {self.total_jobs_enqueued}")
            _logger.info("Going to get job for processing")

            try:
                async with asyncio.timeout(self.get_job_to_process_timeout):
                    if self.exclude_jobs:
                        pk: int = await JobDBModel.aget_job_for_processing(
                            exclude=self.exclude_jobs,
                            wait_seconds_between_queries=self.wait_seconds_between_queries,
                        )
                    else:
                        pk: int = await JobDBModel.aget_job_for_processing(
                            wait_seconds_between_queries=self.wait_seconds_between_queries
                        )
            except TimeoutError:
                _logger.info("Getting jobs for processing timed out")
                continue

            if not pk:
                continue

            assert self.job_queue
            _logger.info(f"Waiting to enqueue job with pk {pk}")
            await self.job_queue.put(pk)
            self.total_jobs_enqueued += 1
            _logger.info(
                f"Added job with pk {pk} to job queue, total jobs enqueued: {self.total_jobs_enqueued}"
            )

    async def worker(self):
        """This is where we run jobs, and start the next jobs."""
        _logger.info("Worker started")
        assert self.job_queue

        while True:
            if self.num_jobs_to_run:
                if self.total_jobs_processed == self.num_jobs_to_run:
                    _logger.info(
                        f"Limit reached, so exiting worker. Enqueued {self.total_jobs_enqueued}."
                    )
                    return

            _logger.info(f"Waiting to get a job")
            try:
                async with asyncio.timeout(self.get_job_from_queue_timeout):
                    pk = await self.job_queue.get()
            except TimeoutError:
                _logger.info("Timeout while waiting to get job")
                continue

            _logger.info(f"Got pk {pk} to process.")

            try:
                job: BaseJob = await JobDBModel.aget_by_id(pk)
            except:
                _logger.exception(
                    f"Exception occured while getting job with pk {pk} from database."
                )
                self.job_queue.task_done()
                continue

            try:
                _logger.info(f"Running job with pk {pk}")
                await job.run()  # run the job
                if job.previous_job:  # this means this job is part of a pipeline
                    next_job_inputs = job.next_job_inputs_asdict()
                    assert job.db_model

                    if isinstance(next_job_inputs, list):
                        for next_j_inputs in next_job_inputs:
                            await JobDBModel.ainit_next_job(job.db_model, next_j_inputs)
                    else:
                        await JobDBModel.ainit_next_job(
                            job.db_model,
                            next_job_inputs,
                        )
                output_serialized = job.outputs_asdict()
                _logger.info(f"Successfully ran job with pk {pk}")
                await JobDBModel.aupdate_in_progress_to_done_by_id(
                    pk, output_serialized
                )
                _logger.info(f"Updated to 'done' job with pk {pk}")
                self.job_queue.task_done()
                self.total_jobs_processed += 1
            except Exception as e:
                _logger.info(f"Failed to run job with pk {pk}")
                tb = traceback.format_exception(e)
                await JobDBModel.amark_as_failed(pk, ".".join(tb))
                _logger.info(f"Marked job with pk {pk} as 'failed' in db.")
                if job.outputs_asdict():
                    await JobDBModel.asave_job_outputs(
                        pk=pk, job_outputs=job.outputs_asdict()
                    )
                self.job_queue.task_done()
                self.total_jobs_processed += 1

    async def run(self):
        if self.max_num_workers < 1:
            raise ValueError("Max number of workers cannot be smaller than one!")

        if self.timeout_seconds:
            try:
                async with asyncio.timeout(self.timeout_seconds):
                    tasks = []
                    tasks.append(asyncio.create_task(self.add_jobs_to_queue()))
                    for _ in range(self.max_num_workers):
                        task = asyncio.create_task(self.worker())
                        tasks.append(task)
                    await asyncio.gather(*tasks)
            except TimeoutError:
                return
        else:
            tasks = []
            tasks.append(asyncio.create_task(self.add_jobs_to_queue()))
            for _ in range(self.max_num_workers):
                task = asyncio.create_task(self.worker())
                _logger.info("Scheduled the creation of a worker")
                tasks.append(task)
            await asyncio.gather(*tasks)


async def run_num_jobs(
    max_num_workers,
    num_jobs: int = 0,
    timeout: int = 0,
    skip_jobs: Optional[list[str]] = None,
):
    _logger.info("Job runner started.")
    if not isinstance(timeout, int):
        raise ValueError("`timeout` should an `int`")

    runner = Runner(
        max_num_workers=max_num_workers,
        num_jobs_to_run=num_jobs,
        timeout_seconds=timeout,
        exclude_jobs=skip_jobs,
    )
    await runner.run()
