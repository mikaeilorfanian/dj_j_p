import asyncio
import logging
import os
import time
import traceback
from dataclasses import Field, dataclass
from datetime import datetime as dt
from enum import Enum
from typing import Optional

from asgiref.sync import sync_to_async
from django.utils import timezone

from django_async_job_pipelines.models import JobDBModel


def logs_filename():
    return f"{os.getgid()}_job_runner.log"


logger = logging.getLogger(__name__)
FORMAT = "%(asctime)s %(process)d %(taskName)s %(filename)s:%(funcName)s:%(lineno)d %(message)s"
logging.basicConfig(format=FORMAT, filename=logs_filename(), level=logging.DEBUG)


class LimitReachedError(Exception):
    pass


@dataclass
class Runner:
    max_num_workers: int
    timeout_seconds: int = 0
    num_jobs_to_run: int = 0
    total_jobs_enqueued: int = 0
    total_jobs_processed: int = 0
    job_queue: Optional[asyncio.Queue] = None

    def __post_init__(self):
        """
        This job queue controls the maximum number of concurrent jobs to be run
        using `asyncio.Queue`.
        """
        self.job_queue = asyncio.Queue(maxsize=self.max_num_workers)

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
                    logger.info("No more enqueues since enough have been enqueued")
                    return
            logger.info(f"Total jobs enqueued {self.total_jobs_enqueued}")
            logger.info("Going to get job for processing")
            try:
                async with asyncio.timeout(0.2):
                    pk: int = await JobDBModel.aget_job_for_processing()
            except TimeoutError:
                logger.info("Getting jobs for processing timed out")
                continue
            # pks: list[int] = await JobDBModel.aget_new_jobs_for_processing(limit=1)
            # if not pks:
            #     sleep_seconds = 0.1
            #     logger.info(f"Got no pks so going to sleep {sleep_seconds} seconds.")
            #     await asyncio.sleep(sleep_seconds)
            #     continue
            # for pk in pks:
            if not pk:
                continue
            assert self.job_queue
            logger.info(f"Waiting to enqueue job with pk {pk}")
            await self.job_queue.put(pk)
            self.total_jobs_enqueued += 1
            logger.info(
                f"Added {pk} to job queue, total jobs enqueued: {self.total_jobs_enqueued}"
            )

    async def worker(self):
        logger.info("Worker started")
        assert self.job_queue

        while True:
            if self.num_jobs_to_run:
                if self.total_jobs_processed == self.num_jobs_to_run:
                    logger.info(
                        f"Limit reached, so exiting worker. Enqueued {self.total_jobs_enqueued}."
                    )
                    return

            logger.info(f"Waiting to get a job")
            try:
                async with asyncio.timeout(0.2):
                    pk = await self.job_queue.get()
            except TimeoutError:
                logger.info("Waiting to get job timed out")
                continue

            logger.info(f"Got pk {pk} to process.")
            # res = await JobDBModel.aupdate_new_to_in_progress_by_id(pk)
            # if not res:
            #     logger.info(f"Could not update to 'in progress' job with pk {pk}")
            #     self.job_queue.task_done()
            #     continue
            # logger.info(f"Updated to 'in progress' job with pk {pk}.")

            try:
                job = await JobDBModel.aget_by_id(pk)
            except:
                logger.exception(
                    f"Exception occured while getting job with pk {pk} from database."
                )
                self.job_queue.task_done()
                continue

            try:
                logger.info(f"Running job with pk {pk}")
                await job.run()
                logger.info(f"Successfully ran job with pk {pk}")
            except Exception as e:
                logger.info(f"Failed to run job with pk {pk}")
                tb = traceback.format_exception(e)
                await JobDBModel.amark_as_failed(pk, ".".join(tb))
                logger.info(f"Marked job with pk {pk} as 'failed' in db.")
                self.job_queue.task_done()
                self.total_jobs_processed += 1
                continue

            await JobDBModel.aupdate_in_progress_to_done_by_id(pk)
            logger.info(f"Updated to 'done' job with pk {pk}")
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
                logger.info("Scheduled the creation of a worker")
                tasks.append(task)
            await asyncio.gather(*tasks)


async def run_num_jobs(
    max_num_workers,
    num_jobs: int = 0,
    timeout: int = 0,
):
    logger.info("Job runner started.")
    if not isinstance(timeout, int):
        raise ValueError("`timeout` should an `int`")

    runner = Runner(
        max_num_workers=max_num_workers,
        num_jobs_to_run=num_jobs,
        timeout_seconds=timeout,
    )
    await runner.run()
