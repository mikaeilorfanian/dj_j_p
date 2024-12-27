import asyncio
import logging
import traceback
from dataclasses import dataclass
from typing import Optional

from django_async_job_pipelines.job import BaseJob
from django_async_job_pipelines.models import JobDBModel


class LimitReachedError(Exception):
    pass


logger = logging.getLogger("django_async_job_pipelines")


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
        if self.get_job_to_process_timeout <= self.wait_seconds_between_queries + 0.5:
            self.get_job_to_process_timeout = self.wait_seconds_between_queries + 0.5

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
            try:
                if self.num_jobs_to_run > 0:
                    if self.total_jobs_enqueued == self.num_jobs_to_run:
                        logger.debug("No more enqueues since enough have been enqueued")
                        return
                logger.debug("Going to get job for enqueueing")

                pk: int | None = None
                try:
                    async with asyncio.timeout(self.get_job_to_process_timeout):
                        if self.exclude_jobs:
                            task = asyncio.create_task(
                                JobDBModel.aget_job_for_processing(
                                    exclude=self.exclude_jobs,
                                    wait_seconds_between_queries=self.wait_seconds_between_queries,
                                )
                            )
                            await task
                            pk = task.result()
                        else:
                            task = asyncio.create_task(
                                JobDBModel.aget_job_for_processing(
                                    wait_seconds_between_queries=self.wait_seconds_between_queries
                                )
                            )
                            await task
                            pk = task.result()
                except TimeoutError:
                    sleep_between_fetching_jobs_for_processing = 1
                    logger.debug(
                        f"Getting job for processing timed out, so going to sleep {sleep_between_fetching_jobs_for_processing}"
                    )
                    await asyncio.sleep(sleep_between_fetching_jobs_for_processing)

                if not pk:
                    continue

                assert self.job_queue
                logger.debug(f"Waiting to enqueue job with pk {pk}")
                await self.job_queue.put(pk)
                self.total_jobs_enqueued += 1
                logger.debug(
                    f"Added job with pk {pk} to job queue, total jobs enqueued: {self.total_jobs_enqueued}"
                )
            except asyncio.CancelledError as e:
                task = asyncio.create_task(self.cleanup_jobs(pk))
                await asyncio.shield(task)
                raise e

    async def worker(self, worker_id: int):
        """This is where we run jobs, and start the next jobs."""
        worker_id_msg = f"Worker ID {worker_id}:"
        logger.debug(f"{worker_id_msg} Worker started")
        assert self.job_queue

        try:
            pk: int | None = None
            while True:
                if self.num_jobs_to_run:
                    if self.total_jobs_processed == self.num_jobs_to_run:
                        logger.debug(
                            f"{worker_id_msg} Num job to process limit reached, so exiting worker. Enqueued {self.total_jobs_enqueued}."
                        )
                        return

                logger.debug(f"{worker_id_msg} Waiting to get a job")
                try:
                    async with asyncio.timeout(self.get_job_from_queue_timeout):
                        pk = await self.job_queue.get()
                except TimeoutError:
                    sleep_between_waiting_to_get_job = 1
                    logger.debug(
                        f"{worker_id_msg} Timeout getting job from job queue, going to sleep {sleep_between_waiting_to_get_job}"
                    )
                    await asyncio.sleep(sleep_between_waiting_to_get_job)
                    continue

                logger.debug(f"{worker_id_msg} Got job from job queue to process: {pk}")

                try:
                    job: BaseJob = await JobDBModel.aget_by_id(pk)
                except:
                    logger.exception(
                        f"{worker_id_msg} Exception occured while getting job with pk {pk} from database."
                    )
                    self.job_queue.task_done()
                    continue

                try:
                    logger.debug(f"{worker_id_msg} Running job with pk {pk}")
                    await job.run()  # run the job
                    logger.debug(f"{worker_id_msg} Ran job with pk {pk}")
                    if job.previous_job:  # this means this job is part of a pipeline
                        logger.debug(f"{worker_id_msg} Job part of pipeline {pk}")
                        assert job.db_model

                        next_job_inputs = job.next_job_inputs_asdict()
                        if next_job_inputs:
                            logger.debug(f"Job {pk} has next job inputs")

                        if isinstance(next_job_inputs, list):
                            logger.debug(
                                f"{worker_id_msg} There will be multiple next jobs {pk}"
                            )
                            # makes the next jobs to be run in parallel
                            for next_j_inputs in next_job_inputs:
                                next_job: JobDBModel | None = (
                                    await JobDBModel.ainit_next_job(
                                        job.db_model, next_j_inputs
                                    )
                                )
                                logger.debug(
                                    f"{worker_id_msg} Next job created {next_job.pk if next_job else None}"
                                )
                        elif next_job_inputs:
                            next_job = await JobDBModel.ainit_next_job(
                                job.db_model,
                                next_job_inputs,
                            )
                            logger.debug(
                                f"{worker_id_msg} Next job created {next_job.pk if next_job else None}"
                            )
                    output_serialized = job.outputs_asdict()
                    logger.debug(f"{worker_id_msg} Successfully ran job with pk {pk}")
                    await JobDBModel.aupdate_in_progress_to_done_by_id(
                        pk, output_serialized
                    )
                    logger.debug(f"{worker_id_msg} Updated to 'done' job with pk {pk}")
                    self.job_queue.task_done()
                    logger.debug(f"{worker_id_msg} Acked msg from queue: {pk}")
                    self.total_jobs_processed += 1
                except Exception as e:
                    logger.debug(f"{worker_id_msg} Failed to run job with pk {pk}")
                    tb = traceback.format_exception(e)
                    await JobDBModel.amark_as_failed(pk, ".".join(tb))
                    logger.debug(
                        f"{worker_id_msg} Marked job with pk {pk} as 'failed' in db."
                    )
                    if job.outputs_asdict():
                        await JobDBModel.asave_job_outputs(
                            pk=pk, job_outputs=job.outputs_asdict()
                        )
                    self.job_queue.task_done()
                    self.total_jobs_processed += 1
        except (asyncio.CancelledError, TimeoutError, KeyboardInterrupt) as e:
            logger.debug(f"Got Cancelled, returning all jobs to NEW")
            task = asyncio.create_task(self.cleanup_jobs(pk))
            await asyncio.shield(task)
            raise e

    async def cleanup_jobs(self, pk: int | None = None):
        logger.debug(f"Q size: {self.job_queue.qsize()}")
        if pk:
            logger.debug(f"Returning job to NEW: {pk}")
            await JobDBModel.amark_as_new_by_pk(pk)
            logger.debug(f"Returned job to NEW: {pk}")
        while self.job_queue.qsize() > 0:
            pk = await self.job_queue.get()
            await JobDBModel.amark_as_new_by_pk(pk)
            logger.debug(f"Returned job to NEW: {pk}")

    async def run(self):
        if self.max_num_workers < 1:
            raise ValueError("Max number of workers cannot be smaller than one!")

        if self.timeout_seconds:
            try:
                async with asyncio.timeout(self.timeout_seconds):
                    tasks = []
                    tasks.append(asyncio.create_task(self.add_jobs_to_queue()))
                    for worker_id in range(self.max_num_workers):
                        task = asyncio.create_task(self.worker(worker_id))
                        logger.debug("Scheduled the creation of a worker")
                        tasks.append(task)
                    await asyncio.gather(*tasks)
            except TimeoutError:
                logger.debug(f"Timeout reached: {self.timeout_seconds} seconds!")
                return
        else:
            tasks = []
            tasks.append(asyncio.create_task(self.add_jobs_to_queue()))
            for worker_id in range(self.max_num_workers):
                task = asyncio.create_task(self.worker(worker_id))
                logger.debug("Scheduled the creation of a worker")
                tasks.append(task)
            await asyncio.gather(*tasks)


async def run_num_jobs(
    max_num_workers,
    num_jobs: int = 0,
    timeout: int = 0,
    skip_jobs: Optional[list[str]] = None,
):
    logger.debug(
        f"Job runner started with {max_num_workers=}, {num_jobs=}, {timeout=}, {skip_jobs=}"
    )
    if not isinstance(timeout, int):
        raise ValueError("`timeout` should an `int`")

    runner = Runner(
        max_num_workers=max_num_workers,
        num_jobs_to_run=num_jobs,
        timeout_seconds=timeout,
        exclude_jobs=skip_jobs,
    )
    await runner.run()
