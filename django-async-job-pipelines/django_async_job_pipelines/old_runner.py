import asyncio
from datetime import datetime as dt
from enum import Enum
import logging
import os
import traceback
from typing import Optional

from asgiref.sync import sync_to_async
from django.utils import timezone
from django_async_job_pipelines.models import JobDBModel


def logs_filename():
    return f"{os.getgid()}_job_runner.log"


logger = logging.getLogger(__name__)
FORMAT = "%(asctime)s %(process)d %(filename)s:%(funcName)s:%(lineno)d %(message)s"
logging.basicConfig(format=FORMAT, filename=logs_filename(), level=logging.INFO)


class RunResult(Enum):
    SUCCESS = "success"
    FAIL = "fail"
    NOT_RAN = "not_ran"


async def run_one_job(pk) -> RunResult:
    res = await JobDBModel.aupdate_new_to_in_progress_by_id(pk)
    if not res:
        logger.info(f"Could not update to 'in progress' job with pk {pk}")
        return RunResult.NOT_RAN

    job = await JobDBModel.aget_by_id(pk)
    try:
        logger.info(f"Running job with pk {pk}")
        await job.run()
        logger.info(f"Successfully ran job with pk {pk}")
    except Exception as e:
        logger.info(f"Failed to run job with pk {pk}")
        tb = traceback.format_exception(e)
        await JobDBModel.amark_as_failed(pk, ".".join(tb))
        return RunResult.FAIL

    await JobDBModel.aupdate_in_progress_to_done_by_id(pk)
    logger.info(f"Updated to 'done' job with pk {pk}")
    return RunResult.SUCCESS


def exit_due_to_timeout(start_dt: dt, timeout: int) -> bool:
    if not timeout:
        return False

    if (timezone.now() - start_dt).total_seconds() > timeout:
        return True

    return False


sleep_time_outer_loop = 0.1


def calculate_sleep_time_inner_loop(num_jobs_running):
    if num_jobs_running > 10:
        sleep_time_inner_loop = 1
    else:
        sleep_time_inner_loop = 0.006

    return sleep_time_inner_loop


async def run_num_jobs(num_jobs: int, timeout: int = 0):
    logger.info("Job runner started.")
    if not isinstance(timeout, int):
        raise ValueError("`timeout` should an `int`")

    start = timezone.now()
    num_jobs_ran = 0

    while True:
        while True:
            if exit_due_to_timeout(start, timeout):
                logger.info("Timeout reached exiting")
                return
            pks: list[tuple[int]] = await sync_to_async(
                JobDBModel.get_new_jobs_for_processing
            )()
            if len(pks) == 0:
                await asyncio.sleep(0.01)
                continue
            logger.info(f"Got pks for new jobs to run: {pks}")
            break

        num_jobs_to_run = num_jobs if num_jobs < len(pks) else len(pks)

        tasks = []
        pks_running = []
        for _ in range(num_jobs_to_run):
            pk = pks.pop()[0]
            pks_running.append(pk)
            t = asyncio.create_task(run_one_job(pk))
            tasks.append(t)
            # t.add_done_callback(tasks.discard)

        logger.info(f"Scheduled {num_jobs_to_run} jobs to run: {pks_running}.")

        done_tasks = 0
        while True:
            done_index: Optional[int] = None
            for index, t in enumerate(tasks):
                if t.done():
                    done_index = index
                    if t.result() != RunResult.NOT_RAN:
                        num_jobs_ran += 1
                    done_tasks += 1
                    break
            if done_index is not None:
                tasks.pop(done_index)
                continue
            if len(tasks) == 0:
                break
            to_sleep = calculate_sleep_time_inner_loop(num_jobs_to_run)
            logger.info(f"Going to sleep in the inner loop {to_sleep} seconds.")
            await asyncio.sleep(to_sleep)

        logger.info(f"All scheduled jobs have finished running.")

        if exit_due_to_timeout(start, timeout):
            logger.info("Timeout reached exiting")
            return

        if num_jobs_ran >= num_jobs:
            logger.info(f"Ran {num_jobs_to_run}, so exiting.")
            break

        logger.info(
            f"Going to sleep in the outer loop {sleep_time_outer_loop} seconds."
        )
        await asyncio.sleep(sleep_time_outer_loop)
