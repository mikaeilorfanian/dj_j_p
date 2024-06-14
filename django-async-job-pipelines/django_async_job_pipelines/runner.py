import asyncio
from datetime import datetime as dt
from enum import Enum
import traceback
from typing import Optional

from asgiref.sync import sync_to_async
from django.utils import timezone
from django_async_job_pipelines.models import JobDBModel


class RunResult(Enum):
    SUCCESS = "success"
    FAIL = "fail"
    NOT_RAN = "not_ran"


async def run_one_job(pk) -> RunResult:
    res = await JobDBModel.aupdate_new_to_in_progress_by_id(pk)
    if not res:
        return RunResult.NOT_RAN

    job = await JobDBModel.aget_by_id(pk)
    try:
        await job.run()
    except Exception as e:
        tb = traceback.format_exception(e)
        await JobDBModel.amark_as_failed(pk, ".".join(tb))
        return RunResult.FAIL

    await JobDBModel.aupdate_in_progress_to_done_by_id(pk)
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
        sleep_time_inner_loop = 0.001

    return sleep_time_inner_loop


async def run_num_jobs(num_jobs: int, timeout: int = 0):
    if not isinstance(timeout, int):
        raise ValueError("`timeout` should an `int`")
    start = timezone.now()
    num_jobs_ran = 0

    while True:
        while True:
            if exit_due_to_timeout(start, timeout):
                return
            pks: list[tuple[int]] = await sync_to_async(
                JobDBModel.get_new_jobs_for_processing
            )()
            if len(pks) == 0:
                await asyncio.sleep(0.01)
                continue
            break

        num_jobs_to_run = num_jobs if num_jobs < len(pks) else len(pks)

        tasks = []
        for _ in range(num_jobs_to_run):
            pk = pks.pop()[0]
            t = asyncio.create_task(run_one_job(pk))
            tasks.append(t)
            # t.add_done_callback(tasks.discard)

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
            await asyncio.sleep(calculate_sleep_time_inner_loop(num_jobs_to_run))

        if exit_due_to_timeout(start, timeout):
            return

        if num_jobs_ran >= num_jobs:
            break

        await asyncio.sleep(sleep_time_outer_loop)
