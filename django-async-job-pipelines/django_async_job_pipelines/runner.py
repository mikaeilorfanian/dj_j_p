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
    if not pk:
        return RunResult.NOT_RAN

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


async def run_num_jobs(num_jobs: int, timeout: int = 0):
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
            if done_index:
                tasks.pop(done_index)
                continue
            if done_tasks >= len(tasks):
                break
            await asyncio.sleep(0.01)

        if exit_due_to_timeout(start, timeout):
            return

        if num_jobs_ran >= num_jobs:
            break

        await asyncio.sleep(0.01)
