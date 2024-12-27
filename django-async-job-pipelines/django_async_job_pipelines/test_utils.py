import logging
import traceback
from enum import Enum

from asgiref.sync import async_to_sync

from .job_runner import run_num_jobs
from .models import JobDBModel

logger = logging.getLogger("django_async_job_pipelines")


def run_jobs(num_jobs_to_run: int = 1, timeout_seconds: int = 5, num_workers: int = 1):
    try:
        async_to_sync(run_num_jobs)(
            max_num_workers=num_workers,
            num_jobs=num_jobs_to_run,
            timeout=timeout_seconds,
        )
    except TimeoutError:
        raise RuntimeError(
            f"Running job consumer timed out while processing {num_jobs_to_run} jobs with a {timeout_seconds} seconds timeout."
        )


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

    output_serialized = job.outputs_asdict()
    await JobDBModel.aupdate_in_progress_to_done_by_id(pk, output_serialized)
    logger.info(f"Updated to 'done' job with pk {pk} with outputs {output_serialized}")
    return RunResult.SUCCESS
