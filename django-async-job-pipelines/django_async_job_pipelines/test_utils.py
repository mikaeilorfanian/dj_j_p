from asgiref.sync import async_to_sync

from .job_runner import run_num_jobs


def run_jobs(run: int = 1, timeout_seconds: int = 5):
    try:
        async_to_sync(run_num_jobs)(
            max_num_workers=1, num_jobs=run, timeout=timeout_seconds
        )
    except TimeoutError:
        raise RuntimeError(
            f"Running job consumer timed out whicle processing {run} jobs with a {timeout_seconds} seconds timeout."
        )
