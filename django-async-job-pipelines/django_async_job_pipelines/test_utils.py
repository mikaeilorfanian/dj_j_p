from asgiref.sync import async_to_sync

from .job_runner import run_num_jobs


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
