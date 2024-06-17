# Django Async Job Pipelines

# Quick start

1. Add "polls" to your INSTALLED_APPS setting like this::

    INSTALLED_APPS = [
        ...,
        "django_async_job_pipelines",
    ]

2. Include the polls URLconf in your project urls.py like this::

    path("jobs/", include("django_async_job_pipelines.urls")),

3. Run ``python manage.py migrate`` to create the models.

4. Start the development server and visit the admin to create a poll.

5. Visit the ``/jobs/`` URL to participate in the poll.

# Compatibility
For now works only with postgres and psycopg3. Does it work with mysql and which driver library?
For use with sqlite3 you should run the sync version.

# Timeout
Useful for scenarios when you're testing and you want your test to include invoking the jobs runner.
It checks if roughly `timeout` seconds have passed since the start of command invocation.  
It won't exit early if there are any tasks in progress. It doesn't calcel pending tasks.

# Number of Jobs to Run
Useful for benchmarking. It's hard to know what number of workers is ideal for your scenario. That's why we have a built-in Django command that can create any number of jobs you want, run them, output the duration it took to run them, and assert that all have run.
`num_jobs_to_consume` runs only some number of jobs. This counts jobs which were processed successfully or failed.

# Create New Job

# Create Jobs in Bulk for Higher Performance

