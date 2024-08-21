# Django Async Job Pipelines

# Quick start

1. Add "django_async_job_pipelines" to your INSTALLED_APPS setting like this::

    INSTALLED_APPS = [
        ...,
        "django_async_job_pipelines",
    ]

2. Include the polls URLconf in your project urls.py like this::

    path("jobs/", include("django_async_job_pipelines.urls")),

3. Run `python manage.py migrate` to create the models.

4. Start the development server and visit the admin to create a poll.

5. Visit the `/jobs/` URL to participate in the poll.

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
Use `job.acreate_new` 

# Create Jobs in Bulk for Higher Performance
Use `job.abulk_create_new`

# Excluding Jobs
You can pass an optional list of job names (remember job name is the name of the class inheriting from `BaseJob` class) to the `consume_jobs` Django command so that you consumer skips them.
Note that this list of names is not validated.

# Inputs and Outputs
Your job class inheriting from `BaseJob` should have an `Inputs` class and/or `Outputs` class if you wish your job to take inputs and produce outputs which get written to the database. This is useful when you want to pass data to other jobs, for example when using a pipeline.
These should always be deserializable to a Python `dict`. They cannot be a JSON `array` (Python `list`) or Python `set`.
It's recommended to use a `dataclass` as your inputs and outputs classes. This way most of serialization and deserialization is taken care of by this library.
If you want to use your own classes then you must provide a `inputs_serialize` method for the `Inputs` class and `outputs_serializer` method for the `Outputs` class.

# Customize Inputs and Outputs Serialization
# TODO how about customizing deserialization
In `Inputs` or `Outputs` classes of your job create a `asdict` method which takes no arguments.


# TODO remove runner 1 and its related code and tests

job.name should not be set by users!


# Optimization
To cut the number of databbase queries for fetching rows which are ready to be processed by a factor of 10 set the X config option like below:
