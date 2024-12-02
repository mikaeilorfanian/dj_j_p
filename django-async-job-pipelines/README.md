# Django Async Job Pipelines

The easiest way to do background jobs efficiently in Django. This package is a Django app you can include in your Django project effortlessly.

Features:
- massive concurrency supported using `async` job runners
- supports chaining of jobs (pipelines)
- uses your Django database as a job queue
- can be run on multiple OS processes simultaneously
- allows excluding jobs so you can run jobs that must be run on special machines (e.g. jobs requiring a GPU)

# The State of the Project
This project is under development. The public APIs are solid and breaking changes will be noted in the docs (this readme for now).

The project is very well tested and its performance is tuned and monitored closely.

# Quick start
1. Install using `pip`: `pip install X`
2. Add "django_async_job_pipelines" to your INSTALLED_APPS setting like this::

    INSTALLED_APPS = [
        ...,
        "django_async_job_pipelines",
    ]
3. Run `python manage.py migrate` to create the models.
4. Start the development server and visit the admin to see the jobs you've created.

# Compatibility
We've tested compatibility with postgres (with psycopg3), sqlite3, Django>=X and Python>=X. 

# Usage
# Creating Jobs
Create a `jobs.py` module in one of your Django's apps root (where `models.py` is). In it you define your jobs. For example:
```python
from django_async_job_pipelines.job import BaseJob

class JobWithSleep(BaseJob):
    async def run(self):
        await asyncio.sleep(0.1)
```
`run` must be defined and is called by the job runner when this jobs runs. This is where you implement what this jobs does.
To send an instnace of this job to the job queue:
```python
from django_async_job_pipelines.job import acreate_new

job = await acreate_new(JobWithSleep)
```
You `await` the `acreate_new` function while it puts the job in a database table. If you have your job runner process it'll pick up this job and run it.

`job` here is a Django model instance. You can use this job to locate it in your db. Since your jobs are in the db, you can use Django's ORM feature with them, e.g. to find all the jobs that have finished running:
```python
from django_async_job_pipelines.models import JobDBModel

JobDBModel.objects.get(id=job.id)
```
Each job has a `name` which is the name of the job class. In this example this job's name in the database is `JobWithSleep`. Using this name you can exclude jobs from running when you invoke the job runner. More on this later.

What if you want your job to take inputs or produce outputs?
```python
from dataclasses import dataclass

class JobWithInputsAndOutputs(BaseJob):
    @dataclass
    class Inputs:
        id: int

    @dataclass
    class Outputs:
        id: int

    async def run(self):
        self.outputs = self.Outputs(id=self.intputs.id * 2)

# send an instance of it to the job queue
job = ascreate_new(JobWithInputsAndOutputs.create(JobWithInputsAndOutputs.Inputs(id=10)))
```
Several things are going on here:
- `Inputs` and `Outputs` are `dataclass` decorated classes within the job class. This helps with type safety.
- Inputs are passed to the job's `create` class method as an instance of the job's `Inputs` class.
- Outputs are set to `self.outputs` as an instance of the job's `Outputs`.
- When the job finishes running, its output gets persited to the database. You can find it using `job.id`.

To make job creation more performant pass a list of jobs to `django_async_job_pipelines.job.abulk_create_new`.

## Inputs and Outputs
The job class inheriting from `BaseJob` should have an `Inputs` class and/or `Outputs` class if you want the job to take inputs and produce outputs which get written to the database. This is useful when you want to pass data to other jobs, for example when using a `pipeline`. Pipelines are discussed later.

Inputs and outputs should be a Python `dict`. So, they cannot be a Python `list`, `set`, etc.

It's recommended to use a `dataclass` as your inputs and outputs classes. This way most of serialization and deserialization is taken care of by this package.
If you want to customize how the intputs and outputs look like as a `dict` then define a `asdict` method on `Inputs` and `Outputs` which takes no arguments. 

### TODO Customize Inputs and Outputs Serialization example

# Running Jobs
To start the bakcground jobs runner:
```bash
python manage.py consume_jobs_async
```
Use `python manage.py consume_jobs_async --help` to see how to customize the job runner.
By default, the job runner runs forever.

## Testing Utils
### Timeout
`timeout` is an arguemnt you can pass to the job runner when your tests require the invocation of the job runner.
The job runner checks if roughly `timeout` seconds have passed since the start of command invocation.  
It won't exit if there are any tasks in progress.

### Number of Jobs to Run
For testing purposes you can also invoke the job runner like so:
```python
import asyncio
from django_async_job_pipelines.job_runner import run_num_jobs


asyncio.run(run_num_jobs(max_num_workers=1, timeout=2, num_jobs=2))
```
This will create one `async` job runner which processes two jobs and exits. If the jobs take more than two seconds in total to run, the job runner will time out.

### Excluding Jobs
You can pass an optional comma-separated set of job names (the job name is the name of the class which inherits from the `BaseJob` class) to the `consume_jobs_async` Django command so the consumer skips them.
Note that this list of names is not validated. 

### Benchmarking
TODO
Useful for benchmarking. It's hard to know what number of workers is ideal for your scenario. That's why we have a built-in Django command that can create any number of jobs you want, run them, output the duration it took to run them, and assert that all have run.
`num_jobs_to_consume` runs only some number of jobs. This counts jobs which were processed successfully or failed.

# TODO remove runner 1 and its related code and tests

job.name should not be set by users!


# Optimization
TODO
To cut the number of databbase queries for fetching rows which are ready to be processed by a factor of 10 set the X config option like below:
