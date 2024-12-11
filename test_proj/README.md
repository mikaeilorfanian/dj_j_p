# Description
This app tests the `django-async-job-pipelines` package hosted here.
It also includes some benchmarks.

# Benchmarks
All benchmarks were ran on a WSL2 Ubuntu 24 on Windows 11 with Postgres 15 running in a Docker container.
My machine has 16GB of memory at 5200 MHz and a i9-13900H CPU.
I also had these apps open while running these benchmarks: a dozen browser tabs, DBeaver, Slack, and Teams.
Memory usage peaked at 88% and CPU at 85% of 20 cores during the most demanding testing.

## Pipeline Benchmarks
The following table documents the performance of the `run_big_test` Django command.
This command does the following:
- Deletes all existing jobs
- Creates a pipeline that
    - Creates a number of jobs to process (each job just produces an integer output which gets persisted to the database)
    - Spawns one or more OS processes each with one or more workers to process the newly created jobs
    - Asserts if the number of jobs processed equals the number of jobs created

| No. Jobs to Create | No. Processes | No. Workers per Process | Time to Process (seconds) |
| ------------------ | ------------- | ----------------------- | ------------------------- | 
| 100 | 1 | 1 | 12 |
| 100| 2 | 2 | 12 |
| 1000 | 1 | 1 | 16 |
| 1000 | 2 | 2 | 16 |
| 10000 | 1 | 1 | 62 |
| 10000 | 4 | 4 | 35 |
| 10000 | 10 | 10 | 45 |
| 100000 | 4 | 4 | 765 |
| 100000 | 10 | 10 | 765 |

## Worker Benchmarks
The following table documents the performance of the `consume_jobs_async` Django command.
This command consumer a number of jobs where each job just produces an integer output which gets persisted to the database.

|No. Jobs to Consume|No. Processes|No. Workers per Process|Time to Process (seconds)|
| ------------------ | ------------- | ----------------------- | ------------------------- | 
|100|1|1|13|
|100|2|2|13|
|1000|1|1|20|
|1000|2|2|10|
|10000|1|1|70|
|10000|4|4|40|
|10000|10|10|40|
|100000|4|4|765|
|100000|10|10|765|
