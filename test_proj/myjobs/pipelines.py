from django_async_job_pipelines.pipeline import BasePipeline

from .jobs import (
    CreateJobs,
    DeleteExistingJobs,
    JobForTests,
    JobWithInputs,
    JobWithSleep,
    SpawnConsumerProcesses,
)


class PipelineWithoutJobs(BasePipeline):
    pass


class OneJobPipelineWithInputs(BasePipeline):
    jobs = [JobWithInputs]


class OneJobPipelineWithoutInputs(BasePipeline):
    jobs = [JobForTests]


class TwoJobsPipeline(BasePipeline):
    jobs = [JobWithInputs, JobWithInputs]


class MultipleJobsPipeline(BasePipeline):
    jobs = [JobWithInputs, JobWithSleep, JobForTests, JobWithInputs]


class OneJobPipeline(BasePipeline):
    jobs = [JobWithSleep]


class PipelineTwoJobs(BasePipeline):
    jobs = [JobWithSleep, JobWithSleep]


class PipelineMultipleJobsOneInMiddleFails(BasePipeline):
    jobs = [JobWithSleep, JobWithInputs, JobWithSleep]


class TestPipelineWith10KJobs(BasePipeline):
    jobs = [DeleteExistingJobs, CreateJobs, SpawnConsumerProcesses]
