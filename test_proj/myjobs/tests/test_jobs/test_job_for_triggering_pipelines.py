from asgiref.sync import async_to_sync
from django_async_job_pipelines.job_runner import run_num_jobs
from django_async_job_pipelines.models import JobDBModel, PipelineDBModel

from myjobs.jobs import JobWithInputs
from myjobs.pipelines import (
    MultipleJobsPipeline,
    OneJobPipelineWithInputs,
    TwoJobsPipeline,
)


class TestTriggeringThePipeline:
    def test_pipeline_with_one_job(self, db):
        job = async_to_sync(OneJobPipelineWithInputs.trigger)(
            inputs=JobWithInputs.Inputs(id=1)
        )
        async_to_sync(run_num_jobs)(1)

        pipeline_job = JobDBModel.get(pk=job.pk)
        assert pipeline_job
        assert pipeline_job.is_done
        assert pipeline_job.name == "StartPipeline"
        assert pipeline_job.inputs == {
            "first_job_inputs": {"id": 1},
            "pipeline_name": "OneJobPipelineWithInputs",
        }
        assert JobDBModel.new_jobs_count() == 1
        assert JobDBModel.not_ready_jobs_count() == 0

        new_jobs = JobDBModel.get_new_jobs_for_processing()
        assert len(new_jobs) == 1

        first_job_pk = new_jobs[0][0]
        first_job = JobDBModel.get(pk=first_job_pk)
        assert first_job.is_new
        assert first_job.previous_job

        assert PipelineDBModel.objects.count() == 1
        pipeline = PipelineDBModel.objects.first()
        assert pipeline.is_new
        assert len(pipeline.jobs.all()) == 1

    def test_pipeline_with_two_jobs(self, db):
        job = async_to_sync(TwoJobsPipeline.trigger)(inputs=JobWithInputs.Inputs(id=1))
        async_to_sync(run_num_jobs)(1)

        pipeline_job = JobDBModel.get(pk=job.pk)
        assert pipeline_job
        assert pipeline_job.is_done
        assert pipeline_job.name == "StartPipeline"
        assert pipeline_job.inputs == {
            "first_job_inputs": {"id": 1},
            "pipeline_name": "TwoJobsPipeline",
        }
        assert JobDBModel.new_jobs_count() == 1  # this is the first job
        assert JobDBModel.not_ready_jobs_count() == 1  # this is the second job

        new_jobs = JobDBModel.get_new_jobs_for_processing()
        assert len(new_jobs) == 1

        first_job_pk = new_jobs[0][0]
        first_job = JobDBModel.get(pk=first_job_pk)
        assert first_job.is_new
        assert first_job.previous_job

        assert PipelineDBModel.objects.count() == 1
        pipeline = PipelineDBModel.objects.first()
        assert pipeline.is_new
        assert len(pipeline.jobs.all()) == 2

    def test_pipeline_with_more_than_two_jobs_and_mix_of_jobs_with_inputs_and_without_and_failing_job(
        self, db
    ):
        job = async_to_sync(MultipleJobsPipeline.trigger)(
            inputs=JobWithInputs.Inputs(id=1)
        )
        async_to_sync(run_num_jobs)(1)

        pipeline_job = JobDBModel.get(pk=job.pk)
        assert pipeline_job
        assert pipeline_job.is_done
        assert pipeline_job.name == "StartPipeline"
        assert pipeline_job.inputs == {
            "first_job_inputs": {"id": 1},
            "pipeline_name": "MultipleJobsPipeline",
        }
        assert JobDBModel.new_jobs_count() == 1  # this is the first job
        assert (
            JobDBModel.not_ready_jobs_count() == 3
        )  # these are the rest of the jobs in the pipeline

        new_jobs = JobDBModel.get_new_jobs_for_processing()
        assert len(new_jobs) == 1

        first_job_pk = new_jobs[0][0]
        first_job = JobDBModel.get(pk=first_job_pk)
        assert first_job.is_new
        assert first_job.previous_job

        assert PipelineDBModel.objects.count() == 1
        pipeline = PipelineDBModel.objects.first()
        assert pipeline.is_new
        assert len(pipeline.jobs.all()) == 4
