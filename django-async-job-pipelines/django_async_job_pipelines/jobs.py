from dataclasses import dataclass
from typing import Optional

from asgiref.sync import sync_to_async
from django.db import transaction
from django.utils.module_loading import import_module

from .job import BaseJob, acreate_new, create_not_ready
from .models import JobDBModel, PipelineDBModel
from .registry import pipeline_registery


class StartPipeline(BaseJob):
    @dataclass
    class Inputs:
        pipeline_name: str
        first_job_inputs: Optional[dict]

    async def run(self):
        """
        This method creates "not ready" jobs in db for the pipeline class
        specified in `Inputs.pipeline_name`.
        It creates a new pipeline row in db for this specific pipeline run.
        Then, this method creates associations between each of these jobs and
        the new pipeline row.
        Finally, it marks the first job to run as "new", so it can be picked up
        by the job runner.
        "not ready" jobs are not picked up by the job runner.
        """
        assert self.inputs

        pipeline_klass_name = self.inputs.pipeline_name
        if pipeline_klass_name not in pipeline_registery.pipeline_class_to_name_map:
            raise ValueError(
                f"Pipeline class named '{pipeline_klass_name}' is not in any registered Django app!"
            )
        module = import_module(
            pipeline_registery.get_import_path_for_class_name(pipeline_klass_name)
        )
        pipeline_klass = getattr(module, pipeline_klass_name)

        await self.run_db_queries_in_a_transaction(pipeline_klass)

    @sync_to_async
    def run_db_queries_in_a_transaction(self, pipeline_klass):
        """
        Creates a bunch of rows as shown below in db using a transaction.
        1. Creates a pipeline row.
        2. Creates a job row for each job in the pipeline class
        3. Creates a pipeline job row to associate each job row with the pipeline row from step 1
        4. Marks the first job as "new", so it can be picked up by the job runner.
        Note that the rest of the jobs are in "not ready" status which means they're not picked up by the
        job runner. The job runner is responsible for marking the next job in the pipeline as "new".
        """
        with transaction.atomic():
            pipeline = PipelineDBModel.create_new_in_db(pipeline_klass)

            first_job: bool = True
            # create "not ready" jobs
            for job_klass in pipeline_klass.jobs:
                if first_job:
                    assert self.inputs
                    if self.inputs.first_job_inputs:
                        if hasattr(job_klass, "Inputs"):
                            inputs = job_klass.Inputs(**self.inputs.first_job_inputs)
                        else:
                            raise ValueError(
                                f"First job inputs were given to the pipeline, but the job class {pipeline_klass} has no `Inputs` class within it!"
                            )

                        job_instance: BaseJob = job_klass.create(inputs=inputs)
                    else:
                        job_instance: BaseJob = job_klass.create()
                    first_job = False
                    first_job_db_model: JobDBModel = create_not_ready(
                        job_instance, self.db_model
                    )
                    pipeline.add_job(first_job_db_model)
                    prev_job: JobDBModel = first_job_db_model
                else:
                    # Here we don't want to enforce that inputs are given to the job (i.e. `check_inputs=False`)
                    # because at this point we don't know what the inputs are.
                    # The inputs are added to job row by the job runner because once the jobs finishes running
                    # we know the user has set the value of the next job's inputs.
                    job_instance: BaseJob = job_klass.create(check_inputs=False)
                    prev_job: JobDBModel = create_not_ready(job_instance, prev_job)
                    pipeline.add_job(prev_job)

            first_job_db_model.status = JobDBModel.JobStatus.NEW
            first_job_db_model.save()

            # create "not ready" job
            # create the pipeline-job row
            # set job status to "new"
