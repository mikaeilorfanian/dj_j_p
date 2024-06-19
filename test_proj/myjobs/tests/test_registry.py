import pytest
from django_async_job_pipelines.registry import job_registery, pipeline_registery

NUM_BUILT_IN_JOBS = 1
NUM_TEST_JOBS = 9
NUM_BUILT_IN_PIPELINES = 0
NUM_TEST_PIPELINES = 0


class TestJobRegistery:
    def test_job_registry_is_able_to_find_job_classes(
        self,
    ):
        assert (
            len(job_registery.job_class_to_name_map)
            == NUM_BUILT_IN_JOBS + NUM_TEST_JOBS
        )

    def test_non_existing_job_is_not_in_registry_and_raises_exception(self):
        with pytest.raises(KeyError):
            job_registery.get_import_path_for_class_name("blahblah")

    def test_built_in_jobs_are_picked_up(self):
        # TODO implement this test
        pass


class TestPipelineRegistery:
    def test_pipeline_registry_is_able_to_find_pipeline_classes(
        self,
    ):
        assert (
            len(pipeline_registery.pipeline_class_to_name_map)
            == NUM_BUILT_IN_PIPELINES + NUM_TEST_PIPELINES
        )

    def test_non_existing_pipeline_is_not_in_registry_and_raises_exception(self):
        with pytest.raises(KeyError):
            pipeline_registery.get_import_path_for_class_name("blahblah")
