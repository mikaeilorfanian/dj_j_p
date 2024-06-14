from django_async_job_pipelines.registry import job_registery


def test_job_registry_is_able_to_find_job_classes():
    assert len(job_registery.job_class_to_name_map) > 0
