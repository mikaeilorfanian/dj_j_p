from importlib import import_module

from django.apps import AppConfig, apps


class DjangoAsyncJobPipelinesConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "django_async_job_pipelines"

    def ready(self):
        """
        Creates a registry of jobs. This is used to store job names in db, so the job code can
        be found once the job needs to run.
        For each Django app checks if the app has a `jobs` module.
        If yes, then it checks all attributes in that `jobs` module to find subclasses of `BaseJob`.
        """
        from .job import BaseJob
        from .pipeline import BasePipeline
        from .registry import job_registery, pipeline_registery

        for (
            app
        ) in apps.get_app_configs():  # get all registered and collected Djang apps
            try:  # try to get the `jobs` module and ignore if it doesn't exist
                jobs_module = import_module(f"{app.name}.jobs")
                for obj in dir(
                    jobs_module
                ):  # get all attributes (as string) of `jobs` module
                    obj = getattr(jobs_module, obj)  # turn string to the object itself
                    if obj is BaseJob:  # we don't want to register the `BaseJob` class
                        continue
                    if (
                        type(obj) is type
                    ):  # assert `obj` is a class and not a `dict` or some other builtin
                        if issubclass(obj, BaseJob):
                            job_registery.add(
                                obj.__name__, app.name
                            )  # register subclasses of `BaseJob`
            except ModuleNotFoundError:
                pass

        for (
            app
        ) in apps.get_app_configs():  # get all registered and collected Djang apps
            try:  # try to get the `pipelines` module and ignore if it doesn't exist
                pipelines_module = import_module(f"{app.name}.pipelines")
                for obj in dir(
                    pipelines_module
                ):  # get all attributes (as string) of `jobs` module
                    obj = getattr(
                        pipelines_module, obj
                    )  # turn string to the object itself
                    if (
                        obj is BasePipeline
                    ):  # we don't want to register the `BasePipeline` class
                        continue
                    if (
                        type(obj) is type
                    ):  # assert `obj` is a class and not a `dict` or some other builtin
                        if issubclass(obj, BasePipeline):
                            pipeline_registery.add(
                                obj.__name__, app.name
                            )  # register subclasses of `BaseJob`
            except ModuleNotFoundError:
                pass
