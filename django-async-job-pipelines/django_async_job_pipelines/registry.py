from dataclasses import dataclass, field


@dataclass
class JobRegistery:
    job_class_to_name_map: dict[str, str] = field(default_factory=dict)

    def add(self, class_name: str, app_name: str):
        if class_name in self.job_class_to_name_map:
            help = "Job class names must be unique!"
            previous = f"{self.job_class_to_name_map[class_name]}.{class_name}"
            raise ValueError(
                f"`{class_name}` seems to be duplicated. It was already added by `{previous}`. {help}"
            )

        self.job_class_to_name_map[class_name] = app_name

    def get_import_path_for_class_name(self, class_name: str) -> str:
        return f"{self.job_class_to_name_map[class_name]}.jobs"


job_registery = JobRegistery()


@dataclass
class PipelineRegistery:
    pipeline_class_to_name_map: dict[str, str] = field(default_factory=dict)

    def add(self, class_name: str, app_name: str):
        if class_name in self.pipeline_class_to_name_map:
            help = "Job class names must be unique!"
            previous = f"{self.pipeline_class_to_name_map[class_name]}.{class_name}"
            raise ValueError(
                f"`{class_name}` seems to be duplicated. It was already added by `{previous}`. {help}"
            )

        self.pipeline_class_to_name_map[class_name] = app_name

    def get_import_path_for_class_name(self, class_name: str) -> str:
        return f"{self.pipeline_class_to_name_map[class_name]}.pipelines"


pipeline_registery = PipelineRegistery()
