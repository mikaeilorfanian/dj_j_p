from dataclasses import dataclass

from .job import BaseJob


class StartPipeline(BaseJob):
    @dataclass
    class Inputs:
        pipeline_name: str
