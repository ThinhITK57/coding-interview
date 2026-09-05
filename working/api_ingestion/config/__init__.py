from dataclasses import dataclass

from .api_config import APIConfig
from .job_config import JobConfig


@dataclass
class IngestionConfig:
    api: APIConfig
    job: JobConfig
    