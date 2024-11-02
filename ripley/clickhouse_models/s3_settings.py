import os
from dataclasses import dataclass, field

from .._base_model import BaseModel


@dataclass
class ClickhouseS3SettingsModel(BaseModel):
    url: str

    compression_method: str = 'auto'
    file_format: str = 'CSV'

    access_key_id: str = field(default_factory=lambda: os.environ.get('AWS_ACCESS_KEY_ID'))
    secret_access_key: str = field(default_factory=lambda: os.environ.get('AWS_SECRET_ACCESS_KEY'))
