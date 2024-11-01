from dataclasses import dataclass
from uuid import UUID

from .._base_model import BaseModel


@dataclass
class ClickhouseDbModel(BaseModel):
    name: str
    engine: str
    data_path: str
    metadata_path: str
    uuid: UUID
    engine_full: str
    comment: str

