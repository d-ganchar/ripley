from dataclasses import dataclass

from .._base_model import BaseModel


@dataclass
class ClickhouseDiskModel(BaseModel):
    name: str
    path: str
    cache_path: str
    type: str

    free_space: int
    total_space: int
    unreserved_space: int
    keep_free_space: int
    is_encrypted: int
    is_read_only: int
    is_write_once: int
    is_remote: int
    is_broken: int
