from dataclasses import dataclass

from .._base_model import BaseModel


@dataclass
class ClickhousePartitionModel(BaseModel):
    database: str
    table: str
    partition: str
    partition_id: str

    active: int
    visible: int
    rows: int
    bytes_on_disk: int
    data_compressed_bytes: int
    data_uncompressed_bytes: int
