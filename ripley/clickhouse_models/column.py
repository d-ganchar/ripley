from dataclasses import dataclass

from .._base_model import BaseModel


@dataclass
class ClickhouseColumnModel(BaseModel):
    database: str
    table: str
    type: str
    default_kind: str
    default_expression: str
    comment: str
    compression_codec: str

    position: int
    data_compressed_bytes: int
    data_uncompressed_bytes: int
    marks_bytes: int
    marks_bytes: int
    is_in_partition_key: int
    is_in_sorting_key: int
    is_in_primary_key: int
    is_in_sampling_key: int

