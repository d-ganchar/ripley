from dataclasses import dataclass
from typing import Union

from .._base_model import BaseModel


@dataclass
class ClickhouseColumnModel(BaseModel):
    database: str
    table: str
    name: str
    type: str
    default_kind: str
    default_expression: str
    comment: str
    compression_codec: str

    position: int
    data_compressed_bytes: int
    data_uncompressed_bytes: int
    marks_bytes: int
    is_in_partition_key: int
    is_in_sorting_key: int
    is_in_primary_key: int

    numeric_precision_radix: Union[int, None]
    numeric_scale: Union[int, None]
    datetime_precision: Union[int, None]
    is_in_sampling_key: int
    character_octet_length: Union[int, None]
    numeric_precision: Union[int, None]

