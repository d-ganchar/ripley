from dataclasses import dataclass
from datetime import datetime
from typing import List
from uuid import UUID

from .._base_model import BaseModel


@dataclass
class ClickhouseTableModel(BaseModel):
    partition_key: str
    database: str
    as_select: str
    comment: str
    create_table_query: str
    name: str
    engine: str
    engine_full: str
    metadata_path: str
    primary_key: str
    sampling_key: str
    storage_policy: str
    sorting_key: str

    parts: int
    is_temporary: int
    total_bytes: int
    total_rows: int
    has_own_data: int
    total_marks: int
    lifetime_rows: int
    lifetime_bytes: int
    active_parts: int

    data_paths: List[str]
    dependencies_database: List[str]
    dependencies_table: List[str]
    loading_dependencies_database: List[str]
    loading_dependencies_table: List[str]
    loading_dependent_database: List[str]
    loading_dependent_table: List[str]

    uuid: UUID
    metadata_modification_time: datetime

    @property
    def full_name(self) -> str:
        return f'{self.database}.{self.name}'
