import os
from dataclasses import dataclass, field

from .._base_model import BaseModel


@dataclass
class ClickhouseRemoteSettingsModel(BaseModel):
    """
    https://clickhouse.com/docs/en/sql-reference/table-functions/remote
    """
    address: str = field(default_factory=lambda: os.environ.get('RIPLEY_REMOTE_CLICKHOUSE_ADDRESS'))
    db: str = field(default_factory=lambda: os.environ.get('RIPLEY_REMOTE_CLICKHOUSE_DB'))
    table: str = field(default_factory=lambda: os.environ.get('RIPLEY_REMOTE_CLICKHOUSE_TABLE'))
    user: str = field(default_factory=lambda: os.environ.get('RIPLEY_REMOTE_CLICKHOUSE_USER'))
    password: str = field(default_factory=lambda: os.environ.get('RIPLEY_REMOTE_CLICKHOUSE_PASSWORD'))
    sharding_key: str = ''
    secure: bool = False
