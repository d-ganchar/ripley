from copy import deepcopy
from dataclasses import dataclass
from ipaddress import IPv6Address

from .._base_model import BaseModel


@dataclass
class ClickhouseProcessModel(BaseModel):
    current_database: str
    user: str
    query_id: str
    initial_query_id: str
    initial_user: str
    os_user: str
    client_hostname: str
    client_name: str
    http_user_agent: str
    http_referer: str
    forwarded_for: str
    quota_key: str
    query: str
    query_kind: str

    is_initial_query: int
    port: int
    initial_port: int
    interface: int
    client_revision: int
    client_version_major: int
    client_version_minor: int
    client_version_patch: int
    http_method: int
    distributed_depth: int
    is_cancelled: int
    is_all_data_sent: int
    read_bytes: int
    read_rows: int
    written_rows: int
    written_bytes: int
    total_rows_approx: int
    memory_usage: int
    peak_memory_usage: int

    elapsed: float
    thread_ids: list

    address: IPv6Address
    initial_address: IPv6Address

    def __init__(self, *args, **kwargs):
        kwargs_copy = deepcopy(kwargs)
        for key, alias in (
            ('ProfileEvents', 'profile_events'),
            ('Settings', 'settings'),
        ):
            kwargs[alias] = kwargs[key]
            del kwargs[alias]

        self.args = args
        self.kwargs = kwargs_copy
