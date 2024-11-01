from typing import Any

from ._clickhouse.main_service import MainService as _MainClickhouse
from ._protocols.clickhouse import ClickhouseProtocol


def from_clickhouse(client: Any) -> ClickhouseProtocol:
    return _MainClickhouse(client)
