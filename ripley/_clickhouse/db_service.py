from typing import Any

from .cmd_service import CmdService
from .system_service import SystemService
from .._sql_cmd.clickhouse import CreateDbOnCluster
from ..clickhouse_models.db import ClickhouseDbModel


class DbService:
    def __init__(self, client: Any, system: SystemService, cmd: CmdService) -> None:
        self._client = client
        self._cmd = cmd
        self._system = system

    def create_db(self, name: str, engine: str = '') -> ClickhouseDbModel:
        self._cmd.run_cmd(
            CreateDbOnCluster,
            model_params=dict(name=name, engine=engine),
        )

        return self._system.get_database_by_name(name)
