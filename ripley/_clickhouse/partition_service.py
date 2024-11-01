from typing import Any

from .cmd_service import CmdService
from .system_service import SystemService
from .._sql_cmd.clickhouse import (
    DetachPartitionOnClusterCmd,
    AttachPartitionOnClusterCmd,
    DropPartitionOnClusterCmd,
    MovePartitionOnClusterCmd,
    ReplacePartitionOnClusterCmd,
)
from ..clickhouse_models.table import ClickhouseTableModel as CTable


class PartitionService:
    def __init__(self, client: Any, system: SystemService, cmd: CmdService) -> None:
        self._client = client
        self._system = system
        self._cmd = cmd

    def move_partition(self, from_table: CTable, to_table: CTable, partition: str) -> None:
        self._cmd.run_cmd(
            MovePartitionOnClusterCmd,
            model_params=dict(
                to_table_name=to_table.full_name,
                table_name=from_table.full_name,
                partition=partition,
            ),
        )

    def drop_partition(self, table: CTable, partition: str) -> None:
        self._cmd.run_cmd(
            DropPartitionOnClusterCmd,
            model_params=dict(
                table_name=table.full_name,
                partition=partition,
            ),
        )

    def replace_partition(self, from_table: CTable, to_table: CTable, partition: str) -> None:
        self._cmd.run_cmd(
            ReplacePartitionOnClusterCmd,
            model_params=dict(
                table_name=to_table.full_name,
                partition=partition,
                from_table_name=from_table.full_name,
            ),
        )

    def detach_partition(self, table: CTable, partition: str) -> None:
        self._cmd.run_cmd(
            DetachPartitionOnClusterCmd,
            model_params=dict(
                table_name=table.full_name,
                partition=partition,
            ),
        )

    def attach_partition(self, table: CTable, partition: str) -> None:
        self._cmd.run_cmd(
            AttachPartitionOnClusterCmd,
            model_params=dict(
                table_name=table.full_name,
                partition=partition,
            ),
        )
