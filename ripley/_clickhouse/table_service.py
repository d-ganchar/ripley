from typing import Any

from .cmd_service import CmdService
from .system_service import SystemService
from .._sql_cmd.clickhouse import (
    RenameTableOnCluster,
    TruncateOnClusterCmd,
    CreateTableAsOnClusterCmd,
    InsertFromS3Cmd,
    InsertIntoS3Cmd,
    ClickhouseS3SettingsModel,
)
from .._sql_cmd.general import BaseInsertIntoTableFromTable
from ..clickhouse_models.table import ClickhouseTableModel


class TableService:
    def __init__(self, client: Any, system: SystemService, cmd: CmdService) -> None:
        self._client = client
        self._cmd = cmd
        self._system = system

    def create_table_as(
        self,
        from_table: ClickhouseTableModel,
        table: str,
        db: str = '',
        order_by: list = None,
        partition_by: list = None,
        engine: str = ''
    ) -> ClickhouseTableModel:
        table_full_name = self._cmd.get_full_table_name(table, db)
        order = f'ORDER BY {", ".join(order_by) if order_by else from_table.sorting_key}'
        partition = ", ".join(partition_by) if partition_by else from_table.partition_key
        if partition:
            partition = f'PARTITION BY {partition}'

        self._cmd.run_cmd(
            CreateTableAsOnClusterCmd,
            model_params=dict(
                table_name=table_full_name,
                from_table=from_table.full_name,
                order_by=order,
                partition_by=partition,
                engine=engine if engine else from_table.engine,
            ),
        )

        return self._system.get_table_by_name(table, db)

    def insert_from_table(self, from_table: ClickhouseTableModel, to_table: ClickhouseTableModel) -> None:
        self._cmd.run_cmd(
            BaseInsertIntoTableFromTable,
            model_params=dict(from_table=from_table.full_name, to_table=to_table.full_name),
        )

    def truncate(self, table: str, db: str = '') -> None:
        table_name = self._cmd.get_full_table_name(table, db)
        self._cmd.run_cmd(
            TruncateOnClusterCmd,
            model_params=dict(table_name=table_name),
        )

    def insert_from_s3(self, table: ClickhouseTableModel, s3_settings: ClickhouseS3SettingsModel):
        self._cmd.run_cmd(
            InsertFromS3Cmd,
            model_params=dict(
                table_name=table.full_name,
                s3_settings=s3_settings,
            ),
        )

    def insert_table_to_s3(self, table: ClickhouseTableModel, s3_settings: ClickhouseS3SettingsModel):
        self._cmd.run_cmd(
            InsertIntoS3Cmd,
            model_params=dict(table_name=table.full_name, s3_settings=s3_settings),
        )

    def rename_table(self, table: ClickhouseTableModel, new_name: str, db: str = '') -> ClickhouseTableModel:
        full_name = self._cmd.get_full_table_name(new_name, db)
        self._cmd.run_cmd(
            RenameTableOnCluster,
            model_params=dict(table=table.full_name, new_name=full_name),
        )

        return self._system.get_table_by_name(new_name, db)
