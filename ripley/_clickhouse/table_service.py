from typing import Any

from .cmd_service import CmdService
from .system_service import SystemService
from .._log import log
from .._sql_cmd.clickhouse import (
    RenameTableOnCluster,
    TruncateOnClusterCmd,
    CreateTableAsOnClusterCmd,
    InsertFromS3Cmd,
    InsertIntoS3Cmd,
    Remote,
    InsertFromRemote,
    CreateDistributedTable,
)
from .._sql_cmd.general import BaseInsertIntoTableFromTable
from ..clickhouse_models.remote_settings import ClickhouseRemoteSettingsModel as RemoteSettings
from ..clickhouse_models.s3_settings import ClickhouseS3SettingsModel as S3Settings
from ..clickhouse_models.s3_settings import S3SelectSettingsModel as S3SelectSettings
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

    def insert_from_s3(
        self,
        table: ClickhouseTableModel,
        s3_settings: S3Settings,
        s3_select_settings: S3SelectSettings = None,
    ):
        fields = []
        field_types = []
        convertors = s3_select_settings.field_convertors if s3_select_settings else []

        for column in self._system.get_table_columns(table.name, table.database):
            s3_name = column.name
            real_name = column.name
            s3_type = column.type

            if s3_select_settings:
                if column.name == s3_select_settings.s3_file_name_column:
                    fields.append(f"'{s3_settings.url}' AS {column.name}")
                    continue

                if s3_select_settings.field_name_transformer:
                    s3_name = s3_select_settings.field_name_transformer(real_name)
                    real_name = f'"{s3_name}"'

                for convertor_field, convertor_type, convertor in convertors:
                    if column.name not in convertor_field:
                        continue

                    real_name = convertor(real_name)
                    s3_type = convertor_type
                    break
            else:
                real_name = f'"{column.name}"'

            s3_name = f'"{s3_name}"'
            fields.append(f'{real_name} AS {column.name}')
            field_types.append(f"'{s3_name} {s3_type},'")

        self._cmd.run_cmd(
            InsertFromS3Cmd,
            model_params=dict(
                table_name=table.full_name,
                s3_settings=s3_settings,
                field_types=field_types,
                fields=fields,
            ),
        )

    def insert_table_to_s3(self, table: ClickhouseTableModel, s3_settings: S3Settings):
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

    def insert_from_remote(self, settings: RemoteSettings, table: str, db: str = '',
                           create_table: bool = False) -> None:
        remote = Remote(
            remote_address=settings.address,
            remote_db=settings.db,
            remote_user=settings.user,
            remote_password=settings.password,
            remote_table=settings.table,
            secure=settings.secure,
            sharding_key=settings.sharding_key,
        )

        if create_table:
            log.info(
                'create table "%s" from remote table "%s", host: %s',
                self._cmd.get_full_table_name(table, db),
                self._cmd.get_full_table_name(settings.table, settings.db),
                settings.address,
            )
            remote_system = Remote(
                remote_address=settings.address,
                remote_db='system',
                remote_user=settings.user,
                remote_password=settings.password,
                remote_table='tables',
            )

            result = self._cmd.exec(f"""
                SELECT create_table_query
                  FROM {remote_system.to_sql()}
                 WHERE name = %(table)s AND database = %(database)s
                 LIMIT 1
            """, params={'table': settings.table, 'database': settings.db})

            sql: str = result[0][0]
            parts = sql.split(' ')
            parts[2] = self._cmd.get_full_table_name(table, db)
            sql = ' '.join(parts)
            log.info(sql)
            self._cmd.exec(sql)

        self._cmd.run_cmd(
            InsertFromRemote,
            model_params=dict(table_name=self._cmd.get_full_table_name(table, db), from_remote=remote),
        )

    def create_distributed_table(self, create_table: str, table: str, database: str, sharding_key: str = '',
                                 cluster: str = "'{cluster}'",) -> ClickhouseTableModel:
        self._cmd.run_cmd(
            CreateDistributedTable,
            model_params=dict(
                create_table=create_table,
                table=table,
                database=database,
                sharding_key=sharding_key,
                cluster=cluster,
            ),
        )

        db_name, table_name = create_table.split('.')
        return self._system.get_table_by_name(table_name, db_name)
