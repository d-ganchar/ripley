from typing import Any, List

from .cmd_service import CmdService
from .db_service import DbService
from .partition_service import PartitionService
from .system_service import SystemService
from .table_service import TableService
from .._protocols.clickhouse import ClickhouseProtocol
from ..clickhouse_models.column import ClickhouseColumnModel as Column
from ..clickhouse_models.db import ClickhouseDbModel as Db
from ..clickhouse_models.disk import ClickhouseDiskModel as Disk
from ..clickhouse_models.partition import ClickhousePartitionModel as Partition
from ..clickhouse_models.process import ClickhouseProcessModel as Process
from ..clickhouse_models.s3_settings import ClickhouseS3SettingsModel as S3Settings
from ..clickhouse_models.table import ClickhouseTableModel as Table


class MainService(ClickhouseProtocol):
    def __init__(self, client: Any) -> None:
        self._client = client
        self._cmd = CmdService(client)
        self._system = SystemService(self._cmd)
        self._partition = PartitionService(client, self._system, self._cmd)
        self._table = TableService(client, self._system, self._cmd)
        self._db = DbService(client, self._system, self._cmd)

    def ping(self) -> bool:
        return self._client.get_connection().ping()

    @property
    def active_db(self) -> str:
        return self._cmd.active_db

    @property
    def on_cluster(self) -> str:
        return self._cmd.on_cluster

    @property
    def settings(self) -> dict:
        return self._cmd.settings

    def set_settings(self, settings: dict):
        self._cmd.set_settings(settings)

    def skip_settings(self):
        self._cmd.skip_settings()

    def set_on_cluster(self, name: str):
        self._cmd.set_on_cluster(name)

    def skip_on_cluster(self):
        self._cmd.skip_on_cluster()

    def create_db(self, name: str, engine: str = '') -> Db:
        return self._db.create_db(name, engine)

    def exec(self, sql: str, params: dict = None) -> List:
        return self._cmd.exec(sql, params)

    def move_partition(self, from_table: Table, to_table: Table, partition: str) -> None:
        self._partition.move_partition(from_table, to_table, partition)

    def replace_partition(self, from_table: Table, to_table: Table, partition: str) -> None:
        self._partition.replace_partition(from_table, to_table, partition)

    def drop_partition(self, table: Table, partition: str) -> None:
        self._partition.drop_partition(table, partition)

    def detach_partition(self, table: Table, partition: str) -> None:
        self._partition.detach_partition(table, partition)

    def attach_partition(self, table: Table, partition: str) -> None:
        self._partition.attach_partition(table, partition)

    def get_databases(self) -> List[Db]:
        return self._system.get_databases()

    def get_database_by_name(self, name: str = '') -> Db:
        return self._system.get_database_by_name(name)

    def get_tables_by_db(self, db: str = '') -> List[Table]:
        return self._system.get_tables_by_db(db)

    def get_table_by_name(self, table: str, db: str = '') -> Table:
        return self._system.get_table_by_name(table, db)

    def get_table_partitions(self, table: str, db: str = '') -> List[Partition]:
        return self._system.get_table_partitions(table, db)

    def get_processes(self) -> List[Process]:
        return self._system.get_processes()

    def get_process_by_query_id(self, query_id: str) -> Process:
        return self._system.get_process_by_query_id(query_id)

    def get_disks(self) -> List[Disk]:
        return self._system.get_disks()

    def get_table_columns(self, table: str, db: str = '') -> List[Column]:
        return self._system.get_table_columns(table, db)

    def create_table_as(self, from_table: Table, table: str, db: str = '', order_by: list = None,
                        partition_by: list = None, engine: str = '') -> Table:
        return self._table.create_table_as(from_table, table, db, order_by, partition_by, engine)

    def insert_from_table(self, from_table: Table, to_table: Table) -> None:
        self._table.insert_from_table(from_table, to_table)

    def truncate(self, table: str, db: str = '') -> None:
        self._table.truncate(table, db)

    def insert_from_s3(self, table: Table, s3_settings: S3Settings):
        self._table.insert_from_s3(table, s3_settings)

    def insert_table_to_s3(self, table: Table, s3_settings: S3Settings):
        self._table.insert_table_to_s3(table, s3_settings)

    def rename_table(self, table: Table, new_name: str, db: str = '') -> None:
        self._table.rename_table(table, new_name, db)
