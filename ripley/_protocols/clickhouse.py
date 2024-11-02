import abc
from typing import Protocol, List

from ..clickhouse_models.column import ClickhouseColumnModel as Column
from ..clickhouse_models.db import ClickhouseDbModel as Db
from ..clickhouse_models.disk import ClickhouseDiskModel as Disk
from ..clickhouse_models.partition import ClickhousePartitionModel as Partition
from ..clickhouse_models.process import ClickhouseProcessModel as Process
from ..clickhouse_models.s3_settings import ClickhouseS3SettingsModel as S3Settings
from ..clickhouse_models.table import ClickhouseTableModel as Table


class ClickhouseProtocol(Protocol, metaclass=abc.ABCMeta):
    @property
    def active_db(self) -> str:
        """
        name of active database
        """

    @property
    def on_cluster(self) -> str:
        """
        name of ON CLUSTER mode
        """

    @property
    def settings(self) -> dict:
        """
        see: https://clickhouse.com/docs/en/operations/settings/settings
        """

    @abc.abstractmethod
    def set_settings(self, settings: dict):
        """
        see: https://clickhouse.com/docs/en/operations/settings/settings
        """

    @abc.abstractmethod
    def skip_settings(self):
        """
        see: https://clickhouse.com/docs/en/operations/settings/settings
        """

    @abc.abstractmethod
    def set_on_cluster(self, name: str):
        """
        enable ON CLUSTER mode
        """

    @abc.abstractmethod
    def skip_on_cluster(self):
        """
        disable ON CLUSTER mode
        """

    @abc.abstractmethod
    def create_db(self, name: str, engine: str = '') -> Db:
        """
        see: https://clickhouse.com/docs/en/sql-reference/statements/create/database
        """

    @abc.abstractmethod
    def ping(self) -> bool:
        pass

    @abc.abstractmethod
    def exec(self, sql: str, params: dict = None) -> List:
        pass

    @abc.abstractmethod
    def move_partition(self, from_table: Table, to_table: Table, partition: str) -> None:
        """
        see: https://clickhouse.com/docs/en/sql-reference/statements/alter/partition#move-partition-to-table
        """

    @abc.abstractmethod
    def replace_partition(self, from_table: Table, to_table: Table, partition: str) -> None:
        """
        see: https://clickhouse.com/docs/en/sql-reference/statements/alter/partition#replace-partition
        """

    @abc.abstractmethod
    def drop_partition(self, table: Table, partition: str) -> None:
        """
        see: https://clickhouse.com/docs/en/sql-reference/statements/alter/partition#drop-partitionpart
        """

    @abc.abstractmethod
    def detach_partition(self, table: Table, partition: str) -> None:
        """
        see: https://clickhouse.com/docs/en/sql-reference/statements/alter/partition#detach-partitionpart
        """

    @abc.abstractmethod
    def attach_partition(self, table: Table, partition: str) -> None:
        """
        see: https://clickhouse.com/docs/en/sql-reference/statements/alter/partition#attach-partitionpart
        """

    @abc.abstractmethod
    def get_databases(self) -> List[Db]:
        """
        system.databases
        see: https://clickhouse.com/docs/en/operations/system-tables/databases
        """

    @abc.abstractmethod
    def get_database_by_name(self, name: str = '') -> Db:
        """
        system.databases
        see: https://clickhouse.com/docs/en/operations/system-tables/databases
        """

    @abc.abstractmethod
    def get_tables_by_db(self, db: str = '') -> List[Table]:
        """
        system.tables
        see: https://clickhouse.com/docs/en/operations/system-tables/tables
        """

    @abc.abstractmethod
    def get_table_by_name(self, table: str, db: str = '') -> Table:
        """
        system.tables
        see: https://clickhouse.com/docs/en/operations/system-tables/tables
        """

    @abc.abstractmethod
    def get_table_partitions(self, table: str, db: str = '') -> List[Partition]:
        """
        system.parts
        see: https://clickhouse.com/docs/en/operations/system-tables/parts
        """

    @abc.abstractmethod
    def get_processes(self) -> List[Process]:
        """
        system.processes
        see: https://clickhouse.com/docs/en/operations/system-tables/processes
        """

    @abc.abstractmethod
    def get_process_by_query_id(self, query_id: str) -> Process:
        """
        system.processes
        see: https://clickhouse.com/docs/en/operations/system-tables/processes
        """

    @abc.abstractmethod
    def get_disks(self) -> List[Disk]:
        """
        system.disks
        see: https://clickhouse.com/docs/en/operations/system-tables/disks
        """

    @abc.abstractmethod
    def get_table_columns(self, table: str, db: str = '') -> List[Column]:
        """
        system.columns
        https://clickhouse.com/docs/en/operations/system-tables/columns
        """

    @abc.abstractmethod
    def create_table_as(
        self,
        from_table: Table,
        table: str,
        db: str = '',
        order_by: list = None,
        partition_by: list = None,
        engine: str = '',
    ) -> Table:
        """
        Creates a table with the same structure or with custom ORDER BY / PARTITION BY / Engine
        Query result:
           CREATE TABLE {db}.{table}
           ENGINE = {engine}
            ORDER BY {order_by}
        PARTITION BY {partition_by}
               AS {from_table}
        """

    @abc.abstractmethod
    def insert_from_table(self, from_table: Table, to_table: Table) -> None:
        """
        INSERT INTO db1.table1 SELECT * FROM db2.table2
        """

    @abc.abstractmethod
    def truncate(self, table: str, db: str = '') -> None:
        pass

    @abc.abstractmethod
    def insert_from_s3(self, table: Table, s3_settings: S3Settings):
        """
        INSERT INTO db1.table1 SELECT * FROM s3(...)

        see: https://clickhouse.com/docs/en/integrations/s3#inserting-data-from-s3
        """

    @abc.abstractmethod
    def insert_table_to_s3(self, table: Table, s3_settings: S3Settings):
        """
        INSERT INTO FUNCTION s3(...) SELECT * FROM {db}.{table}

        see: https://clickhouse.com/docs/en/integrations/s3#exporting-data
        """

    @abc.abstractmethod
    def rename_table(self, table: Table, new_name: str, db: str = '') -> None:
        """
        https://clickhouse.com/docs/en/sql-reference/statements/rename
        """
