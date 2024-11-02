from typing import List

from .cmd_service import CmdService
from ..clickhouse_models.column import ClickhouseColumnModel
from ..clickhouse_models.db import ClickhouseDbModel
from ..clickhouse_models.disk import ClickhouseDiskModel
from ..clickhouse_models.partition import ClickhousePartitionModel
from ..clickhouse_models.process import ClickhouseProcessModel
from ..clickhouse_models.table import ClickhouseTableModel


class SystemService:
    def __init__(self, cmd: CmdService):
        self._cmd = cmd

    def get_databases(self) -> List[ClickhouseDbModel]:
        return self._cmd.get_records("""
            SELECT *
              FROM system.databases
             WHERE lower(name) != 'information_schema' AND name != 'system'
             ORDER BY name
        """, model=ClickhouseDbModel)

    def get_database_by_name(self, name: str = '') -> ClickhouseDbModel:
        return self._cmd.get_first_record("""
            SELECT *
              FROM system.databases
             WHERE database = %(database)s
               AND lower(name) != 'information_schema' AND name != 'system'
             LIMIT 1
        """, params={'database': self._cmd.get_db_or_default(name)}, model=ClickhouseDbModel)

    def get_table_by_name(self, table: str, db: str = '') -> 'ClickhouseTableModel':
        return self._cmd.get_first_record("""
            SELECT *
              FROM system.tables
             WHERE database = %(database)s AND name = %(table)s
               AND lower(name) != 'information_schema' AND name != 'system'
             LIMIT 1
        """, params={'database': self._cmd.get_db_or_default(db), 'table': table}, model=ClickhouseTableModel)

    def get_tables_by_db(self, db: str = '') -> List[ClickhouseTableModel]:
        return self._cmd.get_records("""
            SELECT *
              FROM system.tables
             WHERE database = %(database)s
               AND lower(name) != 'information_schema' AND name != 'system'
        """, params={'database': self._cmd.get_db_or_default(db)}, model=ClickhouseTableModel)

    def get_table_partitions(self, table: str, db: str = '') -> List[ClickhousePartitionModel]:
        return self._cmd.get_records("""
            SELECT partition,
                   partition_id,
                   active,
                   database,
                   table,
                   visible,
                   sum(rows) AS rows,
                   sum(bytes_on_disk) AS bytes_on_disk,
                   sum(data_compressed_bytes) AS data_compressed_bytes,
                   sum(data_uncompressed_bytes) AS data_uncompressed_bytes
              FROM system.parts
             WHERE database = %(database)s AND table = %(table)s
               AND lower(name) != 'information_schema' AND name != 'system'
             GROUP BY database, table, partition_id, partition, active, visible
             ORDER BY partition
        """, params={'database': self._cmd.get_db_or_default(db), 'table': table}, model=ClickhousePartitionModel)

    def get_processes(self) -> List[ClickhouseProcessModel]:
        return self._cmd.get_records('SELECT * FROM system.processes', model=ClickhouseProcessModel)

    def get_process_by_query_id(self, query_id: str) -> ClickhouseProcessModel:
        return self._cmd.get_first_record("""
            SELECT *
              FROM system.processes
             WHERE query_id = %(query_id)s
             LIMIT 1
        """, params={'query_id': query_id}, model=ClickhouseProcessModel)

    def get_disks(self) -> List[ClickhouseDiskModel]:
        return self._cmd.get_records('SELECT * FROM system.disks', model=ClickhouseDiskModel)

    def get_table_columns(self, table: str, db: str = '') -> List[ClickhouseColumnModel]:
        return self._cmd.get_records("""
            SELECT *
              FROM system.columns
             WHERE table = %(table)s AND database = %(database)s
               AND lower(name) != 'information_schema' AND name != 'system'
             ORDER BY position
        """, params={'table': table, 'database': self._cmd.get_db_or_default(db)}, model=ClickhouseColumnModel)
