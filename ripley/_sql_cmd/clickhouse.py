from typing import List

from .._sql_cmd.general import (
    BaseAlter,
    BaseTruncate,
    BaseCreateDb,
    BaseRenameTable,
    AbstractSql,
    BaseCreateTable,
)
from ..clickhouse_models.s3_settings import ClickhouseS3SettingsModel


class CreateDbOnCluster(BaseCreateDb):
    def __init__(self, name: str, on_cluster: str = '', engine: str = ''):
        super().__init__(name)
        self._on_cluster = on_cluster
        self._engine = engine

    def to_sql(self) -> str:
        cmd = super().to_sql()
        on_cluster = f"ON CLUSTER '{self._on_cluster}'" if self._on_cluster else ''
        engine = f"ENGINE {self._engine}" if self._engine else ''
        return f"{cmd}{on_cluster}{engine}"


class RenameTableOnCluster(BaseRenameTable):
    def __init__(self, table: str, new_name: str, on_cluster: str = ''):
        super().__init__(table, new_name)
        self._on_cluster = on_cluster

    def to_sql(self) -> str:
        cmd = super().to_sql()
        return f"{cmd} ON CLUSTER '{self._on_cluster}'" if self._on_cluster else cmd


class AlterOnClusterCmd(BaseAlter):
    def __init__(self, table_name: str, on_cluster: str = ''):
        super().__init__(table_name)
        self._on_cluster = on_cluster

    def to_sql(self) -> str:
        cmd = super().to_sql()
        return f"{cmd} ON CLUSTER '{self._on_cluster}'" if self._on_cluster else cmd


class DetachPartitionOnClusterCmd(AlterOnClusterCmd):
    def __init__(self, table_name, partition: str, on_cluster: str = ''):
        super().__init__(table_name, on_cluster)
        self._partition = partition

    def to_sql(self) -> str:
        cmd = super().to_sql()
        return f"{cmd} DETACH PARTITION '{self._partition}'"


class AttachPartitionOnClusterCmd(AlterOnClusterCmd):
    def __init__(self, table_name: str, partition: str, on_cluster: str = ''):
        super().__init__(table_name, on_cluster)
        self._partition = partition

    def to_sql(self) -> str:
        cmd = super().to_sql()
        return f"{cmd} ATTACH PARTITION '{self._partition}'"


class DropPartitionOnClusterCmd(AlterOnClusterCmd):
    def __init__(self, table_name: str, partition: str, on_cluster: str = ''):
        super().__init__(table_name, on_cluster)
        self._partition = partition

    def to_sql(self) -> str:
        cmd = super().to_sql()
        return f"{cmd} DROP PARTITION '{self._partition}'"


class MovePartitionOnClusterCmd(AlterOnClusterCmd):
    def __init__(self, table_name: str, partition: str, to_table_name, on_cluster: str = ''):
        super().__init__(table_name, on_cluster)
        self._to_table_name = to_table_name
        self._partition = partition

    def to_sql(self) -> str:
        cmd = super().to_sql()
        return f"{cmd} MOVE PARTITION '{self._partition}' TO TABLE {self._to_table_name}"


class ReplacePartitionOnClusterCmd(AlterOnClusterCmd):
    def __init__(self, table_name: str, partition: str, from_table_name, on_cluster: str = ''):
        super().__init__(table_name, on_cluster)
        self._from_table_name = from_table_name
        self._partition = partition

    def to_sql(self) -> str:
        cmd = super().to_sql()
        return f"{cmd} REPLACE PARTITION '{self._partition}' FROM {self._from_table_name}"


class TruncateOnClusterCmd(BaseTruncate):
    def __init__(self, table_name: str, on_cluster: str = ''):
        super().__init__(table_name)
        self._on_cluster = on_cluster

    def to_sql(self) -> str:
        cmd = super().to_sql()
        cmd = f"{cmd} ON CLUSTER '{self._on_cluster}'" if self._on_cluster else cmd
        return cmd


class S3Cmd(AbstractSql):
    def __init__(self, table_name: str, s3_settings: ClickhouseS3SettingsModel, structure: List[str] = None):
        self._s3_settings = s3_settings
        self._structure = structure
        self._table_name = table_name

    def __repr__(self):
        url = self._s3_settings.url
        file_format = self._s3_settings.file_format
        compression = self._s3_settings.compression_method
        structure = f'{",".join(self._structure)},' if self._structure else ''

        return f"s3('{url}', '*', '*', '{file_format}',{structure} '{compression}')"

    def to_sql(self) -> str:
        key_id = self._s3_settings.access_key_id
        secret = self._s3_settings.secret_access_key
        url = self._s3_settings.url
        file_format = self._s3_settings.file_format
        compression = self._s3_settings.compression_method
        structure = f'{",".join(self._structure)},' if self._structure else ''

        return f"s3('{url}', '{key_id}', '{secret}', '{file_format}',{structure} '{compression}')"


class InsertIntoS3Cmd(S3Cmd):
    def __repr__(self):
        cmd = super().__repr__()
        return f'INSERT INTO FUNCTION {cmd} SELECT * FROM {self._table_name}'

    def to_sql(self) -> str:
        cmd = super().to_sql()
        return f'INSERT INTO FUNCTION {cmd} SELECT * FROM {self._table_name}'


class InsertFromS3Cmd(S3Cmd):
    def __repr__(self):
        cmd = super().__repr__()
        return f'INSERT INTO {self._table_name} SELECT * FROM {cmd}'

    def to_sql(self) -> str:
        cmd = super().to_sql()
        return f'INSERT INTO {self._table_name} SELECT * FROM {cmd}'


class CreateTableOnClusterCmd(BaseCreateTable):
    def __init__(self, table_name: str, on_cluster: str = ''):
        super().__init__(table_name)
        self._on_cluster = on_cluster

    def to_sql(self) -> str:
        cmd = super().to_sql()
        cmd = f"{cmd} ON CLUSTER '{self._on_cluster}'" if self._on_cluster else cmd
        return cmd


class CreateTableAsOnClusterCmd(CreateTableOnClusterCmd):
    def __init__(
        self,
        table_name: str,
        from_table: str,
        on_cluster: str = '',
        order_by: str = '',
        partition_by: str = '',
        engine: str = '',
    ):
        super().__init__(table_name, on_cluster)
        self._from_table = from_table
        self._order_by = order_by
        self._partition_by = partition_by
        self._engine = engine

    def to_sql(self) -> str:
        return f"""
            {super().to_sql()}
            engine = {self._engine}
            {self._order_by}
            {self._partition_by}
            AS {self._from_table}
        """
