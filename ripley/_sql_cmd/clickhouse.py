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
        on_cluster = f" ON CLUSTER '{self._on_cluster}'" if self._on_cluster else ''
        engine = f" ENGINE {self._engine}" if self._engine else ''
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


class InsertIntoS3Cmd(AbstractSql):
    def __init__(self, table_name: str, s3_settings: ClickhouseS3SettingsModel):
        self._s3_settings = s3_settings
        self._table_name = table_name

    def __repr__(self):
        return self._get_s3_cmd()

    def _get_s3_cmd(self, key_id: str = '*', secret: str = '*') -> str:
        url = self._s3_settings.url
        file_format = self._s3_settings.file_format
        compression = self._s3_settings.compression_method
        s3_cmd = f"""s3('{url}', '{key_id}', '{secret}', '{file_format}', '{compression}')"""

        return f'INSERT INTO FUNCTION {s3_cmd} SELECT * FROM {self._table_name}'

    def to_sql(self) -> str:
        return self._get_s3_cmd(self._s3_settings.access_key_id, self._s3_settings.secret_access_key)


class InsertFromS3Cmd(AbstractSql):
    def __init__(
        self,
        table_name: str,
        s3_settings: ClickhouseS3SettingsModel,
        fields: List[str],
        field_types: List[str],
    ):
        self._s3_settings = s3_settings
        self._table_name = table_name
        self._fields = fields
        self._field_types = field_types

    def __repr__(self):
        return self._get_s3_cmd()

    def _get_s3_cmd(self, key_id: str = '*', secret: str = '*') -> str:
        url = self._s3_settings.url
        file_format = self._s3_settings.file_format
        compression = self._s3_settings.compression_method

        fields = ', '.join(self._fields)
        field_types = ' || '.join(self._field_types)
        field_types = field_types[:-2] + "'"
        s3_cmd = f"s3('{url}', '{key_id}', '{secret}', '{file_format}', {field_types}, '{compression}')"

        return f'INSERT INTO {self._table_name} SELECT {fields} FROM {s3_cmd}'

    def to_sql(self) -> str:
        return self._get_s3_cmd(self._s3_settings.access_key_id, self._s3_settings.secret_access_key)


class CreateTableOnClusterCmd(BaseCreateTable):
    def __init__(self, table_name: str, on_cluster: str = ''):
        super().__init__(table_name)
        self._on_cluster = on_cluster

    def to_sql(self) -> str:
        cmd = super().to_sql()
        cmd = f"{cmd} ON CLUSTER {self._on_cluster}" if self._on_cluster else cmd
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
        return f"""{super().to_sql()}
            ENGINE = {self._engine}
            {self._order_by}
            {self._partition_by}
            AS {self._from_table}"""


class Remote(AbstractSql):
    def __init__(
        self,
        remote_address: str,
        remote_db: str,
        remote_table: str,
        remote_user: str,
        remote_password: str,
        secure: bool = False,
        sharding_key: str = '',
    ):
        self._remote_address = remote_address
        self._remote_db = remote_db
        self._remote_table = remote_table
        self._remote_user = remote_user
        self._remote_password = remote_password
        self._secure = secure
        self._sharding_key = sharding_key

    def _get_cmd(self, user: str = '*', password: str = '*'):
        params = [
            f"'{self._remote_address}'",
            f"{self._remote_db}",
            f"{self._remote_table}",
            f"'{user}'",
            f"'{password}'",
        ]

        if self._sharding_key:
            params.append(f'{self._sharding_key}')

        params = ', '.join(params)
        if self._secure:
            return f"remoteSecure({params})"
        return f"remote({params})"

    def __repr__(self):
        return self._get_cmd()

    def to_sql(self) -> str:
        return self._get_cmd(self._remote_user, self._remote_password)


class InsertFromRemote(AbstractSql):
    def __init__(self, table_name: str, from_remote: Remote) -> None:
        self._from_remote = from_remote
        self._table_name = table_name

    def __repr__(self):
        return f'INSERT INTO {self._table_name} SELECT * FROM {self._from_remote.__repr__()}'

    def to_sql(self) -> str:
        return f'INSERT INTO {self._table_name} SELECT * FROM {self._from_remote.to_sql()}'


class CreateDistributedTable(AbstractSql):
    def __init__(
        self,
        create_table: str,
        table: str,
        on_cluster: str,
        database: str,
        sharding_key: str = '',
        cluster: str = "'{cluster}'",
    ):
        self._create_table = create_table
        self._table = table
        self._on_cluster = on_cluster
        self._cluster = cluster
        self._database = database
        self._sharding_key = sharding_key

    def to_sql(self) -> str:
        sharding_key = f', {self._sharding_key}' if self._sharding_key else ''
        return f"""CREATE TABLE {self._create_table} ON CLUSTER {self._on_cluster}
            ENGINE = Distributed({self._cluster}, {self._database}, {self._table}{sharding_key})"""
