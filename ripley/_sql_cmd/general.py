class AbstractSql:
    def __repr__(self):
        return self.to_sql().replace('    ', '').replace('\n\n', '\n')

    def to_sql(self) -> str:
        raise NotImplementedError()


class BaseTable(AbstractSql):
    def __init__(self, table_name: str):
        self._table_name = table_name

    def to_sql(self) -> str:
        return f'TABLE {self._table_name}'


class BaseTruncate(BaseTable):
    def to_sql(self) -> str:
        table = super().to_sql()
        return f'TRUNCATE {table}'


class BaseAlter(BaseTable):
    def to_sql(self) -> str:
        table = super().to_sql()
        return f'ALTER {table}'


class BaseCreateTable(BaseTable):
    def to_sql(self) -> str:
        table = super().to_sql()
        return f'CREATE {table}'


class BaseCreateDb(AbstractSql):
    def __init__(self, name: str):
        self._name = name

    def to_sql(self) -> str:
        return f'CREATE DATABASE IF NOT EXISTS {self._name}'


class BaseRenameTable(AbstractSql):
    def __init__(self, table: str, new_name: str):
        self._table = table
        self._new_name = new_name

    def to_sql(self) -> str:
        return f'RENAME TABLE {self._table} TO {self._new_name}'


class BaseInsertIntoTableFromTable(AbstractSql):
    def __init__(self, from_table: str, to_table: str):
        self._from_table = from_table
        self._to_table = to_table

    def to_sql(self) -> str:
        return f'INSERT INTO {self._to_table} SELECT * FROM {self._from_table}'
