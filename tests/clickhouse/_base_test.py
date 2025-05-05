import sys
import unittest
from enum import Enum

from clickhouse_driver import Client

from ripley import from_clickhouse


_PY_VERSION = '_'.join([str(sys.version_info[0]), str(sys.version_info[1])])

def get_full_db_name(name: str) -> str:
    return f'{name}_py_{_PY_VERSION}'


def get_full_table_name(table: str, db: str) -> str:
    return f'{db}.{table}' if db else table


class DB(Enum):
    RIPLEY_TESTS = get_full_db_name('ripley_tests_db1')
    RIPLEY_TESTS2 = get_full_db_name('ripley_tests_db2')


_client = Client(host='localhost', port=9000, user='default', password='', database='default')
_clickhouse = from_clickhouse(_client)
# create default db for tests
_clickhouse.create_db(DB.RIPLEY_TESTS.value)
_clickhouse.create_db(DB.RIPLEY_TESTS2.value)
_client = Client(host='localhost', port=9000, user='default', password='', database=DB.RIPLEY_TESTS.value)


class BaseClickhouseTest(unittest.TestCase):
    maxDiff = 50000
    clickhouse = from_clickhouse(_client)

    def setUp(self):
        for db in DB:
            self.clickhouse.exec(f'DROP DATABASE IF EXISTS {db.value}')
            self.clickhouse.exec(f'CREATE DATABASE {db.value}')

    def create_test_table(self, table_name: str, db_name: str = ''):
        """
        test table with 2 partitions. 1000 records
        """
        if db_name:
            schema_tbl = f'{db_name}.{table_name}'
        else:
            schema_tbl = table_name

        self.clickhouse.exec(f"""
            CREATE TABLE {schema_tbl}
            (
                  key UInt64,
                  value String,
                  day Date
            )
            ENGINE MergeTree()
            PARTITION BY day
            ORDER BY key AS (
                SELECT rowNumberInAllBlocks()                         AS key,
                       concat(toString(key), '+', toString(key))      AS value,
                       if(modulo(key, 3), '2024-01-01', '2025-01-01') AS day
                  FROM generateRandom('a Array(Int8), d Decimal32(4), c Tuple(DateTime64(3), UUID)', 1, 1, 1)
             LIMIT 1000
             )
        """)
