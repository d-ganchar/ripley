import os
from datetime import datetime

import boto3
from parameterized import parameterized

from ripley.clickhouse_models.s3_settings import ClickhouseS3SettingsModel
from ripley.clickhouse_models.remote_settings import ClickhouseRemoteSettingsModel as RemoteSettings
from tests.clickhouse._base_test import BaseClickhouseTest, DB

_S3_BUCKET = 'ripley'
_REGION_NAME = 'us-east-1'


def _init_s3() -> boto3.client:
    return boto3.client(
        's3',
        endpoint_url='https://localhost:9001',
        region_name=_REGION_NAME,
        use_ssl=False,
        verify=False,
    )


class TestClickhouseTableService(BaseClickhouseTest):
    s3 = _init_s3()

    def setUp(self):
        super().setUp()
        self.s3 = _init_s3()

        for bucket in self.s3.list_buckets().get('Buckets', []):
            bucket_name = bucket['Name']
            files = self.s3.list_objects_v2(Bucket=bucket_name).get('Contents', [])
            files = [{'Key': f['Key']} for f in files]

            if files:
                self.s3.delete_objects(Bucket=bucket_name, Delete={'Objects': files})

            self.s3.delete_bucket(Bucket=bucket_name)
        self.s3.create_bucket(Bucket=_S3_BUCKET)

    @classmethod
    def setUpClass(cls):
        os.environ['AWS_ACCESS_KEY_ID'] = 'ripley_key'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'ripley_secret'

    @classmethod
    def tearDownClass(cls):
        del os.environ['AWS_ACCESS_KEY_ID']
        del os.environ['AWS_SECRET_ACCESS_KEY']

    @parameterized.expand([
        [
            '',
            '',
            None,
            None,
            '',
            'CREATE TABLE ripley_tests_db1.create_table_as1 (`key` UInt64, `day` Date) ENGINE = MergeTree '
            'PARTITION BY day ORDER BY key SETTINGS index_granularity = 8192',
            'CREATE TABLE ripley_tests_db1.create_table_as2 (`key` UInt64, `day` Date) ENGINE = MergeTree '
            'PARTITION BY day ORDER BY key SETTINGS index_granularity = 8192'
        ],
        [
            DB.RIPLEY_TESTS2.value,
            DB.RIPLEY_TESTS.value,
            None,
            None,
            '',
            'CREATE TABLE ripley_tests_db1.create_table_as1 (`key` UInt64, `day` Date) ENGINE = MergeTree '
            'PARTITION BY day ORDER BY key SETTINGS index_granularity = 8192',
            'CREATE TABLE ripley_tests_db2.create_table_as2 (`key` UInt64, `day` Date) ENGINE = MergeTree '
            'PARTITION BY day ORDER BY key SETTINGS index_granularity = 8192'
        ],
        [
            '',
            DB.RIPLEY_TESTS2.value,
            ['day'],
            ['key'],
            'AggregatingMergeTree',
            'CREATE TABLE ripley_tests_db2.create_table_as1 (`key` UInt64, `day` Date) ENGINE = MergeTree '
            'PARTITION BY day ORDER BY key SETTINGS index_granularity = 8192',
            'CREATE TABLE ripley_tests_db1.create_table_as2 (`key` UInt64, `day` Date) ENGINE = AggregatingMergeTree '
            'PARTITION BY key ORDER BY day SETTINGS index_granularity = 8192',
        ],
    ])
    def test_create_table_as(
        self,
        db: str,
        from_db: str,
        order_by: list or None,
        partition_by: list or None,
        engine: str,
        original_ddl: str,
        target_ddl: str,
    ):
        from_table_name = 'create_table_as1'
        self.clickhouse.exec(f"""CREATE TABLE {self.get_full_table_name(from_table_name, from_db)}
            (
              key UInt64,
              day Date
            )
            ENGINE MergeTree()
            PARTITION BY day
                ORDER BY key
        """)

        target_table = 'create_table_as2'
        from_table = self.clickhouse.get_table_by_name(from_table_name, from_db)
        new_table = self.clickhouse.create_table_as(
            table=target_table,
            from_table=from_table,
            db=db,
            order_by=order_by,
            engine=engine,
            partition_by=partition_by,
        )

        self.assertEqual(original_ddl, from_table.create_table_query)
        self.assertEqual(target_ddl, new_table.create_table_query)

    @parameterized.expand([
        ['', ''],
        [DB.RIPLEY_TESTS.value, DB.RIPLEY_TESTS2.value],
        [DB.RIPLEY_TESTS2.value, DB.RIPLEY_TESTS.value],
    ])
    def test_insert_from_table(self, from_db: str, to_db: str):
        from_table_name = 'insert_from_table'
        to_table_name = 'insert_from_table2'

        self.create_test_table(from_table_name, from_db)

        from_table = self.clickhouse.get_table_by_name(from_table_name, from_db)
        to_table = self.clickhouse.create_table_as(
            table=to_table_name,
            from_table=from_table,
            db=to_db,
        )

        self.clickhouse.insert_from_table(from_table=from_table, to_table=to_table)

        result = self.clickhouse.exec(f"""
            SELECT count() AS rows FROM {from_table.full_name}
             UNION ALL
            SELECT count() AS rows FROM {to_table.full_name}
        """)

        self.assertEqual(result, [(1000, ), (1000, )])

    @parameterized.expand([
        [DB.RIPLEY_TESTS.value],
        [DB.RIPLEY_TESTS2.value],
    ])
    def test_truncate(self, db_name: str):
        table_name = 'truncate_table'
        self.create_test_table(table_name, db_name)
        self.clickhouse.truncate(table_name, db_name)

        result = self.clickhouse.exec(f'SELECT * FROM {self.get_full_table_name(table_name, db_name)}')
        self.assertListEqual([], result)

    @parameterized.expand([
        [DB.RIPLEY_TESTS.value],
        [DB.RIPLEY_TESTS2.value],
    ])
    def test_insert_from_s3(self, db_name: str):
        def _get_table_results(_table: str) -> tuple:
            return self.clickhouse.exec(f'SELECT count() as records FROM {_table}')

        to_s3_name = 'insert_to_s3'
        from_s3_name = 'insert_from_s3'
        settings = ClickhouseS3SettingsModel(url='http://localhost:9001/ripley/ripley_s3_test4')

        self.create_test_table(to_s3_name, db_name)

        from_table = self.clickhouse.get_table_by_name(to_s3_name, db_name)
        from_s3_table = self.clickhouse.create_table_as(table=from_s3_name, from_table=from_table, db=db_name)
        to_s3_table = self.clickhouse.get_table_by_name(to_s3_name, db_name)

        self.clickhouse.insert_table_to_s3(table=to_s3_table, s3_settings=settings)
        self.clickhouse.insert_from_s3(table=from_s3_table, s3_settings=settings)

        result1 = _get_table_results(to_s3_table.full_name)
        result2 = _get_table_results(from_table.full_name)

        self.assertEqual(result1, [(1000, )])
        self.assertEqual(result2, [(1000, )])

    def test_insert_from_remote(self):
        remote_table = 'insert_from_remote'
        remote_db = 'remote_db'
        full_remote_name = self.get_full_table_name(remote_table, remote_db)

        self.clickhouse.create_db(remote_db)
        self.clickhouse.exec(f"""CREATE OR REPLACE TABLE {full_remote_name} (
              value String,
              day Date
            )
            ENGINE MergeTree() ORDER BY value AS (SELECT 'value', '2024-01-01')
        """)

        settings = RemoteSettings('localhost:9000', remote_db, remote_table, 'default', '')
        table_copy_name = f'copy_{remote_table}'
        self.clickhouse.insert_from_remote(settings, table_copy_name, create_table=True)

        copy_name = self.get_full_table_name(table_copy_name, DB.RIPLEY_TESTS.value)
        original = self.clickhouse.exec(f'SELECT * FROM {full_remote_name}')
        copy = self.clickhouse.exec(f'SELECT * FROM {copy_name}')

        self.assertEqual(original, copy)
        self.clickhouse.truncate(table_copy_name, DB.RIPLEY_TESTS.value)
        self.clickhouse.exec(f"INSERT INTO {full_remote_name} VALUES ('new_value', '2025-01-01')")
        self.clickhouse.insert_from_remote(settings, table_copy_name)

        copy = self.clickhouse.exec(f'SELECT * FROM {copy_name}')
        self.assertEqual(
            copy,
            [
                ('new_value', datetime(2025, 1, 1).date()),
                ('value', datetime(2024, 1, 1).date()),
            ])
