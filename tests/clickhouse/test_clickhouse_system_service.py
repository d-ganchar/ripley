from parameterized import parameterized

from ripley.clickhouse_models.db import ClickhouseDbModel
from ripley.clickhouse_models.disk import ClickhouseDiskModel
from ripley.clickhouse_models.partition import ClickhousePartitionModel
from ripley.clickhouse_models.table import ClickhouseTableModel
from tests.clickhouse._base_test import BaseClickhouseTest, DB


class TestClickhouseSystemService(BaseClickhouseTest):
    def test_get_disks(self):
        disks = self.clickhouse.get_disks()
        self.assertListEqual(
            disks,
            [
                ClickhouseDiskModel(**{
                    'cache_path': '', 'is_broken': 0, 'is_encrypted': 0, 'is_read_only': 0, 'is_remote': 0,
                    'is_write_once': 0, 'keep_free_space': 0, 'name': 'default', 'path': '/var/lib/clickhouse/',
                    'type': 'Local', 'total_space': disks[0].total_space, 'free_space': disks[0].free_space,
                    'unreserved_space': disks[0].unreserved_space,
                })
            ])

    @parameterized.expand([
        [DB.RIPLEY_TESTS.value],
        [DB.RIPLEY_TESTS2.value],
    ])
    def test_get_partition(self, db_name: str):
        table = 'get_partition'
        self.create_test_table(table, db_name)
        partitions = self.clickhouse.get_table_partitions(table, db_name)
        self.assertListEqual(
            [
                ClickhousePartitionModel(**{
                    'database': db_name, 'table': table, 'partition': '2024-01-01', 'partition_id': '20240101',
                    'active': 1, 'rows': 666,  'data_uncompressed_bytes': 11844,
                    'data_compressed_bytes': partitions[0].data_compressed_bytes,
                    'bytes_on_disk': partitions[0].bytes_on_disk,
                    'visible': 1,
                }),
                ClickhousePartitionModel(**{
                    'database': db_name, 'table': table, 'partition': '2025-01-01', 'partition_id': '20250101',
                    'active': 1, 'rows': 334, 'data_uncompressed_bytes': 5936,
                    'data_compressed_bytes': partitions[1].data_compressed_bytes,
                    'bytes_on_disk': partitions[1].bytes_on_disk,
                    'visible': 1,
                })
            ],
            partitions,
        )

    def test_get_databases(self):
        databases = self.clickhouse.get_databases()
        expected = [
            ClickhouseDbModel(**{
                'name': 'default',
                'uuid': '',
                'engine': 'Atomic',
                'data_path': '/var/lib/clickhouse/store/',
                'metadata_path': '',
                'engine_full': 'Atomic',
                'comment': '',
            }),
            ClickhouseDbModel(**{
                'name': DB.RIPLEY_TESTS.value,
                'uuid': '',
                'engine': 'Atomic',
                'data_path': '/var/lib/clickhouse/store/',
                'metadata_path': '',
                'engine_full': 'Atomic',
                'comment': '',
            }),
            ClickhouseDbModel(**{
                'name': DB.RIPLEY_TESTS2.value,
                'uuid': 'uuid',
                'engine': 'Atomic',
                'data_path': '/var/lib/clickhouse/store/',
                'metadata_path': '',
                'engine_full': 'Atomic',
                'comment': '',
            })
        ]

        for ix, database in enumerate(databases):
            expected[ix].uuid = database.uuid
            expected[ix].metadata_path = database.metadata_path

        self.assertEqual(databases, expected)

    @parameterized.expand([
        [DB.RIPLEY_TESTS.value],
        [DB.RIPLEY_TESTS2.value],
    ])
    def test_get_tables_by_db(self, db_name: str):
        table1 = 'get_tables_by_db1'
        table2 = 'get_tables_by_db2'
        self.create_test_table(table1, db_name)
        self.create_test_table(table2, db_name)
        tables = self.clickhouse.get_tables_by_db(db_name)

        self.assertListEqual(
            [
                ClickhouseTableModel(**{
                    'active_parts': 2, 'as_select': '', 'database': db_name,
                    'dependencies_database': [], 'dependencies_table': [], 'comment': '',
                    'create_table_query': f'CREATE TABLE {db_name}.{table1} '
                                          f'(`key` UInt64, `value` String, `day` Date) '
                                          f'ENGINE = MergeTree PARTITION BY day ORDER BY key '
                                          f'SETTINGS index_granularity = 8192',
                    'name': tables[0].name, 'engine': 'MergeTree',
                    'engine_full': 'MergeTree PARTITION BY day ORDER BY key SETTINGS index_granularity = 8192',
                    'parts': 2,
                    'partition_key': 'day', 'primary_key': 'key', 'sampling_key': '', 'storage_policy': 'default',
                    'sorting_key': 'key', 'is_temporary': 0, 'total_bytes': 12667, 'total_rows': 1000,
                    'total_marks': 4, 'lifetime_rows': None, 'lifetime_bytes': None, 'has_own_data': 1,
                    'loading_dependencies_database': [], 'loading_dependencies_table': [],
                    'loading_dependent_database': [], 'loading_dependent_table': [],
                    'data_paths': tables[0].data_paths,
                    'metadata_modification_time': tables[0].metadata_modification_time,
                    'metadata_path': tables[0].metadata_path,
                    'uuid': tables[0].uuid,
                    'metadata_version': 0,
                    'total_bytes_uncompressed': 18036,
                    'active_on_fly_data_mutations': 0,
                    'active_on_fly_alter_mutations': 0,
                    'active_on_fly_metadata_mutations':0,
                    'parameterized_view_parameters': [],
                }),
                ClickhouseTableModel(**{
                    'active_parts': 2, 'as_select': '', 'database': db_name, 'dependencies_database': [],
                    'dependencies_table': [], 'comment': '',
                    'create_table_query':
                        f'CREATE TABLE {db_name}.{table2} '
                        '(`key` UInt64, `value` String, `day` Date) ENGINE = MergeTree PARTITION BY day '
                        'ORDER BY key SETTINGS index_granularity = 8192',
                    'name': tables[1].name, 'engine': 'MergeTree',
                    'engine_full': 'MergeTree PARTITION BY day ORDER BY key SETTINGS index_granularity = 8192',
                    'parts': 2,
                    'partition_key': 'day', 'primary_key': 'key', 'sampling_key': '', 'storage_policy': 'default',
                    'sorting_key': 'key', 'is_temporary': 0, 'total_bytes': 12667, 'total_rows': 1000,
                    'total_marks': 4, 'lifetime_rows': None, 'lifetime_bytes': None, 'has_own_data': 1,
                    'loading_dependencies_database': [], 'loading_dependencies_table': [],
                    'loading_dependent_database': [], 'loading_dependent_table': [],
                    'data_paths': tables[1].data_paths,
                    'metadata_modification_time': tables[1].metadata_modification_time,
                    'metadata_path': tables[1].metadata_path,
                    'metadata_version': 0,
                    'uuid': tables[1].uuid,
                    'total_bytes_uncompressed': 18036,
                    'active_on_fly_data_mutations': 0,
                    'active_on_fly_alter_mutations': 0,
                    'active_on_fly_metadata_mutations': 0,
                    'parameterized_view_parameters': [],
                })
            ],
            tables,
        )
