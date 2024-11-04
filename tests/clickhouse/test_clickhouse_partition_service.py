from datetime import datetime

from parameterized import parameterized

from tests.clickhouse._base_test import BaseClickhouseTest, DB


class TestClickhousePartitionService(BaseClickhouseTest):
    @parameterized.expand([
        [DB.RIPLEY_TESTS.value, DB.RIPLEY_TESTS2.value],
        [DB.RIPLEY_TESTS2.value, DB.RIPLEY_TESTS.value],
    ])
    def test_move_partition(self, from_db: str, to_db: str):
        from_table_name = 'move_partition'
        to_table_name = f'to_{from_table_name}'

        self.create_test_table(from_table_name, from_db)
        from_table = self.clickhouse.get_table_by_name(from_table_name, from_db)
        to_table = self.clickhouse.create_table_as(
            from_table=from_table,
            table=to_table_name,
            db=to_db,
        )

        def _get_partition(_table: str, _db: str, _partition: str):
            _db = _db or self.clickhouse.active_db
            return self.clickhouse.exec(f"""
                SELECT partition,
                   active,
                   visible,
                   sum(rows) AS rows,
                   sum(data_uncompressed_bytes) AS data_uncompressed_bytes
              FROM system.parts
             WHERE database = %(_db)s AND table = %(_table)s AND partition = %(_partition)s
               AND active = 1
               AND visible = 1
             GROUP BY partition, active, visible
            """, params={'_db': _db, '_table': _table, '_partition': _partition})

        partition = '2024-01-01'
        origin_partition = _get_partition(from_table_name, from_db, partition)
        self.clickhouse.move_partition(
            from_table=from_table,
            to_table=to_table,
            partition=partition,
        )

        target_partition = _get_partition(to_table_name, to_db, partition)
        self.assertEqual(origin_partition, target_partition)
        self.assertListEqual([], _get_partition(from_table_name, from_db, partition))

    @parameterized.expand([
        [DB.RIPLEY_TESTS.value, '2024-01-01', [('2025-01-01',)]],
        [DB.RIPLEY_TESTS2.value, '2025-01-01', [('2024-01-01',)]],
    ])
    def test_drop_partition(self, db_name: str, partition: str, expected: list):
        table_name = 'drop_partition'

        self.create_test_table(table_name, db_name)
        table = self.clickhouse.get_table_by_name(table_name, db_name)
        self.clickhouse.drop_partition(table, partition)

        result = self.clickhouse.exec("""
            SELECT DISTINCT partition
              FROM system.parts
            WHERE database = %(db_name)s AND table = %(table)s
              AND rows > 0
        """, params={'db_name': db_name, 'table': table_name})

        self.assertEqual(result, expected)

    @parameterized.expand([
        [
            DB.RIPLEY_TESTS.value,
            DB.RIPLEY_TESTS2.value,
            '2024-01-01',
            335,
            [(0, 'replace_partition_test', datetime(2024, 1, 1).date())],
        ],
        [
            DB.RIPLEY_TESTS2.value,
            DB.RIPLEY_TESTS.value,
            '2025-01-01',
            667,
            [(0, 'replace_partition_test', datetime(2025, 1, 1).date())],
        ],
    ])
    def test_replace_partition(
        self,
        source_db: str,
        target_db: str,
        partition: str,
        expected_count: int,
        expected: list,
    ):
        new_data_table = 'replace_partition'
        old_data_table = 'replace_partition_from'

        self.create_test_table(old_data_table, source_db)
        old_table = self.clickhouse.get_table_by_name(old_data_table, source_db)

        self.clickhouse.create_table_as(from_table=old_table, table=new_data_table, db=target_db)
        new_table = self.clickhouse.get_table_by_name(new_data_table, target_db)

        self.clickhouse.exec(f"""
            INSERT INTO {new_table.full_name}
            SELECT 0, 'replace_partition_test', '{partition}'
        """)

        self.clickhouse.replace_partition(from_table=new_table, to_table=old_table, partition=partition)
        result = self.clickhouse.exec(f'SELECT count(*) AS records FROM {old_table.full_name}')
        self.assertEqual(result, [(expected_count,)])

        for table_name in [new_table.full_name, old_table.full_name]:
            result = self.clickhouse.exec(f"SELECT * FROM {table_name} WHERE day = '{partition}'")
            self.assertEqual(result, expected)

    @parameterized.expand([
        [DB.RIPLEY_TESTS.value, '2025-01-01'],
        [DB.RIPLEY_TESTS2.value, '2024-01-01'],
    ])
    def test_detach_attach_partition(self, db_name: str, partition: str):
        def select_is_active(_table: str, _db: str, _partition: str):
            return self.clickhouse.exec("""
                SELECT active
                  FROM system.parts
                 WHERE table = %(_table)s
                   AND database = %(_db)s AND partition = %(_partition)s
                   AND visible = 1
            """, params={'_table': _table, '_db': _db, '_partition': _partition})

        table_name = 'detach_attach_partition'
        self.create_test_table(table_name, db_name)
        table = self.clickhouse.get_table_by_name(table_name, db_name)
        self.clickhouse.detach_partition(table, partition)

        active = select_is_active(table_name, db_name, partition)
        self.assertEqual(active, [])
        self.clickhouse.attach_partition(table, partition)

        active = select_is_active(table_name, db_name, partition)
        self.assertEqual(active, [(1,)])
