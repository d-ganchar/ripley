import os
from dataclasses import dataclass, field
from typing import Callable, Union, List

from .._base_model import BaseModel


@dataclass
class ClickhouseS3SettingsModel(BaseModel):
    """
    https://clickhouse.com/docs/en/integrations/s3#s3-table-functions
    """
    url: str

    compression_method: str = 'auto'
    file_format: str = 'CSV'

    access_key_id: str = field(default_factory=lambda: os.environ.get('AWS_ACCESS_KEY_ID'))
    secret_access_key: str = field(default_factory=lambda: os.environ.get('AWS_SECRET_ACCESS_KEY'))


@dataclass
class S3SelectSettingsModel(BaseModel):
    """
    S3 fields convertor settings. How to use:
    1) S3 CSV file content:
        unknown_field,{created_year},{updated_year},{need_prefix},name
        UNKNOWN_VALUE,2024-01-01,2025-01-01,eu-a1-123,Ridley Scott

    2) target table structure:
        CREATE TABLE from_s3 (
          created_year UInt64,
          updated_year UInt64,
          s3_url String,
          need_prefix String,
          name String
        )
        ENGINE MergeTree() ORDER BY created_year

    3) Script:
        clickhouse.set_settings({'input_format_skip_unknown_fields': 1})
        clickhouse.insert_from_s3(...
            s3_select_settings=S3SelectSettingsModel(
                s3_file_name_column='s3_url',
                field_name_transformer=lambda x: x if x == 'name' else ''.join(['{', x, '}']),
                field_convertors=[
                    [['created_year', 'updated_year'], 'String', lambda x: f'toYear(toDate({x}))'],
                    [['need_prefix'], 'String', lambda x: f'splitByChar(\'-\', {x})[1]'],
                ]
            )
        )

    Result query:
        INSERT INTO ...
        SELECT toYear(toDate("{created_year}")) AS created_year,
               toYear(toDate("{updated_year}")) AS updated_year,
               '{FILE_URL}' AS s3_url,
               splitByChar('-', "{need_prefix}")[1] AS need_prefix,
               "name" AS name
          FROM s3('{FILE_URL}', '*', '*', 'CSV',
               '"{created_year}" String,' ||
               '"{updated_year}" String,' ||
               '"{need_prefix}" String,' ||
               '"name" String',
               'auto'
               )
    """

    s3_file_name_column: str = ''
    """
    Adds '{FILE_URL}' automatically into a column
    """

    field_convertors: List[List[Union[List[str], Callable]]] = field(default_factory=list)
    """Converts S3 fields before INSERT. Format:
        [{LIST_OF_COLUMN_NAMES}, {S3_TYPE}, {CLICKHOUSE_FUNCTIONS}]
        Example:
            [
                [['created_year', 'updated_year'], 'String', lambda x: f'toYear(toDate({x}))'],
                [['need_prefix'], 'String', lambda x: f'splitByChar(\'-\', {x})[1]'],
            ]
    """

    field_name_transformer: Callable = None
    """How to set s3 alias for Clickhouse 'name' column:
        lambda x: 'fixed_name' if x == 'name' else x)
    """
