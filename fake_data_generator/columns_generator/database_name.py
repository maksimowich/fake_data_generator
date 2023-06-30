from enum import Enum


class DatabaseName(Enum):
    postgresql = {
        'random_clause': 'RANDOM()',
        'create_query': 'CREATE TABLE {dest_table_name_with_schema} AS '
                        'SELECT {string_for_column_names} '
                        'FROM {source_table_name_with_schema} '
                        'WHERE 1<>1;',
    }

    clickhouse = {
        'random_clause': 'RAND()',
        'create_query': 'CREATE TABLE {dest_table_name_with_schema} '
                        'ENGINE = MergeTree() ORDER BY ({order_columns}) SETTINGS allow_nullable_key=1 AS '
                        'SELECT {string_for_column_names} '
                        'FROM {source_table_name_with_schema} '
                        'WHERE 1<>1;',
    }

    @property
    def random_clause(self):
        return self.value['random_clause']

    @property
    def create_query(self):
        return self.value['create_query']
