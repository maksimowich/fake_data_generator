import json
import pandas as pd
import sqlalchemy
import re
from copy import deepcopy
from loguru import logger
from pandas import concat, to_datetime, Series
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType, DecimalType
from fake_data_generator.columns_generator import get_rich_column_info, get_fake_data_for_insertion, Column
from fake_data_generator.columns_generator.get_fake_date_for_columns import get_columns_info_with_set_generators


def infer_data_type(column_data_type):
    if column_data_type == 'string':
        return StringType()
    elif 'int' in column_data_type:
        return IntegerType()
    elif 'decimal' in column_data_type:
        precision, scale = re.search(r'decimal\((\d+),(\d+)\)', column_data_type).groups()
        precision = int(precision)
        scale = int(scale)
        return DecimalType(precision, scale)
    elif column_data_type == 'timestamp':
        return TimestampType()
    elif column_data_type == 'date':
        return DateType()
    else:
        return StringType()


def get_correct_column_values(column_values: Series, column_data_type: str):
    number_of_nulls = column_values.isnull().sum()
    if 'int' in column_data_type:
        correct_column_values = concat([column_values.dropna().astype(int), Series([None] * number_of_nulls)])
        correct_column_values.name = column_values.name
        return correct_column_values
    elif 'decimal' in column_data_type:
        correct_column_values = concat([column_values.dropna().astype(float), Series([None] * number_of_nulls, dtype=float)])
        correct_column_values.name = column_values.name
        return correct_column_values
    elif column_data_type == 'date':
        return to_datetime(column_values).dt.date
    elif column_data_type == 'timestamp':
        return column_values.apply(lambda x: x.to_pydatetime())
    else:
        return column_values


def get_string_for_column_names(columns_to_include):
    return ','.join(map(lambda x: '`' + x + '`', columns_to_include)) if columns_to_include is not None else '*'


def get_create_query(dest_table_name_with_schema, rich_columns_info_dict):
    str_for_column_names_and_types = ', '.join([f"{column_name} {column_info_dict['data_type']}" for column_name, column_info_dict in rich_columns_info_dict.items()])
    return f"CREATE TABLE IF NOT EXISTS {dest_table_name_with_schema} ({str_for_column_names_and_types});"


def get_rich_columns_info(conn,
                          source_table_name_with_schema: str,
                          number_of_rows_from_which_to_create_pattern: int,
                          columns_info: list[Column] = None,
                          columns_to_include: list[str] = None):
    logger.info(f'Start making describe-query from table {source_table_name_with_schema}.')
    describe_query = f"DESCRIBE {source_table_name_with_schema};"
    if isinstance(conn, sqlalchemy.engine.base.Engine):
        with conn.begin() as c:
            describe_data_in_df = pd.read_sql_query(describe_query, c).rename(columns={'name': 'col_name', 'type': 'data_type'})
    else:
        describe_data_in_df = conn.sql(describe_query).toPandas().rename(columns={'name': 'col_name', 'type': 'data_type'})
    logger.info(f'Describe-query result was read into Dataframe. Number of rows fetched is {describe_data_in_df.shape[0]}.')

    logger.info(f'Start making select-query from table {source_table_name_with_schema}.')
    limit_clause = f"LIMIT {number_of_rows_from_which_to_create_pattern}" if number_of_rows_from_which_to_create_pattern is not None else ''
    select_query = f"SELECT {get_string_for_column_names(columns_to_include)} " \
                   f"FROM {source_table_name_with_schema} " \
                   f"ORDER BY RANDOM() {limit_clause};"
    if isinstance(conn, sqlalchemy.engine.base.Engine):
        with conn.begin() as c:
            table_data_in_df = pd.read_sql_query(select_query, c)
    else:
        table_data_in_df = conn.sql(select_query).toPandas()
    logger.info(f'Select-query result was read into Dataframe. Number of rows fetched is {table_data_in_df.shape[0]}.')

    column_name_to_column_info_in_dict = {column_info.get_column_name(): column_info for column_info in deepcopy(columns_info) or []}
    rich_columns_info = []
    for _, row in describe_data_in_df.iterrows():
        if columns_to_include is None or row['col_name'] in columns_to_include:
            column_info = column_name_to_column_info_in_dict.get(row['col_name'], Column(column_name=row['col_name']))
            column_info.set_data_type(row['data_type'])
            rich_columns_info.append(get_rich_column_info(column_values=get_correct_column_values(table_data_in_df[column_info.get_column_name()], row['data_type']),
                                                          column_info=column_info))
    return rich_columns_info


def create_table(conn,
                 source_table_name_with_schema=None,
                 dest_table_name_with_schema=None,
                 columns_to_include=None,
                 create_query=None):
    logger.info(f'Start creating {dest_table_name_with_schema} table.')
    if create_query is None:
        create_query = f'CREATE TABLE IF NOT EXISTS {dest_table_name_with_schema} AS ' \
                       f'SELECT {get_string_for_column_names(columns_to_include)} ' \
                       f'FROM {source_table_name_with_schema} WHERE 1<>1;'
    if isinstance(conn, sqlalchemy.engine.base.Engine):
        with conn.begin() as c:
            c.execute(create_query)
    else:
        conn.sql(create_query)
    logger.info(f'{dest_table_name_with_schema} table was created.')


def execute_insertion(conn,
                      dest_table_name_with_schema,
                      number_of_rows_to_insert,
                      columns_info_with_set_generators,
                      batch_size):
    schema = None
    if not isinstance(conn, sqlalchemy.engine.base.Engine):
        schema = StructType([StructField(column_info.get_column_name(), infer_data_type(column_info.get_data_type()), True) for column_info in columns_info_with_set_generators])
    number_of_rows_left_to_insert = number_of_rows_to_insert
    while number_of_rows_left_to_insert != 0:
        logger.info(f'-----------Start generating batch of fake data-----------')
        fake_data_in_df = get_fake_data_for_insertion(output_size=min(batch_size, number_of_rows_left_to_insert),
                                                      columns_info_with_set_generator=columns_info_with_set_generators)
        logger.info(f'--------Finished generating batch of fake data-----------')

        logger.info(f'Start inserting generated fake data into {dest_table_name_with_schema} table.')
        if isinstance(conn, sqlalchemy.engine.base.Engine):
            fake_data_in_df.to_sql(con=conn,
                                   name=dest_table_name_with_schema.split('.')[1],
                                   schema=dest_table_name_with_schema.split('.')[0],
                                   if_exists='append',
                                   index=False)
        else:
            fake_data_in_df_spark = conn.createDataFrame(fake_data_in_df, schema=schema)
            fake_data_in_df_spark.write.format('hive').mode('append').saveAsTable(dest_table_name_with_schema)
        number_of_rows_left_to_insert -= min(batch_size, number_of_rows_left_to_insert)
        logger.info(f'Insertion of fake data into {dest_table_name_with_schema} was finished.\n'
                    f'\tNumber of rows left to insert: {number_of_rows_left_to_insert}')


def generate_fake_table(conn,
                        source_table_name_with_schema: str,
                        dest_table_name_with_schema: str,
                        number_of_rows_to_insert: int,
                        number_of_rows_from_which_to_create_pattern: int,
                        columns_info: list[Column] = None,
                        columns_to_include: list[str] = None,
                        batch_size=100):
    logger.info(f"generate_fake_table function was called."
                f"\n\tSource table: {source_table_name_with_schema}"
                f"\n\tDestination table: {dest_table_name_with_schema}"
                f"\n\tNumber of rows to insert into destination table: {number_of_rows_to_insert}"
                f"\n\tNumber of source table`s rows from which to create pattern: {number_of_rows_from_which_to_create_pattern}")

    rich_columns_info = get_rich_columns_info(conn,
                                              source_table_name_with_schema,
                                              number_of_rows_from_which_to_create_pattern,
                                              columns_info,
                                              columns_to_include)

    create_table(conn, source_table_name_with_schema, dest_table_name_with_schema, columns_to_include)
    execute_insertion(conn, dest_table_name_with_schema, number_of_rows_to_insert, rich_columns_info, batch_size)


def generate_table_profile(conn,
                           source_table_name_with_schema: str,
                           output_table_profile_path: str,
                           number_of_rows_from_which_to_create_pattern: int,
                           columns_info: list[Column] = None,
                           columns_to_include: list[str] = None):
    logger.info(f"generate_table_profile function was called."
                f"\n\tSource table: {source_table_name_with_schema}"
                f"\n\tNumber of source table`s rows from which to create pattern: {number_of_rows_from_which_to_create_pattern}")

    rich_columns_info = get_rich_columns_info(conn,
                                              source_table_name_with_schema,
                                              number_of_rows_from_which_to_create_pattern,
                                              columns_info,
                                              columns_to_include)

    dict_to_dump = {}
    for column_info in rich_columns_info:
        dict_to_dump.update(column_info.get_as_dict())

    with open(output_table_profile_path, 'w') as file:
        json.dump(dict_to_dump, file)
    logger.info(f'Profile was loaded into {output_table_profile_path}.')


def generate_table_from_profile(conn,
                                source_table_profile_path: str,
                                dest_table_name_with_schema: str,
                                number_of_rows_to_insert: int,
                                columns_info=None,
                                batch_size=100):
    logger.info(f"generate_fake_table function was called."
                f"\n\tSource table profile path: {source_table_profile_path}"
                f"\n\tDestination table: {dest_table_name_with_schema}"
                f"\n\tNumber of rows to insert into destination table: {number_of_rows_to_insert}")

    with open(source_table_profile_path, 'r') as file:
        rich_columns_info_dict = json.load(file)

    for column_info in columns_info or []:
        rich_columns_info_dict.update(column_info.get_as_dict())

    create_table(conn, dest_table_name_with_schema=dest_table_name_with_schema, create_query=get_create_query(dest_table_name_with_schema, rich_columns_info_dict))
    columns_info_with_set_generators = get_columns_info_with_set_generators(rich_columns_info_dict)
    execute_insertion(conn, dest_table_name_with_schema, number_of_rows_to_insert, columns_info_with_set_generators, batch_size)
