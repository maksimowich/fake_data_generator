from fake_data_generator.columns_generator import get_fake_data_for_columns, ColumnInfo
from loguru import logger
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType, DecimalType
import re


def infer_data_type(column_info):
    column_data_type = column_info.get_data_type()
    if column_data_type == 'string':
        return StringType()
    elif 'int' in column_data_type:
        return IntegerType()
    elif 'decimal' in column_data_type:
        precision, scale = re.findall(r'\d+', column_data_type)
        precision = int(precision)
        scale = int(scale)
        return DecimalType(precision, scale)
    elif column_data_type == 'timestamp':
        return TimestampType()
    elif column_data_type == 'date':
        return DateType()
    else:
        return StringType()


def generate_fake_table(conn,
                        source_table_name_with_schema: str,
                        dest_table_name_with_schema: str,
                        output_size: int,
                        number_of_rows_from_which_to_create_pattern: int,
                        columns_info: list[ColumnInfo] = None,
                        columns_to_include: list[str] = None,
                        number_of_batches=1):
    string_for_column_names = ','.join(map(lambda x: '`' + x + '`', columns_to_include)) if columns_to_include is not None else '*'

    logger.info(f"generate_fake_postgres_table function was called."
                f"\n\tSource table: {source_table_name_with_schema}"
                f"\n\tColumns of source table to include in destination table: ({string_for_column_names})"
                f"\n\tDestination table: {dest_table_name_with_schema}"
                f"\n\tNumber of rows to insert into destination table: {output_size}"
                f"\n\tNumber of source table`s rows from which to create pattern: {number_of_rows_from_which_to_create_pattern}")

    logger.info(f'Start making describe-query from table {source_table_name_with_schema}.')
    describe_query = f"DESCRIBE {source_table_name_with_schema};"
    describe_data_in_df = conn.sql(describe_query).toPandas()
    logger.info(f'Describe-query result was read into Dataframe. Number of rows fetched is {describe_data_in_df.shape[0]}.')

    logger.info(f'Start making select-query from table {source_table_name_with_schema}.')
    limit_clause = f"LIMIT {number_of_rows_from_which_to_create_pattern}" if number_of_rows_from_which_to_create_pattern is not None else ''
    select_query = f"SELECT {string_for_column_names} " \
                   f"FROM {source_table_name_with_schema} " \
                   f"ORDER BY RANDOM() " \
                   f"{limit_clause}"
    table_data_in_df = conn.sql(select_query).toPandas()
    logger.info(f'Select-query result was read into Dataframe. Number of rows fetched is {table_data_in_df.shape[0]}.')

    columns_info_in_dict = {column_info.get_column_name(): column_info for column_info in columns_info or []}
    columns_info = [columns_info_in_dict.get(row['col_name'], ColumnInfo(row['col_name'])).create_new_column_info_obj_with_set_data_type(row['data_type'])
                    for _, row in describe_data_in_df.iterrows() if columns_to_include is None or row['col_name'] in columns_to_include]

    for _ in range(number_of_batches):
        fake_data_in_df = get_fake_data_for_columns(data_in_df=table_data_in_df,
                                                    output_size=output_size,
                                                    columns_info=columns_info,
                                                    conn=conn)

        logger.info(f'Start recreating {dest_table_name_with_schema} table.')
        # conn.sql(f'DROP TABLE IF EXISTS {dest_table_name_with_schema};')
        conn.sql(f'CREATE TABLE IF NOT EXISTS {dest_table_name_with_schema} AS '
                 f'SELECT {string_for_column_names} '
                 f'FROM {source_table_name_with_schema} '
                 'WHERE 1<>1;')
        logger.info(f'{dest_table_name_with_schema} table was recreated.')

        logger.info(f'Start inserting generated fake data into {dest_table_name_with_schema} table.')
        schema = StructType([StructField(column_info.get_column_name(), infer_data_type(column_info), True) for column_info in columns_info])
        fake_data_in_df_spark = conn.createDataFrame(fake_data_in_df, schema=schema)
        fake_data_in_df_spark.write.format('hive').mode('append').saveAsTable(dest_table_name_with_schema)

        logger.info(f'Insertion of fake data into {dest_table_name_with_schema} was finished.')
