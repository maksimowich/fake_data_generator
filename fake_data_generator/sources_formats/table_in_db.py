import pandas as pd
import sqlalchemy
from fake_data_generator.columns_generator import get_fake_data_for_columns, ColumnInfo, DatabaseName
from sqlalchemy import text
from loguru import logger


def generate_fake_table(engine: sqlalchemy.engine.base.Engine,
                        source_table_name_with_schema: str,
                        dest_table_name_with_schema: str,
                        output_size: int,
                        number_of_rows_from_which_to_create_pattern: int,
                        columns_info: list[ColumnInfo] = None,
                        columns_to_include: list[str] = None,
                        order_columns: list[str] = None):
    """
    Function that generates table filled with fake data.
    Fake data is generated based on data of specified source table.

    Parameters
    ----------
     engine: sqlalchemy.engine.base.Engine
     source_table_name_with_schema: Source table name with schema
     dest_table_name_with_schema: Name with schema of table that will be generated and filled with fake data
     output_size: Number of rows to insert into destination table
     number_of_rows_from_which_to_create_pattern: Number of rows of source table from which to infer patterns of data
     columns_info: List of TableColumnInfo objects that specify additional information
     columns_to_include: Columns that will be included in destination table. If not specified then all columns are included
     order_columns: Name of columns by which table is ordered. Only relevant with ClickHouse database

    Returns
    -------
     Nothing. Only generate table in database
    """
    string_for_column_names = ','.join(map(lambda x: '"' + x + '"', columns_to_include)) if columns_to_include is not None else '*'
    db = DatabaseName.__members__[engine.name]

    logger.info(f"generate_fake_postgres_table function was called."
                f"\n\tSource table: {source_table_name_with_schema}"
                f"\n\tColumns of source table to include in destination table: ({string_for_column_names})"
                f"\n\tDestination table: {dest_table_name_with_schema}"
                f"\n\tNumber of rows to insert into destination table: {output_size}"
                f"\n\tNumber of source table`s rows from which to create pattern: {number_of_rows_from_which_to_create_pattern}"
                f"\n\tDatabase: {db}")

    logger.info(f'Start making select-query from table {source_table_name_with_schema}.')
    limit_clause = f"LIMIT {number_of_rows_from_which_to_create_pattern}" if number_of_rows_from_which_to_create_pattern is not None else ''
    table_data_in_df = pd.read_sql_query(sql=f"SELECT {string_for_column_names} "
                                             f"FROM {source_table_name_with_schema} "
                                             f"ORDER BY {db.random_clause} "
                                             f"{limit_clause}",
                                         con=engine)
    logger.info(f'Select-query result was read into Dataframe. Number of rows fetched is {table_data_in_df.shape[0]}.')

    fake_data_in_df = get_fake_data_for_columns(data_in_df=table_data_in_df,
                                                output_size=output_size,
                                                columns_info=columns_info,
                                                engine=engine)

    logger.info(f'Start recreating {dest_table_name_with_schema} table.')
    with engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS {dest_table_name_with_schema};'))
        conn.execute(db.create_query.format(dest_table_name_with_schema=dest_table_name_with_schema,
                                            string_for_column_names=string_for_column_names,
                                            source_table_name_with_schema=source_table_name_with_schema,
                                            order_columns=','.join(map(lambda x: '"' + x + '"', order_columns or []))))
    logger.info(f'{dest_table_name_with_schema} table was recreated.')

    logger.info(f'Start inserting generated fake data into {dest_table_name_with_schema} table.')
    number_of_rows_inserted = fake_data_in_df.to_sql(name=dest_table_name_with_schema.split('.')[1],
                                                     schema=dest_table_name_with_schema.split('.')[0],
                                                     con=engine,
                                                     index=False,
                                                     if_exists='append')

    if number_of_rows_inserted == -1:
        with engine.begin() as conn:
            number_of_rows_inserted = conn.execute(f'SELECT COUNT(*)'
                                                   f'FROM {dest_table_name_with_schema};').scalar()

    logger.info(f'Insertion of fake data into {dest_table_name_with_schema} was finished.'
                f'\n\tNumber of rows inserted: {number_of_rows_inserted}\n')
