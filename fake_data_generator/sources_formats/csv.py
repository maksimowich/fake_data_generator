import json
import pandas as pd
from fake_data_generator.columns_generator import get_fake_data_for_columns, CsvColumnInfo
from loguru import logger


def json_to_dict(json_str: str):
    return json.loads(json_str.replace("'", "\""))


def generate_fake_csv_file(source_file_name: str,
                           dest_file_name: str,
                           output_size: int,
                           number_of_rows_from_which_to_create_pattern: int,
                           columns_info: list[CsvColumnInfo] = None,
                           columns_to_include: list[str] = None,
                           encoding='utf-8'):
    logger.info(f"generate_fake_csv_file function was called."
                f"\n\tSource csv: {source_file_name}"
                f"\n\tColumns of source csv to include in destination csv: ({columns_to_include if columns_to_include is not None else '*'})"
                f"\n\tDestination csv: {dest_file_name}"
                f"\n\tNumber of rows to insert into destination csv: {output_size}"
                f"\n\tNumber of source file`s rows from which to create pattern: {number_of_rows_from_which_to_create_pattern}")

    if columns_info is None:
        columns_info = []

    type_converter = {}
    for column_info in columns_info:
        if column_info.get_json_flag():
            type_converter[column_info.get_column_name()] = json_to_dict

    logger.info(f'Start selecting from {source_file_name}.')
    source_csv_data_in_df = pd.read_csv(filepath_or_buffer=source_file_name,
                                        usecols=columns_to_include,
                                        nrows=number_of_rows_from_which_to_create_pattern,
                                        parse_dates=[column_info.get_column_name() for column_info in columns_info if column_info.get_datetime_flag()],
                                        encoding=encoding,
                                        converters=type_converter)
    logger.info('Csv file was read into Dataframe.')

    fake_data_in_df = get_fake_data_for_columns(data_in_df=source_csv_data_in_df,
                                                output_size=output_size,
                                                columns_info=columns_info)

    logger.info(f'Start saving fake data into {dest_file_name}.')
    fake_data_in_df.to_csv(dest_file_name, index=False, encoding=encoding)
    logger.info(f'Fake data was saved into {dest_file_name}.')
