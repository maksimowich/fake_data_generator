import json
import random
import numpy as np
import sqlalchemy
from fake_data_generator.columns_generator.helper_functions import \
    get_common_regex,\
    get_list_of_series_for_json_column, \
    set_value_by_key_path
from fake_data_generator.columns_generator.column_info import ColumnInfo
from fake_data_generator.columns_generator.database_name import DatabaseName
from datetime import timedelta, date
from loguru import logger
from pandas import \
    DataFrame, Series, Timestamp, \
    concat, read_sql_query, read_csv
from pathlib import Path
from random import randint, uniform
from rstr import xeger
from scipy.stats import gaussian_kde
from typing import Callable


def get_fake_data_for_categorical_column(column_values: Series,
                                         output_size: int) -> Series:
    """
    Function that returns random sample generated from frequency distribution of passed values.

    Parameters
    ----------
     column_values: Values from which to create frequency distribution and generate random sample based on created frequency distribution
     output_size: Size of returned random sample

    Returns
    -------
     Random sample of specified size generated from frequency distribution of passed values
    """
    if len(column_values) == 0:
        return Series([], name=column_values.name)

    normalized_frequencies_of_values = column_values.value_counts(normalize=True, dropna=False)
    fake_sample = np.random.choice(a=normalized_frequencies_of_values.index.tolist(),
                                   p=normalized_frequencies_of_values.to_list(),
                                   size=output_size,
                                   replace=True)
    return Series(fake_sample, name=column_values.name)


def get_fake_data_for_number_column(column_values: Series,
                                    output_size: int) -> Series:
    """
    Function that returns random sample generated from kernel density estimation of passed number values.

    Parameters
    ----------
     column_values: Number values from which to create kernel density estimation and generate random sample based on created kernel density estimation
                    Type of values can be int or float
     output_size: Size of returned random sample

    Returns
    -------
     Random sample of specified size generated from kernel density estimation of passed number values
    """
    column_values_without_nulls = column_values.dropna()

    if len(column_values_without_nulls) == 0:
        return Series([None] * output_size, name=column_values.name)
    elif len(column_values_without_nulls) == 1:
        if column_values.dtype == np.int64:
            fake_sample = [int(random.uniform(column_values.iloc[0] * 0.5, column_values.iloc[0] * 1.5)) for _ in range(output_size)]
        else:
            fake_sample = [random.uniform(column_values.iloc[0] * 0.5, column_values.iloc[0] * 1.5) for _ in range(output_size)]
        return Series(fake_sample, name=column_values.name)
    else:
        kde = gaussian_kde(column_values_without_nulls.values)
        x = np.linspace(min(column_values_without_nulls), max(column_values_without_nulls), num=1000)
        pdf = kde.evaluate(x)

        if column_values.dtype == np.float64:
            precisions = [len(str(float_number).split('.')[-1]) for float_number in column_values]

        fake_sample = map(lambda value: int(value) if column_values.dtype == np.int64 else round(value, random.choice(precisions)),
                          np.random.choice(a=x, size=output_size, p=pdf/sum(pdf), replace=True))
        return Series(fake_sample, name=column_values.name)


def get_fake_data_for_datetime_column(column_values: Series,
                                      output_size: int) -> Series:
    """
    Function that returns random sample of dates or dates with time generated from uniform distribution
    with start and end corresponding to min and max dates or dates with time of passed values.

    Parameters
    ----------
     column_values: Values of Timestamp type
     output_size: Size of returned random sample

    Returns
    -------
     Random sample of dates or dates with time of specified size generated from uniform distribution
    """
    column_values_without_nulls = column_values.dropna()
    if len(column_values_without_nulls) == 0 or max(column_values) == min(column_values):
        return Series([None] * output_size, name=column_values.name)

    dates_flag = (type(column_values_without_nulls.loc[0]) is date) or \
        all(time_component == Timestamp('2000-01-01').time() for time_component in column_values.dt.time)

    if dates_flag:
        if not (type(column_values_without_nulls.loc[0]) is date):
            start_date = min(column_values).date()
            end_date = max(column_values).date()
        else:
            start_date = min(column_values)
            end_date = max(column_values)
        range_in_days = (end_date - start_date).days
        fake_dates = [start_date + timedelta(days=randint(1, range_in_days)) for _ in range(output_size)]
        return Series(fake_dates, name=column_values.name)
    else:
        start_timestamp = min(column_values)
        end_timestamp = max(column_values)
        range_in_sec = (end_timestamp - start_timestamp).total_seconds()
        fake_timestamps = [(start_timestamp + timedelta(seconds=uniform(0, range_in_sec))).replace(microsecond=0)
                           for _ in range(output_size)]
        return Series(fake_timestamps, name=column_values.name)


def get_fake_data_for_string_column(column_values: Series,
                                    output_size: int,
                                    regex: str = None,
                                    faker_function: Callable = None) -> Series:
    """
    Function that returns random sample of strings either generated based on common regular expression
    or generated by passed faker function.

    Parameters
    ----------
     column_values: Strings from which to create pattern
     output_size: Size of returned random sample
     regex: Regular expression from which random sample will be generated
     faker_function: Faker function that returns random stings to be added in returned sample

    Returns
    -------
     Random sample of strings of specified size
    """
    if len(column_values.dropna()) == 0 and faker_function is None and regex is None:
        return Series([None] * output_size, name=column_values.name)

    if faker_function is None:
        common_regex = regex or get_common_regex(column_values)
        list_of_fake_strings = [xeger(common_regex) for _ in range(output_size)]
    else:
        list_of_fake_strings = [faker_function() for _ in range(output_size)]
    return Series(list_of_fake_strings, name=column_values.name)


def get_data_for_id_column(column_values: Series,
                           output_size: int) -> Series:
    """
    Function that returns ids.
    If values are of type int then incremental int ids are returned.
    If values are of type string then string ids with found pattern are returned.

    Parameters
    ----------
     column_values: Values of real ids
     output_size: Size of returned Series of ids

    Returns
    -------
     Series of ids of specified size
    """
    if column_values.dtype == np.int64:
        list_of_ids = [incremental_id for incremental_id in range(1, output_size + 1)]
    else:
        set_of_ids = set()
        common_regex = get_common_regex(column_values)
        for _ in range(output_size):
            while True:
                fake_id_in_string = xeger(common_regex)
                if fake_id_in_string not in set_of_ids:
                    set_of_ids.add(fake_id_in_string)
                    break
        list_of_ids = list(set_of_ids)
    return Series(list_of_ids, name=column_values.name)


def get_fake_data_for_fk_column(ref_table_name_with_schema_or_file_path: str,
                                ref_column_name: str,
                                dest_column_name: str,
                                output_size: int,
                                engine: sqlalchemy.engine.base.Engine = None,
                                ref_file_encoding: str = 'utf-8') -> Series:
    if engine is not None:
        fake_data = DataFrame(columns=[ref_column_name])
        db = DatabaseName.__members__[engine.name]
        random_clause = db.random_clause if db is not None else 'RANDOM()'
        while output_size > len(fake_data):
            fake_data = concat(objs=[fake_data, read_sql_query(sql=f"SELECT {ref_column_name} FROM {ref_table_name_with_schema_or_file_path} "
                                                                   f"ORDER BY {random_clause} LIMIT {output_size - fake_data.shape[0]}",
                                                               con=engine)],
                               ignore_index=True)
        return fake_data[ref_column_name].rename(dest_column_name)
    else:
        extension = Path(ref_table_name_with_schema_or_file_path).suffix
        if extension == '.csv':
            csv_in_df = read_csv(filepath_or_buffer=ref_table_name_with_schema_or_file_path,
                                 usecols=[ref_column_name],
                                 encoding=ref_file_encoding)
            fake_sample = csv_in_df.sample(n=output_size, replace=True, ignore_index=True)
            return fake_sample[ref_column_name].rename(dest_column_name)
        else:
            return Series([None] * output_size, name=dest_column_name)


def get_fake_data_for_json(column_values: Series,
                           output_size: int,
                           column_info: ColumnInfo = None,
                           engine: sqlalchemy.engine.base.Engine = None):
    list_of_series = get_list_of_series_for_json_column(column_values)
    list_of_fake_dicts = [{} for _ in range(output_size)]

    if column_info.get_json_info() is not None:
        json_info_in_dict = {column_info.get_column_name(): column_info for column_info in column_info.get_json_info()}
    else:
        json_info_in_dict = {}

    for series in list_of_series:
        fake_json_series = get_fake_data_for_column(column_values=series,
                                                    output_size=output_size,
                                                    column_info=json_info_in_dict.get(series.name),
                                                    engine=engine)
        for index, value in enumerate(fake_json_series):
            key_path = fake_json_series.name.split('->')
            set_value_by_key_path(list_of_fake_dicts[index], key_path, value)

    return Series(data=map(lambda x: json.dumps(x, ensure_ascii=False), list_of_fake_dicts),
                  name=column_values.name)


def get_fake_data_for_column(column_values: Series,
                             output_size: int,
                             column_info: ColumnInfo = None,
                             engine: sqlalchemy.engine.base.Engine = None) -> Series:
    if column_info is None:
        column_info = ColumnInfo()

    if column_info.get_fk() is not None:
        logger.info(f'Column "{column_values.name}" — FOREIGN KEY -> {column_info.get_fk()}')
        return get_fake_data_for_fk_column(ref_table_name_with_schema_or_file_path=column_info.get_fk().get_ref_table_name_with_schema_or_file_path(),
                                           ref_column_name=column_info.get_fk().get_ref_column_name(),
                                           dest_column_name=column_values.name,
                                           output_size=output_size,
                                           engine=engine)

    if column_values.isnull().all():
        return Series(data=[None] * output_size,
                      name=column_values.name)

    json_flag = False
    for column_value in column_values:
        if isinstance(column_value, dict):
            json_flag = True
            break
        elif column_value is None:
            continue
        else:
            break
    if json_flag:
        logger.info(f'Column "{column_values.name}" — JSON COLUMN')
        return get_fake_data_for_json(column_values=column_values,
                                      output_size=output_size,
                                      column_info=column_info,
                                      engine=engine)

    categorical_column_flag = column_info.get_categorical_flag() or (column_values.nunique() / column_values.count() < 0.5)
    id_flag = column_info.get_id_flag() or False

    if categorical_column_flag:
        logger.info(f'Column "{column_values.name}" — CATEGORICAL COLUMN')
        return get_fake_data_for_categorical_column(column_values=column_values,
                                                    output_size=output_size)

    elif id_flag:
        logger.info(f'Column "{column_values.name}" — ID COLUMN')
        return get_data_for_id_column(column_values=column_values,
                                      output_size=output_size)

    else:
        if column_values.dtype == np.int64 or column_values.dtype == np.float64:
            logger.info(f'Column "{column_values.name}" — NUMBER COLUMN')
            return get_fake_data_for_number_column(column_values=column_values,
                                                   output_size=output_size)

        elif str(column_values.dtype).startswith('datetime') or isinstance(column_values.dropna().loc[0], date):
            logger.info(f'Column "{column_values.name}" — DATETIME COLUMN')
            return get_fake_data_for_datetime_column(column_values=column_values,
                                                     output_size=output_size)

        elif column_values.dtype == np.dtype(object):
            logger.info(f'Column "{column_values.name}" — STRING NON CATEGORICAL COLUMN')
            return get_fake_data_for_string_column(column_values=column_values,
                                                   output_size=output_size,
                                                   regex=column_info.get_regex(),
                                                   faker_function=column_info.get_faker_function())


def get_fake_data_for_columns(data_in_df: DataFrame,
                              output_size: int,
                              columns_info: list[ColumnInfo] = None,
                              engine: sqlalchemy.engine.base.Engine = None) -> DataFrame:
    logger.info(f'-----------Start generating fake data-----------')
    list_of_fake_column_data = []
    column_name_to_column_info_dict = {column_info.get_column_name(): column_info for column_info in (columns_info or [])}
    for index, (column_name, column_values) in enumerate(data_in_df.items()):
        logger.info(f'{index + 1}) Start generating fake data for "{column_name}" column.')
        fake_column_data_in_series = get_fake_data_for_column(column_values=column_values,
                                                              column_info=column_name_to_column_info_dict.get(column_name),
                                                              output_size=output_size,
                                                              engine=engine)
        list_of_fake_column_data.append(fake_column_data_in_series)
        logger.info(f'{index + 1}) Fake data for {column_name} was generated.')
    df_to_insert = concat(list_of_fake_column_data, axis=1)
    logger.info(f'-----------Fake data for all columns was generated-----------')
    return df_to_insert
