import random
import math
import numpy as np
from decimal import Decimal
from fake_data_generator.columns_generator.helper_functions import get_common_regex
from fake_data_generator.columns_generator.column_info import ColumnInfo
from datetime import timedelta
from loguru import logger
from pandas import DataFrame, Series, Timestamp, concat
from random import randint, uniform
from rstr import xeger
from scipy.stats import gaussian_kde
from typing import Callable


def get_generator_for_nulls(column_name: str):
    output_size = yield
    while True:
        output_size = yield Series([None] * output_size, name=column_name)


def get_fake_data_generator_for_categorical_column(column_values: Series):
    output_size = yield
    normalized_frequencies_of_values = column_values.value_counts(normalize=True, dropna=False)
    values = normalized_frequencies_of_values.index.tolist()
    probabilities = normalized_frequencies_of_values.to_list()
    while True:
        fake_sample = np.random.choice(a=values, p=probabilities, size=output_size, replace=True)
        output_size = yield Series(fake_sample, name=column_values.name)


def get_fake_data_generator_for_int_column(column_values: Series):
    output_size = yield
    column_values_without_null = column_values.dropna()
    if column_values_without_null.nunique() == 1:
        single_int_value = column_values_without_null.iloc[0]
        while True:
            fake_sample = [int(random.uniform(single_int_value * 0.5, single_int_value * 1.5)) for _ in range(output_size)]
            output_size = yield Series(fake_sample, name=column_values_without_null.name)
    else:
        kde = gaussian_kde(column_values_without_null.values)
        x = np.linspace(min(column_values_without_null), max(column_values_without_null), num=1000)
        pdf = kde.evaluate(x)
        probabilities = pdf/sum(pdf)
        while True:
            fake_sample = map(lambda value: int(value), np.random.choice(a=x, size=output_size, p=probabilities, replace=True))
            output_size = yield Series(fake_sample, name=column_values_without_null.name)


def get_fake_data_generator_for_decimal_column(column_values: Series):
    output_size = yield
    column_values_without_null = column_values.dropna()
    if column_values_without_null.nunique() == 1:
        while True:
            fake_sample = [Decimal(0) for _ in range(output_size)]
            output_size = yield Series(fake_sample, name=column_values_without_null.name)
    else:
        kde = gaussian_kde(column_values_without_null.values)
        x = np.linspace(min(column_values_without_null), max(column_values_without_null), num=1000)
        pdf = kde.evaluate(x)
        precisions = [len(str(float_number).split('.')[-1]) for float_number in column_values_without_null]
        probabilities = pdf/sum(pdf)
        while True:
            fake_sample = map(lambda value: Decimal(str(round(value, random.choice(precisions)))), np.random.choice(a=x, size=output_size, p=probabilities, replace=True))
            output_size = yield Series(fake_sample, name=column_values_without_null.name)


def get_fake_data_generator_for_date_column(column_values: Series):
    output_size = yield
    start_date = column_values.min()
    end_date = column_values.max()
    range_in_days = (end_date - start_date).days
    while True:
        fake_dates = [start_date + timedelta(days=randint(0, range_in_days)) for _ in range(output_size)]
        output_size = yield Series(fake_dates, name=column_values.name)


def get_fake_data_generator_for_timestamp_column(column_values: Series):
    output_size = yield
    start_timestamp = column_values.min()
    end_timestamp = column_values.max()
    range_in_sec = (end_timestamp - start_timestamp).total_seconds()
    while True:
        fake_timestamps = [(start_timestamp + timedelta(seconds=uniform(0, range_in_sec))).replace(microsecond=0) for _ in range(output_size)]
        output_size = yield Series(fake_timestamps, name=column_values.name)


def get_fake_data_generator_for_string_column(column_values: Series,
                                              regex: str = None,
                                              faker_function: Callable = None):
    output_size = yield
    if faker_function is None:
        common_regex = regex or get_common_regex(column_values.dropna())
        while True:
            list_of_fake_strings = [xeger(common_regex) for _ in range(output_size)]
            output_size = yield Series(list_of_fake_strings, name=column_values.name)
    else:
        while True:
            list_of_fake_strings = [faker_function() for _ in range(output_size)]
            output_size = yield Series(list_of_fake_strings, name=column_values.name)


def get_fake_data_generator_for_column(column_values: Series,
                                       column_info: ColumnInfo):
    if column_values.isnull().all():
        return get_generator_for_nulls(column_info.get_column_name())

    categorical_column_flag = (column_info.get_categorical_flag() or (column_values.nunique() / column_values.count() < 0.5)) and 'decimal' not in column_info.get_data_type()
    id_flag = column_info.get_id_flag()
    column_data_type = column_info.get_data_type()

    if categorical_column_flag:
        logger.info(f'Column "{column_values.name}" — CATEGORICAL COLUMN')
        return get_fake_data_generator_for_categorical_column(column_values=column_values)

    else:
        if 'decimal' in column_data_type:
            logger.info(f'Column "{column_values.name}" — DECIMAL COLUMN')
            return get_fake_data_generator_for_decimal_column(column_values=column_values)

        elif 'int' in column_data_type:
            logger.info(f'Column "{column_values.name}" — INT COLUMN')
            return get_fake_data_generator_for_int_column(column_values=column_values)

        elif column_data_type == 'timestamp':
            logger.info(f'Column "{column_values.name}" — TIMESTAMP COLUMN')
            return get_fake_data_generator_for_timestamp_column(column_values=column_values)

        elif column_data_type == 'date':
            logger.info(f'Column "{column_values.name}" — DATE COLUMN')
            return get_fake_data_generator_for_date_column(column_values=column_values)

        elif column_data_type == 'string':
            logger.info(f'Column "{column_values.name}" — STRING NON CATEGORICAL COLUMN')
            return get_fake_data_generator_for_string_column(column_values=column_values,
                                                             regex=column_info.get_regex(),
                                                             faker_function=column_info.get_faker_function())


def get_fake_data_for_insertion(output_size: int,
                                columns_info: list[ColumnInfo]) -> DataFrame:
    list_of_fake_column_data = []
    for index, column_info in enumerate(columns_info):
        logger.info(f'{index + 1}) Start generating fake data for "{column_info.get_column_name()}" column.')
        fake_column_data_in_series = column_info.get_generator().send(output_size)
        list_of_fake_column_data.append(fake_column_data_in_series)
        logger.info(f'{index + 1}) Fake data for {column_info.get_column_name()} was generated.')
    df_to_insert = concat(list_of_fake_column_data, axis=1)
    logger.info(f'-----------Fake data for all columns was generated-----------')
    return df_to_insert
