import json
import re
import math
import pytz
import os
import sys
import sqlalchemy
from pyspark.sql import SparkSession
from copy import deepcopy
from datetime import timedelta, datetime
from decimal import Decimal
from loguru import logger
from numpy import linspace, random
from pandas import concat, read_sql_query, Timestamp, to_datetime, Series
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType, DecimalType
from random import randint, uniform
from rstr import xeger
from scipy.stats import gaussian_kde

logger.remove(0)
logger.add(sys.stdout, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | <cyan>{message}</cyan>")


class Column:
    def __init__(self,
                 column_name: str = None,
                 data_type: str = None,
                 generator=None):
        self.column_name = column_name
        self.data_type = data_type
        self.generator = generator

    def get_as_dict(self):
        return {self.column_name: {'data_type': self.data_type}}

    def set_generator(self, generator):
        next(generator)
        self.generator = generator

    def get_generator(self):
        return self.generator

    def set_data_type(self, data_type):
        self.data_type = data_type

    def get_data_type(self):
        return self.data_type

    def get_column_name(self):
        return self.column_name


class CategoricalColumn(Column):
    def __init__(self,
                 column_name: str = None,
                 data_type: str = None,
                 generator=None,
                 values=None,
                 probabilities=None):
        super().__init__(column_name, data_type, generator)
        self.values = values
        self.probabilities = probabilities

    def get_as_dict(self):
        super_dict = super().get_as_dict()
        if self.data_type == 'date':
            values = list(map(lambda x: x.strftime("%Y-%m-%d"), self.values))
        elif self.data_type == 'timestamp':
            values = list(map(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"), self.values))
        else:
            values = self.values
        super_dict[self.column_name].update({
            'type': 'categorical',
            'values': values,
            'probabilities': self.probabilities
        })
        return super_dict

    def set_values(self, values):
        self.values = values

    def get_values(self):
        return self.values

    def set_probabilities(self, probabilities):
        self.probabilities = probabilities

    def get_probabilities(self):
        return self.probabilities


class StringColumn(Column):
    def __init__(self,
                 column_name: str = None,
                 data_type: str = None,
                 generator=None,
                 common_regex: str = None,
                 string_copy_of: str = None):
        super().__init__(column_name, data_type, generator)
        self.common_regex = common_regex
        self.string_copy_of = string_copy_of

    def get_as_dict(self):
        super_dict = super().get_as_dict()
        super_dict[self.column_name].update({
            'type': 'string',
            'common_regex': self.common_regex,
            'string_copy_of': self.string_copy_of,
        })
        return super_dict

    def set_string_copy_of(self, string_copy_of):
        self.string_copy_of = string_copy_of

    def get_string_copy_of(self):
        return self.string_copy_of

    def set_common_regex(self, common_regex):
        self.common_regex = common_regex

    def get_common_regex(self):
        return self.common_regex


class DateColumn(Column):
    def __init__(self,
                 column_name: str = None,
                 data_type: str = None,
                 generator=None,
                 start_date=None,
                 range_in_days=None):
        super().__init__(column_name, data_type, generator)
        self.start_date = start_date
        self.range_in_days = range_in_days

    def get_as_dict(self):
        super_dict = super().get_as_dict()
        super_dict[self.column_name].update({
            'type': 'date',
            'start_date': self.start_date.strftime('%Y-%m-%d'),
            'range_in_days': self.range_in_days,
        })
        return super_dict

    def set_start_date(self, start_date):
        self.start_date = start_date

    def get_start_date(self):
        return self.start_date

    def set_range_in_days(self, range_in_days):
        self.range_in_days = range_in_days

    def get_range_in_days(self):
        return self.range_in_days


class TimestampColumn(Column):
    def __init__(self,
                 column_name: str = None,
                 data_type: str = None,
                 generator=None,
                 start_timestamp=None,
                 range_in_sec=None,
                 date_flag=False,
                 current_dttm_flag=False):
        super().__init__(column_name, data_type, generator)
        self.start_timestamp = start_timestamp
        self.range_in_sec = range_in_sec
        self.date_flag = date_flag
        self.current_dttm_flag = current_dttm_flag

    def get_current_dttm_flag(self):
        return self.current_dttm_flag

    def get_date_flag(self):
        return self.date_flag

    def get_as_dict(self):
        super_dict = super().get_as_dict()
        super_dict[self.column_name].update({
            'type': 'timestamp',
            'start_timestamp': self.start_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            'range_in_sec': self.range_in_sec,
            'date_flag': self.date_flag,
            'current_dttm_flag': self.current_dttm_flag,
        })
        return super_dict

    def set_start_timestamp(self, start_timestamp):
        self.start_timestamp = start_timestamp

    def get_start_timestamp(self):
        return self.start_timestamp

    def set_range_in_sec(self, range_in_sec):
        self.range_in_sec = range_in_sec

    def get_range_in_sec(self):
        return self.range_in_sec


class IntColumn(Column):
    def __init__(self,
                 column_name: str = None,
                 data_type: str = None,
                 generator=None,
                 x=None,
                 probabilities=None):
        super().__init__(column_name, data_type, generator)
        self.x = x
        self.probabilities = probabilities

    def get_as_dict(self):
        super_dict = super().get_as_dict()
        super_dict[self.column_name].update({
            'type': 'int',
            'x': self.x,
            'probabilities': self.probabilities,
        })
        return super_dict

    def set_x(self, x):
        self.x = x

    def get_x(self):
        return self.x

    def set_probabilities(self, probabilities):
        self.probabilities = probabilities

    def get_probabilities(self):
        return self.probabilities


class DecimalColumn(Column):
    def __init__(self,
                 column_name: str = None,
                 data_type: str = None,
                 generator=None,
                 x=None,
                 probabilities=None,
                 precision=None):
        super().__init__(column_name, data_type, generator)
        self.x = x
        self.probabilities = probabilities
        self.precision = precision

    def get_as_dict(self):
        super_dict = super().get_as_dict()
        super_dict[self.column_name].update({
            'type': 'decimal',
            'x': self.x,
            'probabilities': self.probabilities,
            'precision': self.precision,
        })
        return super_dict

    def set_x(self, x):
        self.x = x

    def get_x(self):
        return self.x

    def set_probabilities(self, probabilities):
        self.probabilities = probabilities

    def get_probabilities(self):
        return self.probabilities

    def set_precision(self, precision: int):
        self.precision = precision

    def get_precision(self):
        return self.precision


def get_info_for_categorical_column(column_values):
    normalized_frequencies_of_values = column_values.value_counts(normalize=True, dropna=False)
    values = normalized_frequencies_of_values.index.tolist()
    if any(isinstance(value, Timestamp) for value in values):
        values = list(map(lambda x: x.to_pydatetime() if isinstance(x, Timestamp) else x, values))
    elif any(isinstance(value, float) for value in values):
        values = list(map(lambda x: int(x) if not math.isnan(x) else None, values))
    probabilities = normalized_frequencies_of_values.to_list()
    return values, probabilities


def get_info_for_number_column(column_values):
    column_values_without_null = column_values.dropna().astype(float)
    kde = gaussian_kde(column_values_without_null.values)
    x = linspace(min(column_values_without_null), max(column_values_without_null), num=100)
    pdf = kde.evaluate(x)
    probabilities = pdf / sum(pdf)
    return x.tolist(), probabilities.tolist()


def get_info_for_date_column(column_values):
    column_values_without_nulls = column_values.dropna()
    start_date = column_values_without_nulls.min()
    end_date = column_values_without_nulls.max()
    range_in_days = (end_date - start_date).days
    return start_date, range_in_days


def get_info_for_timestamp_column(column_values):
    column_values_without_nulls = column_values.dropna()
    start_timestamp = column_values_without_nulls.min()
    end_timestamp = column_values_without_nulls.max()
    range_in_sec = (end_timestamp - start_timestamp).total_seconds()
    return start_timestamp, range_in_sec


def get_common_regex(strings) -> str:
    common_pattern = {}
    for string in strings:
        for index, char in enumerate(string):
            if re.match(r"[0-9]", char):
                if '0-9' not in common_pattern.get(index, ''):
                    common_pattern[index] = common_pattern.get(index, '') + '0-9'
                else:
                    continue
            elif re.match(r"[A-Z]", char):
                if 'A-Z' not in common_pattern.get(index, ''):
                    common_pattern[index] = common_pattern.get(index, '') + 'A-Z'
                else:
                    continue
            elif re.match(r"[a-z]", char):
                if 'a-z' not in common_pattern.get(index, ''):
                    common_pattern[index] = common_pattern.get(index, '') + 'a-z'
                else:
                    continue
            elif re.match(r"[А-Я]", char):
                if 'А-Я' not in common_pattern.get(index, ''):
                    common_pattern[index] = common_pattern.get(index, '') + 'А-Я'
                else:
                    continue
            elif re.match(r"[а-я]", char):
                if 'а-я' not in common_pattern.get(index, ''):
                    common_pattern[index] = common_pattern.get(index, '') + 'а-я'
                else:
                    continue
            else:
                if char not in common_pattern.get(index, ''):
                    common_pattern[index] = common_pattern.get(index, '') + char
    common_pattern_string = ''
    for _, symbol_regex in sorted(common_pattern.items()):
        common_pattern_string += '[' + symbol_regex + ']'
    return common_pattern_string


def get_generator_for_nulls(column_name):
    output_size = yield
    while True:
        output_size = yield Series([None] * output_size, name=column_name)


def get_fake_data_generator_for_categorical_column(column_name, values, probabilities):
    output_size = yield
    while True:
        fake_sample = random.choice(a=values, p=probabilities, size=output_size, replace=True)
        fake_series = Series(fake_sample, name=column_name, dtype=object)
        output_size = yield fake_series.where(fake_series.notna(), None)


def get_fake_data_generator_for_int_column(column_name, x, probabilities):
    output_size = yield
    while True:
        fake_sample = map(lambda value: int(value), random.choice(a=x, size=output_size, p=probabilities, replace=True))
        output_size = yield Series(fake_sample, name=column_name)


def get_fake_data_generator_for_decimal_column(column_name, x, probabilities, precision: int):
    output_size = yield
    while True:
        fake_sample = map(lambda value: Decimal(str(round(value, precision))),
                          random.choice(a=x, size=output_size, p=probabilities, replace=True))
        output_size = yield Series(fake_sample, name=column_name)


def get_fake_data_generator_for_date_column(column_name, start_date, range_in_days):
    output_size = yield
    while True:
        fake_dates = [start_date + timedelta(days=randint(0, range_in_days)) for _ in range(output_size)]
        output_size = yield Series(fake_dates, name=column_name, dtype=object)


def get_fake_data_generator_for_timestamp_column(column_name, start_timestamp, range_in_sec, date_flag,
                                                 current_dttm_flag):
    output_size = yield
    while True:
        if not current_dttm_flag:
            if not date_flag:
                fake_timestamps = [(start_timestamp + timedelta(seconds=uniform(0, range_in_sec))) for _ in
                                   range(output_size)]
            else:
                fake_timestamps = [
                    (start_timestamp + timedelta(seconds=uniform(0, range_in_sec))).replace(hour=0, minute=0, second=0,
                                                                                            microsecond=0) for _ in
                    range(output_size)]
        else:
            if not date_flag:
                fake_timestamps = [datetime.now(tz=pytz.timezone('Europe/Moscow')).replace(microsecond=0) for _ in
                                   range(output_size)]
            else:
                fake_timestamps = [
                    datetime.now(tz=pytz.timezone('Europe/Moscow')).replace(hour=0, minute=0, second=0, microsecond=0)
                    for _ in range(output_size)]
        output_size = yield Series(fake_timestamps, name=column_name)


def get_fake_data_generator_for_string_column(column_name, common_regex):
    output_size = yield
    while True:
        list_of_fake_strings = [xeger(common_regex) for _ in range(output_size)]
        output_size = yield Series(list_of_fake_strings, name=column_name)


def get_rich_column_info(column_values,
                         column_info):
    column_data_type = column_info.get_data_type()
    column_name = column_info.get_column_name()

    categorical_column_flag = (isinstance(column_info, CategoricalColumn) or (
                column_values.nunique() / column_values.count() < 0.2) or column_values.nunique() in [0, 1]) and \
                              'decimal' not in column_data_type and type(column_info) in [Column, CategoricalColumn]

    generator = None
    if categorical_column_flag:
        logger.info(f'Column "{column_values.name}" — CATEGORICAL COLUMN')
        if not isinstance(column_info, CategoricalColumn):
            column_info = CategoricalColumn(column_name=column_name, data_type=column_data_type)
        if column_info.get_values() is None or column_info.get_probabilities() is None:
            values, probabilities = get_info_for_categorical_column(column_values)
            column_info.set_values(values)
            column_info.set_probabilities(probabilities)
        generator = get_fake_data_generator_for_categorical_column(column_name,
                                                                   column_info.get_values(),
                                                                   column_info.get_probabilities())

    else:
        if 'decimal' in column_data_type:
            logger.info(f'Column "{column_values.name}" — DECIMAL COLUMN')
            if not isinstance(column_info, DecimalColumn):
                column_info = DecimalColumn(column_name=column_name, data_type=column_data_type)
            if column_info.get_x() is None or column_info.get_probabilities() is None or column_info.get_precision() is None:
                if column_values.nunique() == 1:
                    column_info.set_generator(get_generator_for_nulls(column_info.get_column_name()))
                    return column_info
                x, probabilities = get_info_for_number_column(column_values)
                precision = int(re.search(r'decimal\((\d+),(\d+)\)', column_data_type).groups()[1])
                column_info.set_x(x)
                column_info.set_probabilities(probabilities)
                column_info.set_precision(precision)
            generator = get_fake_data_generator_for_decimal_column(column_name,
                                                                   column_info.get_x(),
                                                                   column_info.get_probabilities(),
                                                                   column_info.get_precision())

        elif 'int' in column_data_type:
            logger.info(f'Column "{column_values.name}" — INT COLUMN')
            if not isinstance(column_info, IntColumn):
                column_info = IntColumn(column_name=column_name, data_type=column_data_type)
            if column_info.get_x() is None or column_info.get_probabilities():
                x, probabilities = get_info_for_number_column(column_values)
                column_info.set_x(x)
                column_info.set_probabilities(probabilities)
            generator = get_fake_data_generator_for_int_column(column_name,
                                                               column_info.get_x(),
                                                               column_info.get_probabilities())

        elif column_data_type == 'timestamp':
            logger.info(f'Column "{column_values.name}" — TIMESTAMP COLUMN')
            date_flag = column_info.get_date_flag() if isinstance(column_info, TimestampColumn) else False
            current_dttm_flag = column_info.get_current_dttm_flag() if isinstance(column_info,
                                                                                  TimestampColumn) else False
            if not isinstance(column_info, TimestampColumn):
                column_info = TimestampColumn(column_name=column_name, data_type=column_data_type)
            if column_info.get_start_timestamp() is None or column_info.get_range_in_sec() is None:
                start_timestamp, range_in_sec = get_info_for_timestamp_column(column_values)
                column_info.set_start_timestamp(start_timestamp)
                column_info.set_range_in_sec(range_in_sec)
            generator = get_fake_data_generator_for_timestamp_column(column_name,
                                                                     column_info.get_start_timestamp(),
                                                                     column_info.get_range_in_sec(),
                                                                     date_flag,
                                                                     current_dttm_flag)

        elif column_data_type == 'date':
            logger.info(f'Column "{column_values.name}" — DATE COLUMN')
            if not isinstance(column_info, DateColumn):
                column_info = DateColumn(column_name=column_name, data_type=column_data_type)
            if column_info.get_start_date() is None or column_info.get_range_in_days() is None:
                start_date, range_in_days = get_info_for_date_column(column_values)
                column_info.set_start_date(start_date)
                column_info.set_range_in_days(range_in_days)
            generator = get_fake_data_generator_for_date_column(column_name,
                                                                column_info.get_start_date(),
                                                                column_info.get_range_in_days())

        elif column_data_type == 'string':
            logger.info(f'Column "{column_values.name}" — STRING NON CATEGORICAL COLUMN')
            if isinstance(column_info, StringColumn) and column_info.get_string_copy_of() is not None:
                return column_info
            if not isinstance(column_info, StringColumn):
                column_info = StringColumn(column_name=column_name, data_type=column_data_type)
            if column_info.get_common_regex() is None:
                common_regex = get_common_regex(column_values.dropna())
                column_info.set_common_regex(common_regex)
            generator = get_fake_data_generator_for_string_column(column_name,
                                                                  column_info.get_common_regex())
    column_info.set_generator(generator)
    return column_info


def get_columns_info_with_set_generators(rich_columns_info_dict):
    columns_info_with_set_generators = []
    for column_name, column_info_dict in rich_columns_info_dict.items():
        column_type = column_info_dict['type']
        column_data_type = column_info_dict['data_type']
        generator = None
        if column_type == 'categorical':
            if column_data_type == 'date':
                values = list(map(lambda x: datetime.strptime(x, "%Y-%m-%d").date() if isinstance(x, str) else x,
                                  column_info_dict['values']))
            elif column_data_type == 'timestamp':
                values = list(map(lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S") if isinstance(x, str) else x,
                                  column_info_dict['values']))
            else:
                values = column_info_dict['values']
            generator = get_fake_data_generator_for_categorical_column(column_name, values,
                                                                       column_info_dict['probabilities'])
        elif column_type == 'decimal':
            if column_info_dict['x'] is None or column_info_dict['probabilities'] is None or column_info_dict[
                'precision'] is None:
                generator = get_generator_for_nulls(column_name)
            else:
                generator = get_fake_data_generator_for_decimal_column(column_name, column_info_dict['x'],
                                                                       column_info_dict['probabilities'],
                                                                       column_info_dict['precision'])
        elif column_type == 'int':
            generator = get_fake_data_generator_for_int_column(column_name, column_info_dict['x'],
                                                               column_info_dict['probabilities'])
        elif column_type == 'string':
            if column_info_dict.get('string_copy_of') is not None:
                column_info = StringColumn(column_name=column_name, data_type=column_data_type,
                                           string_copy_of=column_info_dict.get('string_copy_of'))
                columns_info_with_set_generators.append(column_info)
                continue
            generator = get_fake_data_generator_for_string_column(column_name, column_info_dict.get('common_regex'))
        elif column_type == 'date':
            start_date = datetime.strptime(column_info_dict['start_date'], "%Y-%m-%d").date()
            generator = get_fake_data_generator_for_date_column(column_name, start_date,
                                                                column_info_dict['range_in_days'])
        elif column_type == 'timestamp':
            start_timestamp = datetime.strptime(column_info_dict['start_timestamp'], "%Y-%m-%d %H:%M:%S")
            generator = get_fake_data_generator_for_timestamp_column(column_name, start_timestamp,
                                                                     column_info_dict['range_in_sec'],
                                                                     column_info_dict['date_flag'],
                                                                     column_info_dict['current_dttm_flag'])
        column_info = Column(column_name=column_name, data_type=column_data_type)
        column_info.set_generator(generator)
        columns_info_with_set_generators.append(column_info)
    return columns_info_with_set_generators


def get_fake_data_for_insertion(output_size,
                                columns_info_with_set_generator):
    list_of_fake_column_data = []
    column_name_to_string_copy_column_name = {column_info.get_string_copy_of(): column_info.get_column_name()
                                              for column_info in columns_info_with_set_generator
                                              if (isinstance(column_info,
                                                             StringColumn) and column_info.get_string_copy_of() is not None)}
    for index, column_info in enumerate(columns_info_with_set_generator):
        logger.info(f'{index + 1}) Start generating fake data for "{column_info.get_column_name()}" column.')
        if column_info.get_column_name() in column_name_to_string_copy_column_name.values():
            continue
        fake_column_data_in_series = column_info.get_generator().send(output_size)
        if column_info.get_column_name() in column_name_to_string_copy_column_name.keys():
            list_of_fake_column_data.append(Series(data=map(str, fake_column_data_in_series),
                                                   name=column_name_to_string_copy_column_name.get(
                                                       column_info.get_column_name())))
        list_of_fake_column_data.append(fake_column_data_in_series)
        logger.info(f'{index + 1}) Fake data for {column_info.get_column_name()} was generated.')
    df_to_insert = concat(list_of_fake_column_data, axis=1)
    return df_to_insert


def get_string_for_column_names(columns_to_include):
    return ','.join(map(lambda x: '`' + x + '`', columns_to_include)) if columns_to_include is not None else '*'


def get_create_query(dest_table_name_with_schema, rich_columns_info_dict):
    str_for_column_names_and_types = ', '.join(
        [f"{column_name} {column_info_dict['data_type']}" for column_name, column_info_dict in
         rich_columns_info_dict.items()])
    return f"CREATE TABLE IF NOT EXISTS {dest_table_name_with_schema} ({str_for_column_names_and_types});"


def get_inferred_data_type(column_data_type):
    if column_data_type == 'string':
        return StringType()
    elif 'int' in column_data_type:
        return IntegerType()
    elif 'decimal' in column_data_type:
        precision, scale = re.search(r'decimal\((\d+),(\d+)\)', column_data_type).groups()
        return DecimalType(int(precision), int(scale))
    elif column_data_type == 'timestamp':
        return TimestampType()
    elif column_data_type == 'date':
        return DateType()
    else:
        return StringType()


def get_correct_column_values(column_values: Series,
                              column_data_type: str):
    number_of_nulls = column_values.isnull().sum()
    if 'int' in column_data_type:
        correct_column_values = concat([column_values.dropna().astype(int), Series([None] * number_of_nulls)])
        correct_column_values.name = column_values.name
        return correct_column_values
    elif 'decimal' in column_data_type:
        correct_column_values = concat(
            [column_values.dropna().astype(float), Series([None] * number_of_nulls, dtype=float)])
        correct_column_values.name = column_values.name
        return correct_column_values
    elif column_data_type == 'date':
        return to_datetime(column_values).dt.date
    elif column_data_type == 'timestamp':
        return column_values.apply(lambda x: x.to_pydatetime())
    else:
        return column_values


def get_rich_columns_info(conn,
                          source_table_name_with_schema: str,
                          number_of_rows_from_which_to_create_pattern: int,
                          columns_info: list[Column] = None,
                          columns_to_include: list[str] = None):
    describe_query = f"DESCRIBE {source_table_name_with_schema};"
    if isinstance(conn, sqlalchemy.engine.base.Engine):
        with conn.begin() as c:
            describe_data_in_df = read_sql_query(describe_query, c).rename(
                columns={'name': 'col_name', 'type': 'data_type'})
    else:
        describe_data_in_df = conn.sql(describe_query).toPandas().rename(
            columns={'name': 'col_name', 'type': 'data_type'})

    logger.info(f'Start making select-query from table {source_table_name_with_schema}.')
    limit_clause = f"LIMIT {number_of_rows_from_which_to_create_pattern}" if number_of_rows_from_which_to_create_pattern is not None else ''
    select_query = f"SELECT {get_string_for_column_names(columns_to_include)} " \
                   f"FROM {source_table_name_with_schema} " \
                   f"ORDER BY RANDOM() {limit_clause};"
    if isinstance(conn, sqlalchemy.engine.base.Engine):
        with conn.begin() as c:
            table_data_in_df = read_sql_query(select_query, c)
    else:
        table_data_in_df = conn.sql(select_query).toPandas()
    logger.info(f'Select-query result was read into Dataframe. Number of rows fetched is {table_data_in_df.shape[0]}.')

    column_name_to_column_info_in_dict = {column_info.get_column_name(): column_info for column_info in
                                          deepcopy(columns_info) or []}
    rich_columns_info = []
    for _, row in describe_data_in_df.iterrows():
        if columns_to_include is None or row['col_name'] in columns_to_include:
            column_info = column_name_to_column_info_in_dict.get(row['col_name'], Column(column_name=row['col_name']))
            column_info.set_data_type(row['data_type'])
            correct_column_values = get_correct_column_values(
                column_values=table_data_in_df[column_info.get_column_name()],
                column_data_type=row['data_type'])
            rich_columns_info.append(get_rich_column_info(column_values=correct_column_values,
                                                          column_info=column_info))
    return rich_columns_info


def create_table_if_not_exists(conn,
                               source_table_name_with_schema=None,
                               dest_table_name_with_schema=None,
                               columns_to_include=None,
                               create_query=None):
    if create_query is None:
        create_query = f'CREATE TABLE IF NOT EXISTS {dest_table_name_with_schema} AS ' \
                       f'SELECT {get_string_for_column_names(columns_to_include)} ' \
                       f'FROM {source_table_name_with_schema} WHERE 1<>1;'
    if isinstance(conn, sqlalchemy.engine.base.Engine):
        with conn.begin() as c:
            c.execute(create_query)
    else:
        conn.sql(create_query)


def execute_insertion(conn,
                      dest_table_name_with_schema,
                      number_of_rows_to_insert,
                      columns_info_with_set_generators,
                      batch_size):
    schema = None
    if not isinstance(conn, sqlalchemy.engine.base.Engine):
        schema = StructType(
            [StructField(column_info.get_column_name(), get_inferred_data_type(column_info.get_data_type()), True)
             for column_info in columns_info_with_set_generators])

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
                        columns_info: list = None,
                        columns_to_include: list = None,
                        batch_size=100):
    rich_columns_info = get_rich_columns_info(conn, source_table_name_with_schema,
                                              number_of_rows_from_which_to_create_pattern, columns_info,
                                              columns_to_include)
    create_table_if_not_exists(conn, source_table_name_with_schema, dest_table_name_with_schema, columns_to_include)
    execute_insertion(conn, dest_table_name_with_schema, number_of_rows_to_insert, rich_columns_info, batch_size)


def generate_table_profile(conn,
                           source_table_name_with_schema: str,
                           output_table_profile_path: str,
                           number_of_rows_from_which_to_create_pattern: int,
                           columns_info: list = None,
                           columns_to_include: list = None):
    rich_columns_info = get_rich_columns_info(conn, source_table_name_with_schema,
                                              number_of_rows_from_which_to_create_pattern, columns_info,
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
    with open(source_table_profile_path, 'r') as file:
        rich_columns_info_dict = json.load(file)

    for column_info in columns_info or []:
        rich_columns_info_dict.update(column_info.get_as_dict())

    create_table_if_not_exists(conn, dest_table_name_with_schema=dest_table_name_with_schema,
                               create_query=get_create_query(dest_table_name_with_schema, rich_columns_info_dict))
    columns_info_with_set_generators = get_columns_info_with_set_generators(rich_columns_info_dict)
    execute_insertion(conn, dest_table_name_with_schema, number_of_rows_to_insert, columns_info_with_set_generators,
                      batch_size)




spark = SparkSession \
    .builder \
    .appName("spark") \
    .config("spark.jars", "hadoop-aws-3.3.2.jar,aws-java-sdk-bundle-1.11.1026.jar") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore.hive-metastore:9083") \
    .config("spark.sql.warehouse.dir", "s3a://metastore/") \
    .config("spark.hadoop.fs.s3a.access.key", "4AOV3JSFNIGKEL2IMP6U") \
    .config("spark.hadoop.fs.s3a.secret.key", "fOaSMCzMyHFDpWXA7in9PIx56BFB3xehZ4RNJKJs") \
    .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("FEAST_S3_ENDPOINT_URL")) \
    .config("spark.sql.debug.maxToStringFields", "100") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .enableHiveSupport() \
    .getOrCreate()

spark.conf.set('spark.sql.session.timeZone', 'Europe/Moscow')

generate_table_from_profile(conn=spark,
                            source_table_profile_path='cdm.nvg_data_gr_bki_aggr_r2.json',
                            dest_table_name_with_schema='stage.nvg_data_gr_bki_aggr_r2',
                            number_of_rows_to_insert=50000,
                            columns_info=None,
                            batch_size=500)
