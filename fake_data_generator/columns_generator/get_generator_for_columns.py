from datetime import timedelta
from decimal import Decimal
from numpy import random
from random import randint, uniform
from rstr import xeger
from pandas import Series


def get_generator_for_nulls(column_name):
    output_size = yield
    while True:
        output_size = yield Series([None] * output_size, name=column_name)


def get_fake_data_generator_for_categorical_column(column_name, values, probabilities):
    output_size = yield
    while True:
        fake_sample = random.choice(a=values, p=probabilities, size=output_size, replace=True)
        output_size = yield Series(fake_sample, name=column_name)


def get_fake_data_generator_for_int_column(column_name, x, probabilities):
    output_size = yield
    while True:
        fake_sample = map(lambda value: int(value), random.choice(a=x, size=output_size, p=probabilities, replace=True))
        output_size = yield Series(fake_sample, name=column_name)


def get_fake_data_generator_for_decimal_column(column_name, x, probabilities, precision):
    output_size = yield
    while True:
        fake_sample = map(lambda value: Decimal(str(round(value, precision))), random.choice(a=x, size=output_size, p=probabilities, replace=True))
        output_size = yield Series(fake_sample, name=column_name)


def get_fake_data_generator_for_date_column(column_name, start_date, range_in_days):
    output_size = yield
    while True:
        fake_dates = [start_date + timedelta(days=randint(0, range_in_days)) for _ in range(output_size)]
        output_size = yield Series(fake_dates, name=column_name)


def get_fake_data_generator_for_timestamp_column(column_name, start_timestamp, range_in_sec):
    output_size = yield
    while True:
        fake_timestamps = [(start_timestamp + timedelta(seconds=uniform(0, range_in_sec))).replace(microsecond=0) for _ in range(output_size)]
        output_size = yield Series(fake_timestamps, name=column_name)


def get_fake_data_generator_for_string_column(column_name, common_regex):
    output_size = yield
    while True:
        list_of_fake_strings = [xeger(common_regex) for _ in range(output_size)]
        output_size = yield Series(list_of_fake_strings, name=column_name)
