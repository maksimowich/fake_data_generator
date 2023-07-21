import re
from loguru import logger
from pandas import concat
from datetime import datetime
from fake_data_generator.columns_generator.column import \
    Column, CategoricalColumn, DecimalColumn, IntColumn, TimestampColumn, DateColumn, StringColumn
from fake_data_generator.columns_generator.get_info_for_columns import \
    get_info_for_categorical_column, get_info_for_number_column, get_info_for_date_column, get_info_for_timestamp_column, get_common_regex
from fake_data_generator.columns_generator.get_generator_for_columns import \
    get_generator_for_nulls, get_fake_data_generator_for_categorical_column, get_fake_data_generator_for_int_column, \
    get_fake_data_generator_for_decimal_column, get_fake_data_generator_for_date_column, \
    get_fake_data_generator_for_timestamp_column, get_fake_data_generator_for_string_column


def get_rich_column_info(column_values,
                         column_info):
    column_data_type = column_info.get_data_type()
    column_name = column_info.get_column_name()

    categorical_column_flag = (isinstance(column_info, CategoricalColumn) or (column_values.nunique() / column_values.count() < 0.2) or column_values.nunique() in [0, 1]) and \
        'decimal' not in column_data_type and type(column_info) in [Column, CategoricalColumn]

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
        column_info.set_generator(generator)
        return column_info

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
            column_info.set_generator(generator)
            return column_info

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
            column_info.set_generator(generator)
            return column_info

        elif column_data_type == 'timestamp':
            logger.info(f'Column "{column_values.name}" — TIMESTAMP COLUMN')
            date_flag = column_info.get_date_flag() if isinstance(column_info, TimestampColumn) else False
            current_dttm_flag = column_info.get_current_dttm_flag() if isinstance(column_info, TimestampColumn) else False
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
            column_info.set_generator(generator)
            return column_info

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
            column_info.set_generator(generator)
            return column_info

        elif column_data_type == 'string':
            logger.info(f'Column "{column_values.name}" — STRING NON CATEGORICAL COLUMN')
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
        column_info = Column(column_name=column_name, data_type=column_data_type)
        generator = None
        if column_type == 'categorical':
            if column_data_type == 'date':
                values = list(map(lambda x: datetime.strptime(x, "%Y-%m-%d").date() if isinstance(x, str) else x, column_info_dict['values']))
            elif column_data_type == 'timestamp':
                values = list(map(lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S") if isinstance(x, str) else x, column_info_dict['values']))
            else:
                values = column_info_dict['values']
            generator = get_fake_data_generator_for_categorical_column(column_name, values, column_info_dict['probabilities'])
        elif column_type == 'decimal':
            if column_info_dict['x'] is None or column_info_dict['probabilities'] is None or column_info_dict['precision'] is None:
                generator = get_generator_for_nulls(column_name)
            else:
                generator = get_fake_data_generator_for_decimal_column(column_name, column_info_dict['x'], column_info_dict['probabilities'], column_info_dict['precision'])
        elif column_type == 'int':
            generator = get_fake_data_generator_for_int_column(column_name, column_info_dict['x'], column_info_dict['probabilities'])
        elif column_type == 'string':
            generator = get_fake_data_generator_for_string_column(column_name, column_info_dict['common_regex'])
        elif column_type == 'date':
            start_date = datetime.strptime(column_info_dict['start_date'], "%Y-%m-%d").date()
            generator = get_fake_data_generator_for_date_column(column_name, start_date, column_info_dict['range_in_days'])
        elif column_type == 'timestamp':
            start_timestamp = datetime.strptime(column_info_dict['start_timestamp'], "%Y-%m-%d %H:%M:%S")
            generator = get_fake_data_generator_for_timestamp_column(column_name, start_timestamp, column_info_dict['range_in_sec'],
                                                                     column_info_dict['date_flag'], column_info_dict['current_dttm_flag'])
        column_info.set_generator(generator)
        columns_info_with_set_generators.append(column_info)
    return columns_info_with_set_generators


def get_fake_data_for_insertion(output_size,
                                columns_info_with_set_generator):
    list_of_fake_column_data = []
    for index, column_info in enumerate(columns_info_with_set_generator):
        logger.info(f'{index + 1}) Start generating fake data for "{column_info.get_column_name()}" column.')
        fake_column_data_in_series = column_info.get_generator().send(output_size)
        list_of_fake_column_data.append(fake_column_data_in_series)
        logger.info(f'{index + 1}) Fake data for {column_info.get_column_name()} was generated.')
    df_to_insert = concat(list_of_fake_column_data, axis=1)
    return df_to_insert
