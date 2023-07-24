import re
from loguru import logger
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
            if not isinstance(column_info, StringColumn):
                column_info = StringColumn(column_name=column_name, data_type=column_data_type)
            if column_info.get_common_regex() is None:
                common_regex = get_common_regex(column_values.dropna())
                column_info.set_common_regex(common_regex)
            generator = get_fake_data_generator_for_string_column(column_name,
                                                                  column_info.get_common_regex())

    column_info.set_generator(generator)
    return column_info
