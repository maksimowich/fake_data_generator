from datetime import datetime
from fake_data_generator.columns_generator.column import Column, StringColumn
from fake_data_generator.columns_generator.get_generator_for_columns import \
    get_generator_for_nulls, get_fake_data_generator_for_categorical_column, get_fake_data_generator_for_int_column, \
    get_fake_data_generator_for_decimal_column, get_fake_data_generator_for_date_column, \
    get_fake_data_generator_for_timestamp_column, get_fake_data_generator_for_string_column


def get_columns_info_with_set_generators(rich_columns_info_dict):
    columns_info_with_set_generators = []
    for column_name, column_info_dict in rich_columns_info_dict.items():
        column_type = column_info_dict['type']
        column_data_type = column_info_dict['data_type']
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
            if column_info_dict.get('string_copy_of') is not None:
                column_info = StringColumn(column_name=column_name, data_type=column_data_type, string_copy_of=column_info_dict.get('string_copy_of'))
                columns_info_with_set_generators.append(column_info)
                continue
            generator = get_fake_data_generator_for_string_column(column_name, column_info_dict.get('common_regex'))
        elif column_type == 'date':
            start_date = datetime.strptime(column_info_dict['start_date'], "%Y-%m-%d").date()
            generator = get_fake_data_generator_for_date_column(column_name, start_date, column_info_dict['range_in_days'])
        elif column_type == 'timestamp':
            start_timestamp = datetime.strptime(column_info_dict['start_timestamp'], "%Y-%m-%d %H:%M:%S")
            generator = get_fake_data_generator_for_timestamp_column(column_name, start_timestamp, column_info_dict['range_in_sec'],
                                                                     column_info_dict['date_flag'], column_info_dict['current_dttm_flag'])
        column_info = Column(column_name=column_name, data_type=column_data_type)
        column_info.set_generator(generator)
        columns_info_with_set_generators.append(column_info)
    return columns_info_with_set_generators
