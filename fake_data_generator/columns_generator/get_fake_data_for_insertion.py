from loguru import logger
from pandas import concat, Series
from fake_data_generator.columns_generator.column import StringColumn


def get_fake_data_for_insertion(output_size,
                                columns_info_with_set_generator):
    list_of_fake_column_data = []
    column_name_to_string_copy_column_name = {column_info.get_string_copy_of(): column_info.get_column_name()
                                              for column_info in columns_info_with_set_generator
                                              if (isinstance(column_info, StringColumn) and column_info.get_string_copy_of() is not None)}
    for index, column_info in enumerate(columns_info_with_set_generator):
        logger.info(f'{index + 1}) Start generating fake data for "{column_info.get_column_name()}" column.')
        if column_info.get_column_name() in column_name_to_string_copy_column_name.values():
            continue
        fake_column_data_in_series = column_info.get_generator().send(output_size)
        if column_info.get_column_name() in column_name_to_string_copy_column_name.keys():
            list_of_fake_column_data.append(Series(data=map(str, fake_column_data_in_series),
                                                   name=column_name_to_string_copy_column_name.get(column_info.get_column_name())))
        list_of_fake_column_data.append(fake_column_data_in_series)
        logger.info(f'{index + 1}) Fake data for {column_info.get_column_name()} was generated.')
    df_to_insert = concat(list_of_fake_column_data, axis=1)
    return df_to_insert
