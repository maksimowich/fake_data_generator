from loguru import logger
from pandas import concat


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
