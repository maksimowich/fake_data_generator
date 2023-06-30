from fake_data_generator import generate_fake_table
from sqlalchemy import create_engine


engine = create_engine('clickhouse://stager:stager@db2.mpkazantsev.ru')

generate_fake_table(engine=engine,
                    source_table_name_with_schema='maindb.adm_dates_and_timestamps',
                    dest_table_name_with_schema='maindb.demo6_dates_and_timestamps',
                    output_size=10,
                    number_of_rows_from_which_to_create_pattern=1000,
                    order_columns=['date_column'])
