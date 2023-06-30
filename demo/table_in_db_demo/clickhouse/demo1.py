from fake_data_generator import generate_fake_table, ColumnInfo
from sqlalchemy import create_engine


engine = create_engine('clickhouse://stager:stager@db2.mpkazantsev.ru')

columns_info = [
    ColumnInfo(column_name='aircraft_code', id_flag=True),
]

generate_fake_table(engine=engine,
                    source_table_name_with_schema='maindb.adm_aircrafts_data',
                    dest_table_name_with_schema='maindb.demo1_1_aircrafts_data',
                    output_size=10,
                    number_of_rows_from_which_to_create_pattern=1000,
                    columns_info=columns_info,
                    columns_to_include=['aircraft_code', 'range'],
                    order_columns=['aircraft_code'])

generate_fake_table(engine=engine,
                    source_table_name_with_schema='maindb.adm_aircrafts_data',
                    dest_table_name_with_schema='maindb.demo1_2_aircrafts_data',
                    output_size=20,
                    number_of_rows_from_which_to_create_pattern=1000,
                    columns_info=columns_info,
                    order_columns=['aircraft_code'])
