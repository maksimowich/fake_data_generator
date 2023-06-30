from fake_data_generator import generate_fake_table, ColumnInfo
from sqlalchemy import create_engine


engine = create_engine('postgresql+psycopg2://postgres:5555@db2.mpkazantsev.ru/demo')

columns_info = [
    ColumnInfo(column_name='aircraft_code', id_flag=True),
    ColumnInfo(column_name='model', json_info=[ColumnInfo(column_name='en', categorical_flag=True)]),
]

generate_fake_table(engine=engine,
                    source_table_name_with_schema='bookings.aircrafts_data',
                    dest_table_name_with_schema='adm.demo1_1_aircrafts_data',
                    output_size=10,
                    number_of_rows_from_which_to_create_pattern=1000,
                    columns_info=columns_info,
                    columns_to_include=['aircraft_code', 'model', 'range'])


generate_fake_table(engine=engine,
                    source_table_name_with_schema='bookings.aircrafts_data',
                    dest_table_name_with_schema='adm.demo1_2_aircrafts_data',
                    output_size=20,
                    number_of_rows_from_which_to_create_pattern=1000,
                    columns_info=columns_info)

ColumnInfo(column_name='column_1', categorical_flag=True)

ColumnInfo(column_name='column_1', id_flag=True)

ColumnInfo(column_name='column_2', )