from fake_data_generator import generate_fake_table, ColumnInfo
from faker import Faker
from sqlalchemy import create_engine

engine = create_engine('postgresql+psycopg2://postgres:5555@db2.mpkazantsev.ru/demo')

faker = Faker()

columns_info = [
    ColumnInfo(column_name='airport_code', id_flag=True),
    ColumnInfo(column_name='city', json_info=[ColumnInfo(column_name='en', faker_function=faker.city)]),
]

generate_fake_table(engine=engine,
                    source_table_name_with_schema='bookings.airports_data',
                    dest_table_name_with_schema='adm.demo3_airports_data',
                    output_size=10,
                    number_of_rows_from_which_to_create_pattern=1000,
                    columns_info=columns_info,
                    columns_to_include=['airport_code', 'city'])


ColumnInfo(column_name='city',
           json_info=[ColumnInfo(column_name='en', faker_function=faker.city)]),
