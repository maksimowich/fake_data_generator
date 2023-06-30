from fake_data_generator import generate_fake_table, ColumnInfo
from sqlalchemy import create_engine
from faker import Faker


engine = create_engine('clickhouse://stager:stager@db2.mpkazantsev.ru')

faker = Faker()

columns_info = [
    ColumnInfo(column_name='airport_code', id_flag=True),
    ColumnInfo(column_name='city', json_info=[ColumnInfo(column_name='en', faker_function=faker.city)]),
]

generate_fake_table(engine=engine,
                    source_table_name_with_schema='maindb.adm_airports_data',
                    dest_table_name_with_schema='maindb.demo3_airports_data',
                    output_size=10,
                    number_of_rows_from_which_to_create_pattern=1000,
                    columns_info=columns_info,
                    columns_to_include=['airport_code', 'city'],
                    order_columns=['airport_code'])
