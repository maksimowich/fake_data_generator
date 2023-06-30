from fake_data_generator import CsvColumnInfo, generate_fake_csv_file
from faker import Faker

faker = Faker()

columns_info = [
    CsvColumnInfo('airport_code', id_flag=True),
    CsvColumnInfo('airport_name', json_flag=True, json_info=[CsvColumnInfo('en', categorical_flag=True)]),
    CsvColumnInfo('city', json_flag=True, json_info=[CsvColumnInfo('en', faker_function=faker.city)])
]

columns_to_include = ['airport_code', 'airport_name', 'city', 'timezone']

generate_fake_csv_file('./bookings/airports_data.csv',
                       './fake_bookings/demo3_fake_airports_data.csv',
                       output_size=15,
                       number_of_rows_from_which_to_create_pattern=100,
                       columns_info=columns_info,
                       encoding='windows-1251',
                       columns_to_include=columns_to_include)
