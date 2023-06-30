from fake_data_generator import CsvColumnInfo, generate_fake_csv_file


columns_info = [
    CsvColumnInfo('aircraft_code', id_flag=True),
    CsvColumnInfo('model', categorical_flag=True),
]

generate_fake_csv_file('./bookings/aircrafts_data.csv',
                       './fake_bookings/demo2_fake_aircrafts_data.csv',
                       output_size=15,
                       number_of_rows_from_which_to_create_pattern=100,
                       columns_info=columns_info,
                       encoding='windows-1251')
