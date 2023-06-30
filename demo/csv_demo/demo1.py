from fake_data_generator import CsvColumnInfo, ForeignKey, generate_fake_csv_file


columns_info = [
    CsvColumnInfo('flight_id', id_flag=True),
    CsvColumnInfo('flight_no', id_flag=True),
    CsvColumnInfo('scheduled_departure', datetime_flag=True),
    CsvColumnInfo('scheduled_arrival', datetime_flag=True),
    CsvColumnInfo('actual_departure', datetime_flag=True),
    CsvColumnInfo('actual_arrival', datetime_flag=True),
    CsvColumnInfo('aircraft_code', fk=ForeignKey(ref_table_name_with_schema_or_file_path='./bookings/aircrafts_data.csv',
                                                 ref_column_name='aircraft_code',
                                                 ref_file_encoding='windows-1251'))
]

generate_fake_csv_file('./bookings/flights.csv',
                       './fake_bookings/demo1_fake_flights.csv',
                       output_size=15,
                       number_of_rows_from_which_to_create_pattern=100,
                       columns_info=columns_info)
