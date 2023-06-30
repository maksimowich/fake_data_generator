from fake_data_generator import generate_fake_table, ColumnInfo, ForeignKey
from sqlalchemy import create_engine


engine = create_engine('clickhouse://stager:stager@db2.mpkazantsev.ru')

columns_info = [
    ColumnInfo(column_name='flight_id', id_flag=True),
    ColumnInfo(column_name='flight_no', id_flag=True),
    ColumnInfo(column_name='aircraft_code',
               fk=ForeignKey(ref_table_name_with_schema_or_file_path='maindb.adm_aircrafts_data',
                             ref_column_name='aircraft_code')),
    ColumnInfo(column_name='departure_airport',
               fk=ForeignKey(ref_table_name_with_schema_or_file_path='maindb.adm_airports_data',
                             ref_column_name='airport_code')),
    ColumnInfo(column_name='arrival_airport',
               fk=ForeignKey(ref_table_name_with_schema_or_file_path='maindb.adm_airports_data',
                             ref_column_name='airport_code')),
]

generate_fake_table(engine=engine,
                    source_table_name_with_schema='maindb.adm_flights',
                    dest_table_name_with_schema='maindb.demo2_flights',
                    output_size=10,
                    number_of_rows_from_which_to_create_pattern=1000,
                    columns_info=columns_info,
                    order_columns=['flight_id'])
