from fake_data_generator import Entity, ColumnInfo, ForeignKey, generate_fake_data
from sqlalchemy import create_engine


aircrafts_data__table = Entity(source_entity_name='bookings.aircrafts_data',
                               dest_entity_name='adm.demo8_aircrafts_data',
                               output_size=10,
                               columns_info=[
                                   ColumnInfo(column_name='aircraft_code', id_flag=True)
                               ])

airports_data__table = Entity(source_entity_name='bookings.airports_data',
                              dest_entity_name='adm.demo8_airports_data',
                              output_size=10,
                              columns_to_include=['airport_code'],
                              columns_info=[
                                ColumnInfo(column_name='airport_code', id_flag=True)
                              ])

flights__table = Entity(source_entity_name='bookings.flights',
                        dest_entity_name='adm.demo8_flights',
                        output_size=10,
                        columns_info=[
                            ColumnInfo(column_name='flight_id',
                                       id_flag=True),
                            ColumnInfo(column_name='flight_no',
                                       id_flag=True),
                            ColumnInfo(column_name='aircraft_code',
                                       fk=ForeignKey(ref_table_name_with_schema_or_file_path='adm.demo8_aircrafts_data',
                                                     ref_column_name='aircraft_code')),
                            ColumnInfo(column_name='departure_airport',
                                       fk=ForeignKey(ref_table_name_with_schema_or_file_path='adm.demo8_airports_data',
                                                     ref_column_name='airport_code')),
                            ColumnInfo(column_name='arrival_airport',
                                       fk=ForeignKey(ref_table_name_with_schema_or_file_path='adm.demo8_airports_data',
                                                     ref_column_name='airport_code')),
                        ])

engine = create_engine('postgresql+psycopg2://postgres:5555@db2.mpkazantsev.ru/demo')

generate_fake_data(entities=[flights__table, airports_data__table, aircrafts_data__table],
                   source_format='table_in_db',
                   engine=engine)
