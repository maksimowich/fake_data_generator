from fake_data_generator import generate_fake_table, ColumnInfo, ForeignKey
from sqlalchemy import create_engine

engine = create_engine('postgresql+psycopg2://postgres:5555@db2.mpkazantsev.ru/demo')

columns_info = [
    ColumnInfo(column_name='book_ref',
               fk=ForeignKey(ref_table_name_with_schema_or_file_path='bookings.bookings',
                             ref_column_name='book_ref'))
]

generate_fake_table(engine=engine,
                    source_table_name_with_schema='adm.tickets',
                    dest_table_name_with_schema='adm.fake_tickets',
                    output_size=5,
                    number_of_rows_from_which_to_create_pattern=1000,
                    columns_info=columns_info)

