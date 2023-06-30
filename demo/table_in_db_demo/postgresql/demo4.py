from fake_data_generator import generate_fake_table, ColumnInfo, ForeignKey
from faker import Faker
from sqlalchemy import create_engine


engine = create_engine('postgresql+psycopg2://postgres:5555@db2.mpkazantsev.ru/demo')

faker = Faker()

columns_info = [
    ColumnInfo(column_name='ticket_no',
               id_flag=True),
    ColumnInfo(column_name='book_ref',
               fk=ForeignKey(ref_table_name_with_schema_or_file_path='bookings.bookings',
                             ref_column_name='book_ref')),
    ColumnInfo(column_name='passenger_name',
               faker_function=faker.name),
    ColumnInfo(column_name='contact_data',
               json_info=[
                        ColumnInfo(column_name='email', faker_function=faker.email),
                        ColumnInfo(column_name='phone', faker_function=faker.phone_number)
                    ]),
]

generate_fake_table(engine=engine,
                    source_table_name_with_schema='bookings.tickets',
                    dest_table_name_with_schema='adm.demo4_tickets',
                    output_size=10,
                    number_of_rows_from_which_to_create_pattern=1000,
                    columns_info=columns_info)


faker = Faker()
ColumnInfo(column_name='passenger_name',
           faker_function=faker.name)

ColumnInfo(column_name='contact_data',
           json_info=[
                    ColumnInfo(column_name='email', faker_function=faker.email),
                    ColumnInfo(column_name='phone', faker_function=faker.phone_number)
                ])