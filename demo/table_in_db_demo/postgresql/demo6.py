from fake_data_generator import generate_fake_table
from sqlalchemy import create_engine


engine = create_engine('postgresql+psycopg2://postgres:5555@db2.mpkazantsev.ru/demo')

generate_fake_table(engine=engine,
                    source_table_name_with_schema='adm.dates_and_timestamps',
                    dest_table_name_with_schema='adm.demo6_dates_and_timestamps',
                    output_size=10,
                    number_of_rows_from_which_to_create_pattern=1000)
