import sys
from fake_data_generator.sources_formats.table_in_db import generate_fake_table, DatabaseName
from fake_data_generator.sources_formats.csv import generate_fake_csv_file
from fake_data_generator.sources_formats.xml import generate_fake_xml_files
from loguru import logger


logger.remove(0)
logger.add(sys.stdout, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | <cyan>{message}</cyan>")
