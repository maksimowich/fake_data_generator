import sys
from loguru import logger
from fake_data_generator.sources_formats.generate_fake_table import generate_fake_table
from fake_data_generator.sources_formats.generate_table_profile import generate_table_profile
from fake_data_generator.sources_formats.generate_table_from_profile import generate_table_from_profile


logger.remove(0)
logger.add(sys.stdout, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | <cyan>{message}</cyan>")
