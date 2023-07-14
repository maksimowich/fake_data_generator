import sys
from fake_data_generator.sources_formats.table_in_db import generate_fake_table
from loguru import logger


logger.remove(0)
logger.add(sys.stdout, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | <cyan>{message}</cyan>")
