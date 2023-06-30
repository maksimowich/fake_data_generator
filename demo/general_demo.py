from rstr import xeger

from fake_data_generator.columns_generator.get_fake_data_for_columns import get_data_for_id_column
from pandas import Series

#
# fake_data = get_data_for_id_column(Series(['ABC', 'ZXB', 'GAX']), output_size=17576)
#
#
# print(fake_data.head())
# print(fake_data.shape)
for _ in range(6):
    print(xeger('[ABCzxc]'))
