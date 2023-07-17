import pandas as pd
from pandas import concat, Series
# Example Series with null values
column_values = pd.Series([12.1, 2.21, None, 4.31, None], name='xxx')

x = concat([column_values.dropna().astype(float), Series([None] * 2, dtype=float)])
print(x)