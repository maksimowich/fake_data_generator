import re
import math
from numpy import linspace
from scipy.stats import gaussian_kde
from pandas import Timestamp


def get_info_for_categorical_column(column_values):
    normalized_frequencies_of_values = column_values.value_counts(normalize=True, dropna=False)
    values = normalized_frequencies_of_values.index.tolist()
    if any(isinstance(value, Timestamp) for value in values):
        values = list(map(lambda x: x.to_pydatetime() if isinstance(x, Timestamp) else x, values))
    elif any(isinstance(value, float) for value in values):
        values = list(map(lambda x: int(x) if not math.isnan(x) else None, values))
    probabilities = normalized_frequencies_of_values.to_list()
    return values, probabilities


def get_info_for_number_column(column_values):
    column_values_without_null = column_values.dropna().astype(float)
    kde = gaussian_kde(column_values_without_null.values)
    x = linspace(min(column_values_without_null), max(column_values_without_null), num=100)
    pdf = kde.evaluate(x)
    probabilities = pdf/sum(pdf)
    return x.tolist(), probabilities.tolist()


def get_info_for_date_column(column_values):
    column_values_without_nulls = column_values.dropna()
    start_date = column_values_without_nulls.min()
    end_date = column_values_without_nulls.max()
    range_in_days = (end_date - start_date).days
    return start_date, range_in_days


def get_info_for_timestamp_column(column_values):
    column_values_without_nulls = column_values.dropna()
    start_timestamp = column_values_without_nulls.min()
    end_timestamp = column_values_without_nulls.max()
    range_in_sec = (end_timestamp - start_timestamp).total_seconds()
    return start_timestamp, range_in_sec


def get_common_regex(strings) -> str:
    """
    Function that returns common regular expression of given strings.

    Parameters
    ----------
     strings: Iterable object returning strings for which common regex should be found

    Returns
    -------
     String representing common regular expression of specified strings

    Examples
    --------
    # >>> get_common_regex(['123', '32314', '131'])
    # '[0-9][0-9][0-9][0-9][0-9]'
    # >>> get_common_regex(['abc', 'a0c', 'xyz'])
    # '[a-z][a-z0-9][a-z]'
    # >>> get_common_regex(['1234-2314', '1241-1234', '2514-2141'])
    '[0-9][0-9][0-9][0-9][-][0-9][0-9][0-9][0-9]'
    """
    common_pattern = {}
    for string in strings:
        for index, char in enumerate(string):
            if re.match(r"[0-9]", char):
                if '0-9' not in common_pattern.get(index, ''):
                    common_pattern[index] = common_pattern.get(index, '') + '0-9'
                else:
                    continue
            elif re.match(r"[A-Z]", char):
                if 'A-Z' not in common_pattern.get(index, ''):
                    common_pattern[index] = common_pattern.get(index, '') + 'A-Z'
                else:
                    continue
            elif re.match(r"[a-z]", char):
                if 'a-z' not in common_pattern.get(index, ''):
                    common_pattern[index] = common_pattern.get(index, '') + 'a-z'
                else:
                    continue
            elif re.match(r"[А-Я]", char):
                if 'А-Я' not in common_pattern.get(index, ''):
                    common_pattern[index] = common_pattern.get(index, '') + 'А-Я'
                else:
                    continue
            elif re.match(r"[а-я]", char):
                if 'а-я' not in common_pattern.get(index, ''):
                    common_pattern[index] = common_pattern.get(index, '') + 'а-я'
                else:
                    continue
            else:
                if char not in common_pattern.get(index, ''):
                    common_pattern[index] = common_pattern.get(index, '') + char
    common_pattern_string = ''
    for _, symbol_regex in sorted(common_pattern.items()):
        common_pattern_string += '[' + symbol_regex + ']'
    return common_pattern_string
