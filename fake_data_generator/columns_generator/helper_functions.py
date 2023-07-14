import re
from typing import Iterable


def get_common_regex(strings: Iterable[str]) -> str:
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
