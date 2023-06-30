from fake_data_generator.columns_generator.get_common_regex import get_common_regex


def test_general():
    assert get_common_regex(['123', '32314', '131']) == '[0-9][0-9][0-9][0-9][0-9]'
    assert get_common_regex(['abc', 'a0c', 'xyz']) == '[a-z][a-z0-9][a-z]'
    assert get_common_regex(['1234-2314', '1241-1234', '2514-2141']) == '[0-9][0-9][0-9][0-9][-][0-9][0-9][0-9][0-9]'
