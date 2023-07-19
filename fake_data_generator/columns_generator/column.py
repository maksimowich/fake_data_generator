class Column:
    def __init__(self,
                 column_name: str = None,
                 data_type: str = None,
                 generator=None):
        self.column_name = column_name
        self.data_type = data_type
        self.generator = generator

    def get_as_dict(self):
        return {self.column_name: {'data_type': self.data_type}}

    def set_generator(self, generator):
        next(generator)
        self.generator = generator

    def get_generator(self):
        return self.generator

    def set_data_type(self, data_type):
        self.data_type = data_type

    def get_data_type(self):
        return self.data_type

    def get_column_name(self):
        return self.column_name


class CategoricalColumn(Column):
    def __init__(self,
                 column_name: str = None,
                 data_type: str = None,
                 generator=None,
                 values=None,
                 probabilities=None):
        super().__init__(column_name, data_type, generator)
        self.values = values
        self.probabilities = probabilities

    def get_as_dict(self):
        super_dict = super().get_as_dict()
        super_dict[self.column_name].update({
            'type': 'categorical',
            'values': self.values,
            'probabilities': self.probabilities
        })
        return super_dict

    def set_values(self, values):
        self.values = values

    def get_values(self):
        return self.values

    def set_probabilities(self, probabilities):
        self.probabilities = probabilities

    def get_probabilities(self):
        return self.probabilities


class StringColumn(Column):
    def __init__(self,
                 column_name: str = None,
                 data_type: str = None,
                 generator=None,
                 common_regex=None):
        super().__init__(column_name, data_type, generator)
        self.common_regex = common_regex

    def get_as_dict(self):
        super_dict = super().get_as_dict()
        super_dict[self.column_name].update({
            'type': 'string',
            'common_regex': self.common_regex,
        })
        return super_dict

    def set_common_regex(self, common_regex):
        self.common_regex = common_regex

    def get_common_regex(self):
        return self.common_regex


class DateColumn(Column):
    def __init__(self,
                 column_name: str = None,
                 data_type: str = None,
                 generator=None,
                 start_date=None,
                 range_in_days=None):
        super().__init__(column_name, data_type, generator)
        self.start_date = start_date
        self.range_in_days = range_in_days

    def get_as_dict(self):
        super_dict = super().get_as_dict()
        super_dict[self.column_name].update({
            'type': 'date',
            'start_date': self.start_date,
            'range_in_days': self.range_in_days,
        })
        return super_dict

    def set_start_date(self, start_date):
        self.start_date = start_date

    def get_start_date(self):
        return self.start_date

    def set_range_in_days(self, range_in_days):
        self.range_in_days = range_in_days

    def get_range_in_days(self):
        return self.range_in_days


class TimestampColumn(Column):
    def __init__(self,
                 column_name: str = None,
                 data_type: str = None,
                 generator=None,
                 start_timestamp=None,
                 range_in_sec=None):
        super().__init__(column_name, data_type, generator)
        self.start_timestamp = start_timestamp
        self.range_in_sec = range_in_sec

    def get_as_dict(self):
        super_dict = super().get_as_dict()
        super_dict[self.column_name].update({
            'type': 'timestamp',
            'start_timestamp': self.start_timestamp,
            'range_in_sec': self.range_in_sec,
        })
        return super_dict

    def set_start_timestamp(self, start_timestamp):
        self.start_timestamp = start_timestamp

    def get_start_timestamp(self):
        return self.start_timestamp

    def set_range_in_sec(self, range_in_sec):
        self.range_in_sec = range_in_sec

    def get_range_in_sec(self):
        return self.range_in_sec


class IntColumn(Column):
    def __init__(self,
                 column_name: str = None,
                 data_type: str = None,
                 generator=None,
                 x=None,
                 probabilities=None):
        super().__init__(column_name, data_type, generator)
        self.x = x
        self.probabilities = probabilities

    def get_as_dict(self):
        super_dict = super().get_as_dict()
        super_dict[self.column_name].update({
            'type': 'int',
            'x': self.x,
            'probabilities': self.probabilities,
        })
        return super_dict

    def set_x(self, x):
        self.x = x

    def get_x(self):
        return self.x

    def set_probabilities(self, probabilities):
        self.probabilities = probabilities

    def get_probabilities(self):
        return self.probabilities


class DecimalColumn(Column):
    def __init__(self,
                 column_name: str = None,
                 data_type: str = None,
                 generator=None,
                 x=None,
                 probabilities=None,
                 precision=None):
        super().__init__(column_name, data_type, generator)
        self.x = x
        self.probabilities = probabilities
        self.precision = precision

    def get_as_dict(self):
        super_dict = super().get_as_dict()
        super_dict[self.column_name].update({
            'type': 'decimal',
            'x': self.x,
            'probabilities': self.probabilities,
            'precision': self.precision,
        })
        return super_dict

    def set_x(self, x):
        self.x = x

    def get_x(self):
        return self.x

    def set_probabilities(self, probabilities):
        self.probabilities = probabilities

    def get_probabilities(self):
        return self.probabilities

    def set_precision(self, precision):
        self.precision = precision

    def get_precision(self):
        return self.precision
