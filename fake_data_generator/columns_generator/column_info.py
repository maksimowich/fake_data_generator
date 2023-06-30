from typing import Callable


class ForeignKey:
    def __init__(self,
                 ref_table_name_with_schema_or_file_path: str,
                 ref_column_name: str,
                 ref_file_encoding: str = None):
        self.ref_table_name_with_schema_or_file_path = ref_table_name_with_schema_or_file_path
        self.ref_column_name = ref_column_name
        self.ref_file_encoding = ref_file_encoding

    def __str__(self):
        return self.ref_table_name_with_schema_or_file_path + ':' + self.ref_column_name

    def get_ref_table_name_with_schema_or_file_path(self):
        return self.ref_table_name_with_schema_or_file_path

    def get_ref_column_name(self):
        return self.ref_column_name

    def get_ref_file_encoding(self):
        return self.ref_file_encoding


class ColumnInfo:
    """
    Class needed for column description.

    Parameters
    ----------
     column_name: Column name
     fk: Foreign key
     categorical_flag: Whether to treat column as categorical or not
     id_flag: Whether to treat column as id
     regex: Common regular expression of column`s values
     json_info: If column is of json type, json_info parameter specifies information values of particular keys
                It is list of ColumnInfo objects
                Names of json keys are separated by '->' if there is nested structure
     faker_function: Function that will return random values for column
        """

    def __init__(self,
                 column_name: str = None,
                 fk: ForeignKey = None,
                 categorical_flag: bool = None,
                 id_flag: bool = None,
                 regex: str = None,
                 json_info: list = None,
                 faker_function: Callable = None):
        self.column_name = column_name
        self.fk = fk
        self.categorical_flag = categorical_flag
        self.id_flag = id_flag
        self.regex = regex
        self.json_info = json_info
        self.faker_function = faker_function

    def get_column_name(self):
        return self.column_name

    def get_fk(self):
        return self.fk

    def get_categorical_flag(self):
        return self.categorical_flag

    def get_id_flag(self):
        return self.id_flag

    def get_regex(self):
        return self.regex

    def get_json_info(self):
        return self.json_info

    def get_faker_function(self):
        return self.faker_function


class CsvColumnInfo(ColumnInfo):
    def __init__(self,
                 column_name: str = None,
                 fk: ForeignKey = None,
                 categorical_flag: bool = None,
                 id_flag: bool = None,
                 regex: str = None,
                 json_info: list = None,
                 faker_function: Callable = None,
                 json_flag: bool = False,
                 datetime_flag: bool = False):
        super().__init__(column_name, fk, categorical_flag, id_flag, regex, json_info, faker_function)
        self.json_flag = json_flag
        self.datetime_flag = datetime_flag

    def get_json_flag(self):
        return self.json_flag

    def get_datetime_flag(self):
        return self.datetime_flag
