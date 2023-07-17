from typing import Callable
from copy import deepcopy


class ForeignKey:
    def __init__(self,
                 ref_table_name_with_schema_or_file_path: str,
                 ref_column_name: str):
        self.ref_table_name_with_schema_or_file_path = ref_table_name_with_schema_or_file_path
        self.ref_column_name = ref_column_name

    def __str__(self):
        return self.ref_table_name_with_schema_or_file_path + ':' + self.ref_column_name

    def get_ref_table_name_with_schema_or_file_path(self):
        return self.ref_table_name_with_schema_or_file_path

    def get_ref_column_name(self):
        return self.ref_column_name


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
                 faker_function: Callable = None,
                 data_type: str = None,
                 generator=None):
        self.column_name = column_name
        self.fk = fk
        self.categorical_flag = categorical_flag
        self.id_flag = id_flag
        self.regex = regex
        self.faker_function = faker_function
        self.data_type = data_type
        self.generator = generator

    def set_generator(self, generator):
        next(generator)
        self.generator = generator

    def get_generator(self):
        return self.generator

    def get_data_type(self):
        return self.data_type

    def create_new_column_info_obj_with_set_data_type(self, data_type):
        new_obj = deepcopy(self)
        new_obj.data_type = data_type
        return new_obj

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

    def get_faker_function(self):
        return self.faker_function
