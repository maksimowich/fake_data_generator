from fake_data_generator import ColumnInfo


class Entity:
    def __init__(self,
                 source_entity_name: str,
                 dest_entity_name: str,
                 output_size: int,
                 columns_info: [ColumnInfo] = None,
                 number_of_rows_from_which_to_create_pattern: int = None,
                 columns_to_include: list[str] = None,
                 file_encoding: str = None):
        self.source_entity_name = source_entity_name
        self.dest_entity_name = dest_entity_name
        self.output_size = output_size
        self.number_of_rows_from_which_to_create_pattern = number_of_rows_from_which_to_create_pattern
        self.columns_to_include = columns_to_include
        if columns_info is None:
            self.columns_info = []
        else:
            self.columns_info = columns_info
        self.file_encoding = file_encoding

    def get_source_entity_name(self):
        return self.source_entity_name

    def get_dest_entity_name(self):
        return self.dest_entity_name

    def get_output_size(self):
        return self.output_size

    def get_number_of_rows_from_which_to_create_pattern(self):
        return self.number_of_rows_from_which_to_create_pattern

    def get_columns_info(self):
        return self.columns_info

    def get_columns_to_include(self):
        return self.columns_to_include

    def get_file_encoding(self):
        if self.file_encoding is None:
            return 'utf-8'
        else:
            return self.file_encoding
