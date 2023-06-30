import pandas
import xml.etree.ElementTree as ET
from lxml import etree
import os
from fake_data_generator.columns_generator import get_fake_data_for_column, ColumnInfo


def get_list_of_correct_type(lst: list[str]):
    lst_of_correct_type = []
    for string_value in lst:
        try:
            if string_value is None:
                lst_of_correct_type.append(None)
            else:
                lst_of_correct_type.append(int(string_value))
        except (ValueError, TypeError):
            lst_of_correct_type = []
            break
    else:
        return lst_of_correct_type
    for string_value in lst:
        try:
            if string_value is None:
                lst_of_correct_type.append(None)
            else:
                lst_of_correct_type.append(float(string_value))
        except (ValueError, TypeError):
            lst_of_correct_type = []
            break
    else:
        return lst_of_correct_type
    if len(lst_of_correct_type) == 0:
        return lst
    else:
        return lst_of_correct_type


def parse_xml_files(path_to_dir_with_xml_files: str,
                    starting_element_xpath: str = None) -> dict[str: list[str]]:
    dict_element_name_to_element_values_in_string = {}

    def parse_element(element, element_xpath: str):
        element_content = element.text if not ((element.text or ' ').isspace() or element.text == '') else None
        if dict_element_name_to_element_values_in_string.get(element_xpath) is None:
            dict_element_name_to_element_values_in_string[element_xpath] = []
        dict_element_name_to_element_values_in_string.get(element_xpath).append(element_content)
        for child_element in element:
            parse_element(child_element, element_xpath + '/' + child_element.tag)

    for filename in os.listdir(path_to_dir_with_xml_files):
        if filename.endswith('.xml'):
            filepath = os.path.join(path_to_dir_with_xml_files, filename)
            tree = etree.parse(filepath)
            starting_element = tree.getroot() if starting_element_xpath is None else next(iter(tree.xpath(starting_element_xpath)))
            starting_element_xpath = '/' + starting_element.tag if starting_element_xpath is None else starting_element_xpath
            parse_element(starting_element, element_xpath=starting_element_xpath)

    return dict_element_name_to_element_values_in_string


def generate_fake_xml_files(path_to_dir_with_xml_files: str,
                            path_to_dir_where_to_write_fake_xml_files: str,
                            elements_info: list[ColumnInfo] = None,
                            starting_element_xpath: str = None):
    dict_element_name_to_element_values_in_string = parse_xml_files(path_to_dir_with_xml_files, starting_element_xpath)
    dict_element_name_to_fake_element_values = {}
    element_name_to_element_info_dict = {element_info.get_column_name(): element_info for element_info in elements_info or []}

    for element_name, element_values_in_string in dict_element_name_to_element_values_in_string.items():
        fake_data_for_element = get_fake_data_for_column(column_values=pandas.Series(get_list_of_correct_type(element_values_in_string), name=element_name),
                                                         column_info=element_name_to_element_info_dict.get(element_name),
                                                         output_size=len(element_values_in_string))
        dict_element_name_to_fake_element_values[element_name] = {'element_values': fake_data_for_element, 'count_of_inserted_values': 0}

    if not os.path.exists(path_to_dir_where_to_write_fake_xml_files):
        os.makedirs(path_to_dir_where_to_write_fake_xml_files)

    for filename in os.listdir(path_to_dir_with_xml_files):
        if filename.endswith('.xml'):
            filepath = os.path.join(path_to_dir_with_xml_files, filename)
            root = ET.parse(filepath).getroot()
            for element_name, element_fake_values_and_count in dict_element_name_to_fake_element_values.items():
                xpath_of_element = './' + element_name.split('/', 2)[-1]
                matching_elements = root.findall(xpath_of_element)
                for matching_element in matching_elements:
                    current_count_of_inserted_values = element_fake_values_and_count['count_of_inserted_values']
                    fake_element_value = element_fake_values_and_count['element_values'][current_count_of_inserted_values]
                    if fake_element_value is None:
                        fake_element_value = ''
                    matching_element.text = str(fake_element_value)
                    element_fake_values_and_count['count_of_inserted_values'] = current_count_of_inserted_values + 1
            tree = ET.ElementTree(root)
            tree.write(f'{path_to_dir_where_to_write_fake_xml_files}/{filename}')
