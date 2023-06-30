from fake_data_generator import generate_fake_xml_files, ColumnInfo


generate_fake_xml_files(path_to_dir_with_xml_files='C:\\Users\\Alexander\\PycharmProjects\\PACKAGES\\generator\\demo\\xml_demo\\xml_files',
                        path_to_dir_where_to_write_fake_xml_files='./output_demo1',
                        elements_info=[ColumnInfo(column_name='SPMRequest/AppID', regex='[abc][abc][abc]')],
                        starting_element_xpath='/SPMRequest/requestData/XMLdata/response/SINGLE_FORMAT/LOANS')
