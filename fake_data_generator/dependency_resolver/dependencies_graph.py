import networkx as nx
from fake_data_generator.dependency_resolver.entity import Entity
from fake_data_generator.sources_formats import \
    generate_fake_table, \
    generate_fake_csv_file, \
    generate_fake_xml_files
from loguru import logger


AVAILABLE_SOURCE_FORMATS = ['table_in_db', 'csv', 'xml']


def get_dependencies_graph(entities: list[Entity]) -> nx.DiGraph:
    dependencies_graph = nx.DiGraph()

    for entity in entities:
        dependencies_graph.add_node(node_for_adding=entity, label=entity.get_dest_entity_name())

    for entity in entities:
        ref_entities_names = [column_info.get_fk().get_ref_table_name_with_schema_or_file_path() for column_info in entity.get_columns_info() if column_info.get_fk() is not None]
        for ref_entity_name in ref_entities_names:
            ref_entity = next(filter(lambda x: x.get_dest_entity_name() == ref_entity_name, entities), None)
            if ref_entity is not None:
                dependencies_graph.add_edge(ref_entity, entity)

    return dependencies_graph


def generate_fake_data(entities: list[Entity],
                       source_format: str,
                       engine=None):
    if source_format not in AVAILABLE_SOURCE_FORMATS:
        logger.error(f'Specified source format {source_format} is not supported.\nAvailable source formats are {AVAILABLE_SOURCE_FORMATS}')
        return

    dependencies_graph = get_dependencies_graph(entities)

    if source_format == 'table_in_db':
        for entity in nx.topological_sort(dependencies_graph):
            generate_fake_table(engine=engine,
                                source_table_name_with_schema=entity.get_source_entity_name(),
                                dest_table_name_with_schema=entity.get_dest_entity_name(),
                                output_size=entity.get_output_size(),
                                number_of_rows_from_which_to_create_pattern=entity.get_number_of_rows_from_which_to_create_pattern(),
                                columns_info=entity.get_columns_info(),
                                columns_to_include=entity.get_columns_to_include())
    elif source_format == 'csv':
        for entity in nx.topological_sort(dependencies_graph):
            generate_fake_csv_file(source_file_name=entity.get_source_entity_name(),
                                   dest_file_name=entity.get_dest_entity_name(),
                                   output_size=entity.get_output_size(),
                                   number_of_rows_from_which_to_create_pattern=entity.get_number_of_rows_from_which_to_create_pattern(),
                                   columns_info=entity.get_columns_info(),
                                   columns_to_include=entity.get_columns_to_include,
                                   encoding=entity.get_file_encoding())
    elif source_format == 'xml':
        logger.info('pass')
