import os
import json

from singer import metadata
from singer.catalog import Catalog
import singer
from .streams import STREAM_OBJECTS

LOGGER = singer.get_logger()


def _get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


# Load schemas from schemas folder
def _load_schemas():
    schemas = {}

    schema_path = _get_abs_path('schemas')
    files = [f for f in os.listdir(schema_path) if os.path.isfile(os.path.join(schema_path, f))]
    for filename in files:
        path = _get_abs_path("schemas") + "/" + filename
        file_raw = filename.replace(".json", "")
        with open(path) as file:
            try:
                schemas[file_raw] = json.load(file)
            except:
                LOGGER.info('Failed to load file %s', file_raw)
                raise

    return schemas

def _load_shared_schema_refs():
    """
        Load all the schemas from the 'shared/' folder to resolve schema refs
    """
    shared_schemas_path = _get_abs_path('schemas/shared')

    shared_file_names = [f for f in os.listdir(shared_schemas_path)
                         if os.path.isfile(os.path.join(shared_schemas_path, f))]

    shared_schema_refs = {}
    for shared_file in shared_file_names:
        with open(os.path.join(shared_schemas_path, shared_file)) as data_file:
            shared_schema_refs['shared/' + shared_file] = json.load(data_file)

    return shared_schema_refs

def do_discover():
    raw_schemas = _load_schemas()
    catalog_entries = []

    for stream_name, stream in STREAM_OBJECTS.items():
        # create and add catalog entry
        schema = raw_schemas[stream_name]

        mdata = metadata.to_map(
            metadata.get_standard_metadata(
                schema=schema,
                key_properties=stream.key_properties,
                valid_replication_keys=stream.replication_keys,
                replication_method=stream.replication_method,
            )
        )
        # Set the replication_key MetaData to automatic as well
        mdata = metadata.write(mdata, ('properties', stream.replication_keys[0]), 'inclusion', 'automatic')
        # load all refs
        refs = _load_shared_schema_refs()
        catalog_entry = {
            "stream": stream_name,
            "tap_stream_id": stream_name,
            "schema": singer.resolve_schema_references(schema, refs),
            "metadata": metadata.to_list(mdata),
            "key_properties": stream.key_properties
        }
        catalog_entries.append(catalog_entry)

    return Catalog.from_dict({"streams": catalog_entries})
