import os
import json

from singer import metadata
from singer.catalog import Catalog
from .streams import STREAM_OBJECTS
import singer

LOGGER = singer.get_logger()


def _get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


# Load schemas from schemas folder
def _load_schemas():
    schemas = {}

    for filename in os.listdir(_get_abs_path("schemas")):
        path = _get_abs_path("schemas") + "/" + filename
        file_raw = filename.replace(".json", "")
        with open(path) as file:
            try:
                schemas[file_raw] = json.load(file)
            except:
                LOGGER.info('Failed to load file {}'.format(file_raw))
                raise

    return schemas


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
        ))
        # Set the replication_key MetaData to automatic as well
        mdata = metadata.write(mdata, ('properties', stream.replication_keys[0]), 'inclusion', 'automatic')
        custom_field = singer.utils.load_json(
            os.path.normpath(
                os.path.join(_get_abs_path("schemas/custom_field.json"))))
        refs = {"custom_field.json": custom_field}
        ref_schema = singer.utils.load_json(
            os.path.normpath(
                os.path.join(_get_abs_path("schemas/ref_schema.json"))))
        refs.update({"ref_schema.json": ref_schema})
        catalog_entry = {
            "stream": stream_name,
            "tap_stream_id": stream_name,
            "schema": singer.resolve_schema_references(schema, refs),
            "metadata": metadata.to_list(mdata),
            "key_properties": stream.key_properties
        }
        catalog_entries.append(catalog_entry)

    return Catalog.from_dict({"streams": catalog_entries})
