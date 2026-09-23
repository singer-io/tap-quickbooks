import os
import json

from singer import metadata
from singer.catalog import Catalog
import singer
from tap_quickbooks.client import QuickbooksForbiddenError
from .streams import STANDARD_STREAMS, BATCH_STREAMS

LOGGER = singer.get_logger()
STREAMS = STANDARD_STREAMS | BATCH_STREAMS


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
        with open(path, encoding='utf-8') as file:
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
        with open(os.path.join(shared_schemas_path, shared_file), encoding='utf-8') as data_file:
            shared_schema_refs['shared/' + shared_file] = json.load(data_file)

    return shared_schema_refs


def _apply_access_checks(client, schemas, field_metadata):
    """
    Probe each stream for read access and remove inaccessible streams.
    """
    inaccessible_streams = []
    for stream_name, stream_obj in STREAMS.items():
        if stream_name not in schemas:
            continue

        stream = stream_obj(client=client, config=client.config, state={})
        if not stream.check_access():
            inaccessible_streams.append(stream_name)

    for stream_name in inaccessible_streams:
        schemas.pop(stream_name, None)
        field_metadata.pop(stream_name, None)

    if not schemas:
        raise QuickbooksForbiddenError(
            "HTTP-error-code: 403, Error: The credentials do not have 'read' access to any supported streams."
        )
    if inaccessible_streams:
        LOGGER.warning(
            "No 'read' access to stream(s): %s. Excluded from catalog.",
            ", ".join(sorted(inaccessible_streams)),
        )


def do_discover(client, check_access=True):
    """
    Build and return catalog entries for streams the credentials can read.

    When `check_access` is False, the per-stream access probes are skipped so
    that building an implicit catalog for a plain sync (no --catalog/--discover)
    does not pay the cost of an extra network call per stream.
    """
    raw_schemas = _load_schemas()
    field_metadata = {}
    catalog_entries = []

    for stream_name, stream in STREAMS.items():
        if stream_name not in raw_schemas:
            continue
        schema = raw_schemas[stream_name]
        mdata = metadata.to_map(
            metadata.get_standard_metadata(
                schema=schema,
                key_properties=stream.key_properties,
                valid_replication_keys=stream.replication_keys,
                replication_method=stream.replication_method,
            )
        )
        mdata = metadata.write(mdata, ('properties', stream.replication_keys[0]), 'inclusion', 'automatic')
        field_metadata[stream_name] = mdata

    if check_access:
        _apply_access_checks(client, raw_schemas, field_metadata)
    refs = _load_shared_schema_refs()

    for stream_name, stream in STREAMS.items():
        if stream_name not in raw_schemas:
            continue
        # create and add catalog entry
        schema = raw_schemas[stream_name]
        mdata = field_metadata[stream_name]
        catalog_entry = {
            "stream": stream_name,
            "tap_stream_id": stream_name,
            "schema": singer.resolve_schema_references(schema, refs),
            "metadata": metadata.to_list(mdata),
            "key_properties": stream.key_properties
        }
        catalog_entries.append(catalog_entry)

    return Catalog.from_dict({"streams": catalog_entries})
