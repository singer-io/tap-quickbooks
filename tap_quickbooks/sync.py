import singer
from singer import Transformer, metadata

from .streams import STREAM_OBJECTS

LOGGER = singer.get_logger()

def do_sync(client, config, state, catalog):
    selected_streams = catalog.get_selected_streams(state)

    for stream in selected_streams:
        stream_id = stream.tap_stream_id
        stream_schema = stream.schema
        stream_object = STREAM_OBJECTS.get(stream_id)(client, config, state)

        if stream_object is None:
            raise Exception("Attempted to sync unknown stream {}".format(stream_id))

        singer.write_schema(
            stream_id,
            stream_schema.to_dict(),
            stream_object.key_properties,
            stream_object.replication_keys,
        )

        LOGGER.info("Syncing stream: %s", stream_id)

        with Transformer() as transformer:
            for rec in stream_object.sync():
                singer.write_record(
                    stream_id,
                    transformer.transform(rec,
                                          stream.schema.to_dict(),
                                          metadata.to_map(stream.metadata)))
