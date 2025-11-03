import singer
from singer import Transformer, metadata

from .streams import STREAM_OBJECTS, BATCH_STREAMS

LOGGER = singer.get_logger()


def sync_batch_streams(client, config, state, catalog, batch_streams):
    for stream in selected_batch_streams:
        # TODO: Emit schemas
        # TODO: Batch query
        # TODO: Figure out how to update state
        pass
    singer.write_state(state)

def do_sync(client, config, state, catalog):
    selected_streams = catalog.get_selected_streams(state)

    selected_batch_streams = [stream for stream in selected_streams if stream in BATCH_STREAMS]
    other_selected_streams = [stream for stream in selected_streams if stream not in BATCH_STREAMS]

    # This will sync all batch eligible streams in one go
    sync_batch_streams(client, config, state, catalog, selected_batch_streams)

    for stream in other_selected_streams:
        stream_id = stream.tap_stream_id
        stream_schema = stream.schema
        stream_object = STREAM_OBJECTS.get(stream_id)

        if stream_object is None:
            raise Exception("Attempted to sync unknown stream {}".format(stream_id))

        stream_object = stream_object(client, config, state)
        singer.write_schema(
            stream_id,
            stream_schema.to_dict(),
            stream_object.key_properties,
            stream_object.replication_keys,
        )

        LOGGER.info("Syncing stream: %s", stream_id)
        state = singer.set_currently_syncing(state, stream_id)
        singer.write_state(state)

        with Transformer() as transformer:
            for rec in stream_object.sync():
                singer.write_record(
                    stream_id,
                    transformer.transform(rec,
                                          stream.schema.to_dict(),
                                          metadata.to_map(stream.metadata)))

    state = singer.set_currently_syncing(state, None)
    singer.write_state(state)
