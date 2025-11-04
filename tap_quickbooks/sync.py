import json
import singer
from singer import Transformer, metadata
from tap_quickbooks import query_builder

from .streams import STREAM_OBJECTS, BATCH_STREAMS

LOGGER = singer.get_logger()


def sync_batch_streams(client, config, state, batch_streams):
    stream_objects = [STREAM_OBJECTS.get(stream.tap_stream_id)(client, config, state) for stream in batch_streams]
    bookmarks = {stream.stream_name: singer.get_bookmark(state, stream.stream_name, 'LastUpdatedTime', config.get('start_date'))
                 for stream in stream_objects}
    max_results = int(config.get('max_results', '1000'))
    start_positions = {stream.stream_name: 1
                       for stream in stream_objects}

    for i, stream in enumerate(batch_streams):
        stream_object = stream_objects[i]
        stream_id = stream.tap_stream_id
        stream_schema = stream.schema

        singer.write_schema(
            stream_id,
            stream_schema.to_dict(),
            stream_object.key_properties,
            stream_object.replication_keys
        )
    LOGGER.info("Syncing batches: %s", [stream.tap_stream_id for stream in batch_streams])
    # not sure about this...
    state = singer.set_currently_syncing(state, batch_streams[0].tap_stream_id)
    singer.write_state(state)

    while True:
        query = query_builder.build_batch_query(stream_objects, bookmarks, start_positions, max_results)

        resp = client.post(f'/v3/company/{{realm_id}}/batch?minorversion={client.minor_version}',
                           data=json.dumps(query)).get('BatchItemResponse',[])
        if not [result for result in resp if result.get('QueryResponse',{}).get('maxResults')]:
            break

        for result in resp:
            keys = result.get('QueryResponse',{}).keys() - {'startPosition','maxResults'}
            if keys:
                table_name = list(keys)[0]
                stream_obj = [stream for stream in stream_objects if stream.table_name == table_name][0]
                stream = [stream for stream in batch_streams if stream.tap_stream_id == stream_obj.stream_name][0]
                stream_name = stream_obj.stream_name
                records = result.get('QueryResponse',{}).get(table_name)
                for record in records:
                    with Transformer() as transformer:
                        singer.write_record(
                            stream.tap_stream_id,
                            transformer.transform(record,
                                                  stream.schema.to_dict(),
                                                  metadata.to_map(stream.metadata)))
                if records:
                    state = singer.write_bookmark(state, stream_name, 'LastUpdatedTime', record.get('MetaData').get('LastUpdatedTime'))
                    singer.write_state(state)

                start_positions[stream_name] += len(records)

def do_sync(client, config, state, catalog):

    selected_streams = catalog.get_selected_streams(state)

    selected_batch_streams = [stream for stream in selected_streams if stream.tap_stream_id in BATCH_STREAMS]
    other_selected_streams = [stream for stream in selected_streams if stream.tap_stream_id not in BATCH_STREAMS]

    # This will sync all batch eligible streams in one go
    if selected_batch_streams:
        sync_batch_streams(client, config, state, selected_batch_streams)

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
