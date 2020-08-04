import singer
from singer import Transformer, metadata

from .streams import STREAM_OBJECTS

LOGGER = singer.get_logger()

def do_sync(client, config, state, catalog):
    pass
