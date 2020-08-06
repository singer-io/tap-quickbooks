import singer
from singer import utils
from singer.catalog import Catalog, write_catalog
from tap_quickbooks.discover import do_discover
from tap_quickbooks.client import QuickbooksClient
from tap_quickbooks.sync import do_sync


LOGGER = singer.get_logger()


@utils.handle_top_exception(LOGGER)
def main():
    required_config_keys = ['start_date']
    args = singer.parse_args(required_config_keys)

    config = args.config
    client = QuickbooksClient(args.config_path, config)
    catalog = args.catalog or Catalog([])
    state = args.state

    if args.properties and not args.catalog:
        raise Exception("DEPRECATED: Use of the 'properties' parameter is not supported. Please use --catalog instead")

    if args.discover:
        LOGGER.info("Starting discovery mode")
        catalog = do_discover()
        write_catalog(catalog)
    else:
        LOGGER.info("Starting sync mode")
        do_sync(client, config, state, catalog)

if __name__ == "__main__":
    main()
