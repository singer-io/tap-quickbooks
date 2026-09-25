import singer
from singer import utils
from singer.catalog import Catalog, write_catalog
from tap_quickbooks.discover import do_discover
from tap_quickbooks.client import QuickbooksClient
from tap_quickbooks.sync import do_sync


LOGGER = singer.get_logger()


@utils.handle_top_exception(LOGGER)
def main():
    required_config_keys = ['start_date', 'user_agent', 'realm_id', 'client_id', 'client_secret', 'refresh_token']
    args = singer.parse_args(required_config_keys)

    config = args.config
    if args.dev:
        LOGGER.warning("Executing Tap in Dev mode")
    client = QuickbooksClient(args.config_path, config, args.dev)
    state = args.state

    if args.properties and not args.catalog:
        raise Exception("DEPRECATED: Use of the 'properties' parameter is not supported. Please use --catalog instead")

    if args.discover:
        LOGGER.info("Starting discovery mode")
        client.do_authorization_check()
        catalog = do_discover(client)
        write_catalog(catalog)
        LOGGER.info("Finished discovery mode")
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            # Fallback only; documented usage always syncs with a catalog from --discover.
            # Skip access probing here to avoid a network call per stream on every sync run.
            catalog = do_discover(client, check_access=False)

        LOGGER.info("Starting sync mode")
        do_sync(client, config, state, catalog)
        LOGGER.info("Finished sync mode")

if __name__ == "__main__":
    main()
