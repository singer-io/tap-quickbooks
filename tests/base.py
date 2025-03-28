import copy
import os
import unittest
import time
from datetime import datetime as dt
import pytz

import tap_tester.menagerie   as menagerie
import tap_tester.connections as connections


class TestQuickbooksBase(unittest.TestCase):
    """
    To run tests that use this base method, a connection must be manually authed first
    using the dev credentials with a connection named 'tap_tester_quickbooks_combined_test'.

    This enables the tests to grab a new refresh token. Any test runs after the inital auth will
    grab the token from the previous connection.
    """
    REPLICATION_KEYS = "valid-replication-keys"
    PRIMARY_KEYS = "table-key-properties"
    FOREIGN_KEYS = "table-foreign-key-properties"
    REPLICATION_METHOD = "forced-replication-method"
    INCREMENTAL = "INCREMENTAL"
    START_DATE_FORMAT = "%Y-%m-%dT00:00:00Z" # %H:%M:%SZ
    # list of streams which supports custom field
    custom_command_streams = ['invoices','estimates','credit_memos','refund_receipts','sales_receipts','purchase_orders']
    DATETIME_FMT = {
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.000000Z",
        "%Y-%m-%dT%H:%M:%S%z"
    }

    def name(self):
        """
            Quickbooks uses the token chaining to get the existing token which requires
            all tests to have same name So do not overwrite the test name below
        """
        return "tap_tester_quickbooks_combined_test"

    def setUp(self):
        missing_envs = [x for x in [
            'TAP_QUICKBOOKS_OAUTH_CLIENT_ID',
            'TAP_QUICKBOOKS_OAUTH_CLIENT_SECRET',
            'TAP_QUICKBOOKS_OAUTH_REFRESH_TOKEN',
            'TAP_QUICKBOOKS_REALM_ID'
        ] if os.getenv(x) is None]
        if missing_envs:
            raise Exception("Missing environment variables: {}".format(missing_envs))

    @staticmethod
    def get_type():
        return "platform.quickbooks"

    @staticmethod
    def tap_name():
        return "tap-quickbooks"

    def get_properties(self, original=True):
        if original:
            return {
                'start_date' : '2016-06-02T00:00:00Z',
                'sandbox': 'true'
            }
        else:
            return {
                'start_date' : self.start_date,
                'sandbox': 'true'
            }

    def get_credentials(self):
        return {
            # Refresh Tokens expire and a valid chain needs to be maintained
            'refresh_token': os.getenv('TAP_QUICKBOOKS_OAUTH_REFRESH_TOKEN'),
            'client_id': os.getenv('TAP_QUICKBOOKS_OAUTH_CLIENT_ID'),
            'client_secret': os.getenv('TAP_QUICKBOOKS_OAUTH_CLIENT_SECRET'),
            'realm_id': os.getenv('TAP_QUICKBOOKS_REALM_ID')
        }

    @staticmethod
    def expected_check_streams():
        return {
            "accounts",
            "bill_payments",
            "bills",
            "budgets",
            "classes",
            "credit_memos",
            "customers",
            "customer_types",
            "departments",
            "deposits",
            "employees",
            "estimates",
            "invoices",
            "items",
            "journal_entries",
            "payment_methods",
            "payments",
            "purchase_orders",
            "purchases",
            "refund_receipts",
            "sales_receipts",
            "tax_agencies",
            "tax_codes",
            "tax_rates",
            "terms",
            "time_activities",
            "transfers",
            "vendor_credits",
            "vendors",
            "profit_loss_report",
            "deleted_objects"
        }

    def expected_metadata(self):
        """The expected streams and metadata about the streams"""

        mdata = {}
        for stream in self.expected_check_streams():
            if self.is_report_stream(stream):
                mdata[stream] = {
                    self.PRIMARY_KEYS: {'ReportDate'},
                    self.REPLICATION_METHOD: self.INCREMENTAL,
                    self.REPLICATION_KEYS: {'ReportDate'},
                }
            elif stream == "deleted_objects":
                mdata[stream] = {
                    self.PRIMARY_KEYS: {'Id', 'Type'},
                    self.REPLICATION_METHOD: self.INCREMENTAL,
                    self.REPLICATION_KEYS: {'MetaData'},
                }
            else:
                mdata[stream] = {
                    self.PRIMARY_KEYS: {'Id'},
                    self.REPLICATION_METHOD: self.INCREMENTAL,
                    self.REPLICATION_KEYS: {'MetaData'},
                }

        return mdata


    def expected_primary_keys(self):
        """
        return a dictionary with key of table name
        and value as a set of primary key fields
        """
        return {table: properties.get(self.PRIMARY_KEYS, set())
                for table, properties
                in self.expected_metadata().items()}


    def expected_automatic_fields(self):
        """
        return a dictionary with key of table name and value as the primary keys and replication keys
        """
        pks = self.expected_primary_keys()
        rks = self.expected_replication_keys()

        return {stream: rks.get(stream, set()) | pks.get(stream, set())
                for stream in self.expected_streams()}


    def expected_replication_method(self):
        """return a dictionary with key of table name and value of replication method"""
        return {table: properties.get(self.REPLICATION_METHOD, None)
                for table, properties
                in self.expected_metadata().items()}

    def expected_streams(self):
        """A set of expected stream names"""
        return set(self.expected_metadata().keys())


    def expected_replication_keys(self):
        """
        return a dictionary with key of table name
        and value as a set of replication key fields
        """
        return {table: properties.get(self.REPLICATION_KEYS, set())
                for table, properties
                in self.expected_metadata().items()}

    def expected_primary_keys(self):

        """
        return a dictionary with key of table name
        and value as a set of primary key fields
        """
        return {table: properties.get(self.PRIMARY_KEYS, set())
                for table, properties
                in self.expected_metadata().items()}

    def ensure_connection(self, original=True):
        def preserve_refresh_token(existing_conns, payload):
            if not existing_conns:
                return payload
            conn_with_creds = connections.fetch_existing_connection_with_creds(existing_conns[0]['id'])
            # Even though is a credential, this API posts the entire payload using properties
            payload['properties']['refresh_token'] = conn_with_creds['credentials']['refresh_token']
            return payload

        conn_id = connections.ensure_connection(self, payload_hook=preserve_refresh_token, original_properties = original)
        return conn_id


    def select_all_streams_and_fields(self, conn_id, catalogs, select_all_fields: bool = True):
        """Select all streams and all fields within streams"""
        for catalog in catalogs:
            schema = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])

            non_selected_properties = []
            if not select_all_fields:
                # get a list of all properties so that none are selected
                non_selected_properties = schema.get('annotated-schema', {}).get(
                    'properties', {})
                # remove properties that are automatic
                for prop in self.expected_automatic_fields().get(catalog['stream_name'], []):
                    if prop in non_selected_properties:
                        del non_selected_properties[prop]
                non_selected_properties = non_selected_properties.keys()
            additional_md = []

            connections.select_catalog_and_fields_via_metadata(
                conn_id, catalog, schema, additional_md=additional_md,
                non_selected_fields=non_selected_properties
            )

    ##########################################################################
    ### Tap Specific Methods
    ##########################################################################
    def minimum_record_count_by_stream(self):
        """
        The US sandbox comes with the following preset data

          Construction Trade
            141 transactions
            31 customers
            26 vendors
            4 employees
            20 items
            90 accounts

        see their docs for more info:
        https://developer.intuit.com/app/developer/qbo/docs/develop/sandboxes#launch-a-sandbox

        For the remaining streams at least 1 record existed already, or 1 has been added.
        """
        # All streams should have at least a record (thi)
        record_counts = {stream: 1 for stream in self.expected_check_streams()}

        # By default quickbooks sandbox apps come with the following records
        record_counts["accounts"]= 90
        record_counts["customers"] = 29
        record_counts["employees"] = 2
        record_counts["items"] = 18
        record_counts["vendors"] = 26

        return record_counts

    def strptime_to_timestamp(self, dtime):
        for date_format in self.DATETIME_FMT:
            try:
                date_stripped = int(time.mktime(dt.strptime(dtime, date_format).timetuple()))
                return date_stripped
            except ValueError:
                continue

    def strftime_to_datetime(self, date_str):
        for date_format in self.DATETIME_FMT:
            try:
                date_time = dt.strptime(date_str, date_format)
                return date_time
            except ValueError:
                continue

    def is_report_stream(self, stream):
        return stream in ["profit_loss_report"]

    def convert_state_to_utc(self, date_str):
        """
        Convert a saved bookmark value of the form '2020-08-25T13:17:36-07:00' to a string
        formatted utc datetime, in order to compare against the json formatted datetime values
        """
        date_object = self.strftime_to_datetime(date_str)
        date_object_utc = date_object.astimezone(tz=pytz.UTC)
        return dt.strftime(date_object_utc, "%Y-%m-%dT%H:%M:%SZ")

    def is_incremental(self, stream):
        """Checking if the given stream is incremental or not."""
        return self.expected_metadata().get(stream).get(self.REPLICATION_METHOD) == self.INCREMENTAL

    def create_interrupt_sync_state(self, state, interrupt_stream, pending_streams, sync_records):
        """This function will create a new interrupt sync bookmark state where
        "currently_syncing" will have the non-null value and this state will be
        used to resume the data extraction."""

        interrupted_sync_states = copy.deepcopy(state)
        bookmark_state = interrupted_sync_states["bookmarks"]
        # Set the interrupt stream as currently syncing
        interrupted_sync_states["currently_syncing"] = interrupt_stream

        # For pending streams, removing the bookmark_value
        for stream in pending_streams:
            bookmark_state.pop(stream, None)

        if self.is_incremental(interrupt_stream):
            # update state for interrupt stream and set the bookmark to a date earlier
            interrupt_stream_bookmark = bookmark_state.get(interrupt_stream, {})
            interrupt_stream_bookmark.pop("offset", None)

            replication_key = list(state["bookmarks"][interrupt_stream].keys())[0]
            interrupt_stream_rec = []
            for record in sync_records.get(interrupt_stream).get("messages"):
                if record.get("action") == "upsert":
                    rec = record.get("data")
                    interrupt_stream_rec.append(rec)
            interrupt_stream_index = len(interrupt_stream_rec) // 2 if len(interrupt_stream_rec) > 1 else 0
            interrupt_stream_bookmark[replication_key] = interrupt_stream_rec[interrupt_stream_index]["MetaData"][replication_key]
            bookmark_state[interrupt_stream] = interrupt_stream_bookmark
            interrupted_sync_states["bookmarks"] = bookmark_state
        return interrupted_sync_states
