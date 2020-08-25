import os
import unittest
from datetime import datetime as dt
from datetime import timedelta

import tap_tester.menagerie   as menagerie
import tap_tester.connections as connections


class TestQuickbooksBase(unittest.TestCase):
    REPLICATION_KEYS = "valid-replication-keys"
    PRIMARY_KEYS = "table-key-properties"
    FOREIGN_KEYS = "table-foreign-key-properties"
    REPLICATION_METHOD = "forced-replication-method"
    API_LIMIT = "max-row-limit"
    INCREMENTAL = "INCREMENTAL"
    FULL = "FULL_TABLE"
    START_DATE_FORMAT = "%Y-%m-%dT00:00:00Z" # %H:%M:%SZ

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

    def get_properties(self):
        return {
            'start_date' : '2016-06-02T00:00:00Z',
            'sandbox': 'true'
            #'end_date' : '2016-06-06T00:00:00Z'
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
            "invoices",
            "items",
            "budgets",
            "classes",
            "credit_memos",
            "bill_payments",
            "sales_receipts",
            "purchases",
            "payments",
            "purchase_orders",
            "payment_methods",
            "journal_entries",
            "items",
            "invoices",
            "customers",
            "refund_receipts",
            "deposits",
            "departments",
            "employees",
            "estimates",
            "bills",
            "tax_agencies",
            "tax_codes",
            "tax_rates",
            "terms",
            "time_activities",
            "transfers",
            "vendor_credits",
            "vendors",
        }

    def expected_metadata(self):
        """The expected streams and metadata about the streams"""

        mdata = {}
        for stream in self.expected_check_streams():
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

    def ensure_connection(self):
        def preserve_refresh_token(existing_conns, payload):
            if not existing_conns:
                return payload
            conn_with_creds = connections.fetch_existing_connection_with_creds(existing_conns[0]['id'])
            # Even though is a credential, this API posts the entire payload using properties
            payload['properties']['refresh_token'] = conn_with_creds['credentials']['refresh_token']
            return payload

        conn_id = connections.ensure_connection(self, payload_hook=preserve_refresh_token)
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
