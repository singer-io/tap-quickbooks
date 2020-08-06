import os
import unittest
from datetime import datetime as dt
from datetime import timedelta

import tap_tester.menagerie   as menagerie
import tap_tester.connections as connections

#from test_client import TestClient


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
            "TAP_QUICKBOOKS_CLIENT_ID",
            "TAP_QUICKBOOKS_CLIENT_SECRET",
            "TAP_QUICKBOOKS_REALM_ID",
            "TAP_QUICKBOOKS_REFRESH_TOKEN" # maybe?
        ] if os.getenv(x) is None]
        if missing_envs:
            raise Exception("Missing environment variables: {}".format(missing_envs))

    @staticmethod
    def get_type():
        return "platform.quickbooks"

    @staticmethod
    def tap_name():
        return "tap-quickbooks"

    def get_properties(self, original: bool = True):
        return_value = {
            'start_date' : '2016-06-02T00:00:00Z',
            'end_date' : '2016-06-06T00:00:00Z'
        }
        if original:
            return return_value

        # Start Date test needs the new connections start date to be after the default
        assert self.START_DATE > return_value["start_date"]
        assert self.END_DATE > return_value["end_date"]

        # Reassign start and end dates
        return_value["start_date"] = self.START_DATE
        return_value["end_date"] = self.END_DATE
        return return_value

    def get_credentials(self):
        #token = TestClient.get_token_information()
        return {
            'refresh_token': token['refresh_token'],
            'client_id': token['client_id'],
            'client_secret': token['client_secret'],
            'access_token': token['access_token']
        }

    @staticmethod
    def expected_check_streams():
        return {
            'accounts',
            'invoices',
            'items'
        }

    def expected_metadata(self):
        """The expected streams and metadata about the streams"""

        return {
            "accounts": {
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {'LastUpdatedTime'},
            },
            "invoices": {
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {'LastUpdatedTime'},
            },
            "items": {
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {'LastUpdatedTime'},
            },
        }

    def expected_replication_method(self):
        """return a dictionary with key of table name and value of replication method"""
        return {table: properties.get(self.REPLICATION_METHOD, None)
                for table, properties
                in self.expected_metadata().items()}

    def expected_streams(self):
        """A set of expected stream names"""
        return set(self.expected_metadata().keys())

    def expected_primary_keys(self):

        """
        return a dictionary with key of table name
        and value as a set of primary key fields
        """
        return {table: properties.get(self.PRIMARY_KEYS, set())
                for table, properties
                in self.expected_metadata().items()}

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
