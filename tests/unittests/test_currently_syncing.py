import unittest
from unittest import mock
from singer.catalog import Catalog
from tap_quickbooks.client import QuickbooksClient
from tap_quickbooks.sync import do_sync

class TestCurrentlySuncing(unittest.TestCase):

    @mock.patch("tap_quickbooks.streams.Stream.sync")
    @mock.patch("singer.set_currently_syncing", return_value={})
    @mock.patch("tap_quickbooks.client.QuickbooksClient.__init__", return_value=None)
    def test_currently_syncing(self, mocked_stream_sync, mocked_currently_syncing, mocked_quickbooks_client):
        """Test case to check the currently syncing is working as expected"""
        config = {
            "client_id": "test_client_id",
            "start_date": "2012-08-21T00:00:00Z",
            "refresh_token": "test_refresh_token",
            "client_secret": "test_client_secret",
            "sandbox": "true",
            "user_agent": "test",
            "realm_id": "12345"
        }
        state = {}

        client = QuickbooksClient("/opt/code/config.json", config)

        fake_catalog = {
            "streams":[
                {"tap_stream_id": "bills",
                 "schema": {"properties": {}},
                 "metadata": [
                     {"breadcrumb": [],
                      "metadata": {"selected": "true"}}
                 ],},
                {"tap_stream_id": "budgets",
                 "schema": {"properties": {}},
                 "metadata": [
                     {"breadcrumb": [],
                      "metadata": {"selected": "true"}}
                 ],},
                {"tap_stream_id": "invoices",
                 "schema": {"properties": {}},
                 "metadata": [
                     {"breadcrumb": [],
                      "metadata": {"selected": "true"}}
                 ],},
            ]
        }

        do_sync(client=client,
                config=config,
                state=state,
                catalog=Catalog.from_dict(fake_catalog))

        # Verify the call count
        self.assertEqual(mocked_currently_syncing.call_count, 4)

        # Expected calls of 'singer.set_currently_syncing'
        expected_calls = [
            mock.call({}, 'bills'),
            mock.call({}, 'budgets'),
            mock.call({}, 'invoices'),
            mock.call({}, None)
        ]
        # Verify the calls
        self.assertEqual(mocked_currently_syncing.mock_calls, expected_calls)
