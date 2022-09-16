import unittest
from unittest import mock
from tap_quickbooks.client import QuickbooksClient
from tap_quickbooks.sync import do_sync

class Schema:
    """Mock class for the schema"""
    def __init__(self, stream):
        self.stream = stream

    def to_dict(self):
        return {'stream': self.stream}

class Stream:
    """Mock class for the stream"""
    def __init__(self, stream_name):
        self.tap_stream_id = stream_name
        self.stream = stream_name
        self.replication_key = 'updated_at'
        self.schema = Schema(stream_name)
        self.metadata = {}

class Catalog:
    """Mock class for the singer catalog"""
    def __init__(self, streams):
        self.streams = streams

    def get_selected_streams(self, state):
        for stream in self.streams:
            yield Stream(stream)

    def get_stream(self, stream_name):
        return Stream(stream_name)

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

        do_sync(client=client, config=config, state=state, catalog=Catalog(
            ['bills', 'budgets', 'invoices']))

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
