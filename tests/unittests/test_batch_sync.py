import unittest
from unittest import mock
from parameterized import parameterized
from tap_quickbooks.client import QuickbooksClient
from tap_quickbooks.sync import do_sync
from singer import utils

class Schema:
    """Mocked Schema"""

    def __init__(self, stream_name) -> None:
        self.stream = stream_name

    def to_dict(self):
        return {'stream': self.stream}

class Stream:
    """Mocked Stream """

    def __init__(self, stream_name) -> None:
        self.tap_stream_id = stream_name
        self.schema = Schema(stream_name)
        self.metadata = {}

class MockCatalog:
    """Mocked Catalog """

    def __init__(self, stream) -> None:
        self.stream = stream

    def get_selected_streams(self, state):
        for stream in self.stream:
            yield Stream(stream)

def get_query_response(table_name, stream_name):
    if stream_name == 'accounts':
        return {table_name: [{'id': 123, 'foo': 'bar', 'MetaData': {'LastUpdatedTime': '2020-01-01'}},
                             {'id': 456, 'foo': 'baz', 'MetaData': {'LastUpdatedTime': '2020-02-02'}}],
                'maxResults': 2}
    elif stream_name == 'budgets':
        return {table_name: [{'id': 111, 'budgetAmount': 100.00, 'MetaData': {'LastUpdatedTime': '2020-01-01'}},
                             {'id': 222, 'budgetAmount': 555.55, 'MetaData': {'LastUpdatedTime': '2020-01-02'}},
                             {'id': 333, 'budgetAmount': 0.07, 'MetaData': {'LastUpdatedTime': '2020-01-03'}}],
                'maxResults': 3}
    else:
        return {table_name: [], 'maxResults': 0}

def get_data(stream_names):
    """Return data based on stream_name and count of data """

    return {
        'BatchItemResponse': [{'QueryResponse': get_query_response(table_name, stream_name)}
                              for table_name, stream_name in stream_names]
    }

class TestBatchSync(unittest.TestCase):
    config = {
        'realm_id': 123,
        'client_id': 'client',
        'client_secret': 'shhhhh',
        'user_agent': 'joey',
        'refresh_token': 'asdfasdfda'
    }

    @mock.patch('tap_quickbooks.client.QuickbooksClient.post')
    @mock.patch('singer.write_record')
    @mock.patch('singer.write_state')
    def test_sync_batch_streams(self, mock_write_state, mock_write_record, mock_post):
        """Test we can sync batch streams"""

        client = QuickbooksClient('path', self.config)
        mock_post.side_effect = [
            get_data([('Account', 'accounts'), ('Budget', 'budgets')]),
            get_data([('Account', 'accounts')]),
            get_data([])
        ]

        mock_catalog = MockCatalog(['accounts', 'budgets'])
        do_sync(client=client, config=self.config, state={}, catalog=mock_catalog)

        expected_calls = [
            mock.call(
                {'bookmarks': {'accounts': {'LastUpdatedTime': '2020-02-02'}, 'budgets': {'LastUpdatedTime': '2020-01-03'}}, 'currently_syncing': None}),
            mock.call(
                {'bookmarks': {'accounts': {'LastUpdatedTime': '2020-02-02'}, 'budgets': {'LastUpdatedTime': '2020-01-03'}}, 'currently_syncing': None}),
            mock.call(
                {'bookmarks': {'accounts': {'LastUpdatedTime': '2020-02-02'}, 'budgets': {'LastUpdatedTime': '2020-01-03'}}, 'currently_syncing': None}),
            mock.call(
                {'bookmarks': {'accounts': {'LastUpdatedTime': '2020-02-02'}, 'budgets': {'LastUpdatedTime': '2020-01-03'}}, 'currently_syncing': None})
            ]

        self.assertEqual(mock_write_state.mock_calls, expected_calls)
        self.assertEqual(mock_post.call_count, 3)
        self.assertEqual(mock_write_record.call_count, 7)

    @mock.patch('tap_quickbooks.client.QuickbooksClient.post')
    @mock.patch('singer.write_record')
    @mock.patch('singer.write_state')
    def test_sync_batch_streams_makes_batch_request(self, mock_write_state, mock_write_record, mock_post):
        """Minor version is included in api call"""

        client = QuickbooksClient('path', self.config)
        mock_post.return_value = get_data([])

        mock_catalog = MockCatalog(['accounts', 'budgets'])
        do_sync(client=client, config=self.config, state={}, catalog=mock_catalog)

        self.assertEqual(mock_post.mock_calls, [
            mock.call(
                '/v3/company/{realm_id}/batch?minorversion=75',
                data='{"BatchItemRequest": [{"bId": "Account", "Query": "SELECT * FROM Account WHERE Metadata.LastUpdatedTime >= \'None\' AND Active IN (true, false) ORDER BY Metadata.LastUpdatedTime ASC STARTPOSITION 1 MAXRESULTS 1000"}, {"bId": "Budget", "Query": "SELECT * FROM Budget WHERE Metadata.LastUpdatedTime >= \'None\' AND Active IN (true, false) ORDER BY Metadata.LastUpdatedTime ASC STARTPOSITION 1 MAXRESULTS 1000"}]}')
            ]
        )
