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

def get_data(stream_name, count: int = 0):
    """Return data based on stream_name and count of data """

    if stream_name == "profit_loss_report":
        return [{"ReportDate": "2022-06-21T00:00:00.000000Z", "AccountingMethod": "Accrual"}]

    if stream_name == 'deleted_objects':
        mock_data = {'CDCResponse': [{'QueryResponse': [{'Account': []}]}]}
        for i in range(count):
            dummy = {'Id': i+1, 'status': 'Deleted',
                     'MetaData': {'LastUpdatedTime': '2022-06-21T00:00:00Z'}}
            mock_data.get('CDCResponse')[0].get('QueryResponse')[
                0].get('Account').append(dummy)
        return mock_data

class TestSyncCov(unittest.TestCase):
    @mock.patch('tap_quickbooks.client.QuickbooksClient.get')
    @mock.patch('singer.write_record')
    @mock.patch('singer.write_state')
    @mock.patch('tap_quickbooks.client.QuickbooksClient.__init__', return_value=None)
    def test_sync_unknown_stream(self, mock_init, mock_write_state, mock_write_rcord, mock_get):
        """Test we raise an error for unknown stream"""

        client = QuickbooksClient('path', {})
        mock_get.return_value = get_data('test')

        mock_catalog = MockCatalog(['test'])
        with self.assertRaises(Exception) as e:
            do_sync(client=client, config={}, state={}, catalog=mock_catalog)

        self.assertEqual(str(e.exception),
                         "Attempted to sync unknown stream test")

    @parameterized.expand([  # test_name, [state, config, write_state_call_count]
        ['start_date_unused', [{'bookmarks': {"profit_loss_report": {
            "LastUpdatedTime": "2022-07-21T00:00:00+00:00"}}}, {}, 4]],
        ['start_date_used', [{}, {'start_date': '2022-07-21T00:00:00Z'}, 4]],
    ])
    @mock.patch('tap_quickbooks.client.QuickbooksClient.get')
    @mock.patch('singer.write_record')
    @mock.patch('singer.write_state')
    @mock.patch('tap_quickbooks.streams.ReportStream.parse_report_columns')
    @mock.patch('tap_quickbooks.streams.ReportStream.parse_report_rows')
    @mock.patch('tap_quickbooks.streams.ReportStream.day_wise_reports')
    @mock.patch('singer.utils.now')
    @mock.patch('tap_quickbooks.client.QuickbooksClient.__init__', return_value=None)
    def test_sync_report_stream(self, test_name, test_data, mock_init, mock_now, mock_day_wise_report, mock_report_rows, mock_report_columns, mock_write_state, mock_write_record, mock_get):
        """Test sync call for the Report stream"""

        client = QuickbooksClient('path', {})
        now_time = utils.strptime_with_tz('2022-07-22 15:32:13.000000+00:00')
        mock_now.return_value = now_time

        config = test_data[1]
        mock_catalog = MockCatalog(['profit_loss_report'])
        mock_day_wise_report.return_value = get_data('profit_loss_report')

        do_sync(client=client, config=config, state=test_data[0], catalog=mock_catalog)

        expected_calls = [
            mock.call(
                {"currently_syncing": None, "bookmarks": {"profit_loss_report": {"LastUpdatedTime": "2022-06-21T00:00:00+00:00"}}}),
            mock.call(
                {"currently_syncing": None, "bookmarks": {"profit_loss_report": {"LastUpdatedTime": "2022-06-21T00:00:00+00:00"}}}),
            mock.call(
                {"currently_syncing": None, "bookmarks": {"profit_loss_report": {"LastUpdatedTime": "2022-06-21T00:00:00+00:00"}}}),
            mock.call(
                {"currently_syncing": None, "bookmarks": {"profit_loss_report": {"LastUpdatedTime": "2022-06-21T00:00:00+00:00"}}})
        ]

        self.assertEqual(mock_write_state.mock_calls, expected_calls)
        self.assertEqual(mock_write_state.call_count, test_data[2])

    @parameterized.expand([  # test_name, data_count, write_state_count, client_get_count
        ['having_less_than_1000_data', 100, 3, 1],
        ['having_more_than_1000_data', 1001, 3, 25],
    ])
    @mock.patch('tap_quickbooks.client.QuickbooksClient.get')
    @mock.patch('singer.write_record')
    @mock.patch('singer.write_state')
    @mock.patch('tap_quickbooks.client.QuickbooksClient.__init__', return_value=None)
    def test_sync_deleted_stream(self, test_name, test_data_count, exp_write_state_count, exp_get_count, mock_init, mock_write_state, mock_write_record, mock_get):
        """Test sync call for deleted_stream"""

        client = QuickbooksClient('path', {})

        config = {'start_date': '2022-05-21T00:00:00Z'}

        mock_catalog = MockCatalog(['deleted_objects'])

        mock_get.return_value = dict(
            get_data('deleted_objects', test_data_count))

        do_sync(client=client, config=config, state={}, catalog=mock_catalog)

        expected_calls = [
            mock.call(
                {"currently_syncing": None, "bookmarks": {"deleted_objects": {"LastUpdatedTime": "2022-06-21T00:00:00Z"}}}),
            mock.call(
                {"currently_syncing": None, "bookmarks": {"deleted_objects": {"LastUpdatedTime": "2022-06-21T00:00:00Z"}}}),
            mock.call(
                {"currently_syncing": None, "bookmarks": {"deleted_objects": {"LastUpdatedTime": "2022-06-21T00:00:00Z"}}}),
        ]

        self.assertEqual(mock_write_state.mock_calls, expected_calls)

        self.assertEqual(mock_write_state.call_count, exp_write_state_count)

        self.assertEqual(mock_get.call_count, exp_get_count)
