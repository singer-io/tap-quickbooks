import unittest
from unittest import mock

import singer

from tap_quickbooks.streams import DeletedObjects


class TestBookmarks(unittest.TestCase):
    @mock.patch('singer.write_state')
    def test_deleted_objects_uses_start_date_when_bookmark_missing(self, mock_write_state):
        client = mock.Mock()
        client.get.return_value = {
            'CDCResponse': [{
                'QueryResponse': [{
                    'Account': [{
                        'Id': '1',
                        'status': 'Deleted',
                        'MetaData': {'LastUpdatedTime': '2022-07-01T00:00:00Z'}
                    }]
                }]
            }]
        }

        config = {'start_date': '2022-01-01T00:00:00Z'}
        stream = DeletedObjects(client=client, config=config, state={})

        records = list(stream.sync())

        self.assertEqual(len(records), 1)
        self.assertEqual(
            singer.get_bookmark(stream.state, 'deleted_objects', 'LastUpdatedTime'),
            '2022-07-01T00:00:00Z'
        )
        first_call_params = client.get.call_args_list[0][1]['params']
        self.assertEqual(first_call_params['changedSince'], '2022-01-01T00:00:00Z')
        self.assertTrue(mock_write_state.called)

    def test_parse_data_and_write_tracks_max_bookmark(self):
        stream = DeletedObjects(client=mock.Mock(), config={}, state={})
        stream.max_date = '2022-01-01T00:00:00Z'

        payload = [
            {
                'Account': [
                    {'Id': '1', 'status': 'Deleted', 'MetaData': {'LastUpdatedTime': '2022-06-01T00:00:00Z'}},
                    {'Id': '2', 'status': 'Deleted', 'MetaData': {'LastUpdatedTime': '2022-03-01T00:00:00Z'}},
                ]
            }
        ]

        records = list(stream.parse_data_and_write(payload))

        self.assertEqual(len(records), 2)
        self.assertEqual(stream.max_date, '2022-06-01T00:00:00Z')
