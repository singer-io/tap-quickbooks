import unittest

import tap_quickbooks.query_builder as query_builder

class TestQueryBuilder(unittest.TestCase):

    def test_build_query_simple(self):
        query = query_builder.build_query("test", "2020-08-18", 1, 200)
        self.assertEqual(query, "SELECT * FROM test WHERE Metadata.LastUpdatedTime >= '2020-08-18' ORDER BY Metadata.LastUpdatedTime ASC STARTPOSITION 1 MAXRESULTS 200")

    def test_build_query_additional_where(self):
        query = query_builder.build_query("test", "2020-08-18", 1, 200, "Active IN (true, false)")
        self.assertEqual(query, "SELECT * FROM test WHERE Metadata.LastUpdatedTime >= '2020-08-18' AND Active IN (true, false) ORDER BY Metadata.LastUpdatedTime ASC STARTPOSITION 1 MAXRESULTS 200")

    def test_build_batch_query(self):
        streams = [MockStream('Foo', 'foos'),
                   MockStream('Bar', 'bars', 'Active IN (true, false)')]
        bookmarks = {'foos': '2020-07-08T00:00:00Z',
                     'bars': '2025-01-25T01:23:45.678Z'}
        start_positions = {'foos': 12, 'bars': 123}
        query = query_builder.build_batch_query(streams, bookmarks, start_positions, 500)

        self.assertEqual(
            query,
            {
                "BatchItemRequest": [
                    {
                        "bId": "Foo",
                        "Query": query_builder.build_query('Foo', '2020-07-08T00:00:00Z', 12, 500)
                    },
                    {
                        "bId": "Bar",
                        "Query": query_builder.build_query('Bar', '2025-01-25T01:23:45.678Z', 123,500, 'Active IN (true, false)')
                    }
                ]
            }
        )

class MockStream:
    def __init__(self, table_name, stream_name, additional_where=None):
        self.table_name = table_name
        self.stream_name = stream_name
        self.additional_where = additional_where
