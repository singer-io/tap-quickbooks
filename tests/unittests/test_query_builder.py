import unittest

import tap_quickbooks.query_builder as query_builder

class TestQueryBuilder(unittest.TestCase):

    def test_build_query_simple(self):
        query = query_builder.build_query("test", "2020-08-18", 1, 200)
        self.assertEqual(query, "SELECT * FROM test WHERE Metadata.LastUpdatedTime >= '2020-08-18' ORDER BY Metadata.LastUpdatedTime ASC STARTPOSITION 1 MAXRESULTS 200")

    def test_build_query_additional_where(self):
        query = query_builder.build_query("test", "2020-08-18", 1, 200, "Active IN (true, false)")
        self.assertEqual(query, "SELECT * FROM test WHERE Metadata.LastUpdatedTime >= '2020-08-18' AND Active IN (true, false) ORDER BY Metadata.LastUpdatedTime ASC STARTPOSITION 1 MAXRESULTS 200")
