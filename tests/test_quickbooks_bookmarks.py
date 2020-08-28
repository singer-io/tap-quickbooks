import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
import re

from base import TestQuickbooksBase

class TestQuickbooksBookmarks(TestQuickbooksBase):
    def name(self):
        return "tap_tester_quickbooks_combined_test"

    def expected_replication_keys(self):
        """
        return a dictionary with key of table name
        and value as a set of replication key fields
        """
        return {table: properties.get(self.REPLICATION_KEYS, set())
                for table, properties
                in self.expected_metadata().items()}

    def expected_streams(self):
        return {'accounts'}


    def test_run(self):
        conn_id = self.ensure_connection()

        # Run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # Verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, msg="unable to locate schemas for connection {}".format(conn_id))

        # Select only the expected streams tables
        expected_streams = self.expected_streams()
        catalog_entries = [ce for ce in found_catalogs if ce['tap_stream_id'] in expected_streams]
        self.select_all_streams_and_fields(conn_id, catalog_entries)

        # Run a sync job using orchestrator
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Verify the bookmark is correct, LastUpdatedTime assumes no other Account records are added to Sandbox
        actual_state = menagerie.get_state(conn_id)
        expected_state = {'bookmarks': {'accounts': {'LastUpdatedTime': '2020-08-25T13:17:37-07:00'}}}
        self.assertEqual(actual_state, expected_state)

        # Verify actual rows were synced
        sync_record_count = runner.examine_target_output_file(
            self, conn_id, self.expected_streams(), self.expected_primary_keys())

        # Examine target output
        for stream in self.expected_streams():
            with self.subTest(stream=stream):
                # Each stream should have 1 or more records returned
                self.assertGreaterEqual(sync_record_count[stream], 1)

        # Update state and sync again
        new_state = {'bookmarks': {'accounts': {'LastUpdatedTime': '2020-08-25T13:17:36-07:00'}}}
        menagerie.set_state(conn_id, new_state)

        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify only 1 record synced with the new state
        sync_record_count = runner.examine_target_output_file(
            self, conn_id, self.expected_streams(), self.expected_primary_keys())
        self.assertEqual(sync_record_count, {'accounts': 1})
