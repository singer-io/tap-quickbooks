import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
import re

from base import TestQuickbooksBase

class TestQuickbooksSyncAll(TestQuickbooksBase):

    def test_run(self):
        conn_id = self.ensure_connection()

        expected_streams = self.expected_streams()
        # Run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # Verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, msg="unable to locate schemas for connection {}".format(conn_id))

        # Select all tables and fields
        self.select_all_streams_and_fields(conn_id, found_catalogs)

        # Run a sync job using orchestrator
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Verify actual rows were synced
        sync_record_count = runner.examine_target_output_file(
            self, conn_id, expected_streams, self.expected_primary_keys())

        # Examine target output
        for stream in expected_streams:
            with self.subTest(stream=stream):
                # Each stream should have 1 or more records returned
                self.assertGreaterEqual(sync_record_count[stream], 1)

        # Taking sync response
        synced_records = runner.get_records_from_target_output()

        # Taking all streams from the response
        streams = synced_records.keys()

        custom_field_stream_count = 0
        for stream in streams:
            if stream in self.custom_command_streams:
                first_record = synced_records.get(stream).get('messages')[0].get('data')
                actual_custom_field_keys = list(first_record.get("CustomField")[0].keys())
                # For sand box only 3 fields are coming
                expected_custom_field_keys = ['DefinitionId','Name','Type']
                actual_custom_field_keys.sort()
                expected_custom_field_keys.sort()
                self.assertListEqual(actual_custom_field_keys,expected_custom_field_keys)
                custom_field_stream_count +=1
            else:
                continue 
        self.assertEqual(custom_field_stream_count,6)
