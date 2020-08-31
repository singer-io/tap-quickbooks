import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

from base import TestQuickbooksBase

class TestQuickbooksPagination(TestQuickbooksBase):
    def name(self):
        return "tap_tester_quickbooks_combined_test"


    def expected_streams(self):
        return {'accounts'}


    def get_properties(self):
        return {
            'start_date' : '2016-06-02T00:00:00Z',
            'sandbox': 'true',
            'max_results': '10'
        }


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
        sync_record_count = runner.examine_target_output_file(
            self, conn_id, self.expected_streams(), self.expected_primary_keys())

        # Test by stream
        for stream in self.expected_streams():
            with self.subTest(stream=stream):

                record_count = sync_record_count.get(stream, 0)

                expected_count = self.minimum_record_count_by_stream().get(stream)

                # Verify the sync meets or exceeds the default record count
                self.assertLessEqual(expected_count, record_count)

                # Verify the number or records exceeds the max_results (api limit)
                api_limit = self.get_properties().get('max_results')
                self.assertGreater(record_count, api_limit,
                                   msg="Record count not large enough to gaurantee pagination.")
