import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

from base import TestQuickbooksBase

page_size_key = 'max_results'

class TestQuickbooksPagination(TestQuickbooksBase):

    def expected_streams(self):
        """
        All streams except 'budgets' are under test. The 'budgets' stream
        returns a single record of the current budget state and will never exceed
        our pagination size (max_results) for this test.
        """
        return self.expected_check_streams().difference({'budgets'})

    def get_properties(self):
        return {
            'start_date' : '2016-06-02T00:00:00Z',
            'sandbox': 'true',
            page_size_key: '10'
        }


    def test_run(self):
        """Executing run_test with different page_size values for different streams
        - Verify for each stream you can get multiple pages of data. This requires we ensure more than 1 page of data exists at all times for any given stream.
        - Verify the sync meets or exceeds the default record count
        - Verify we did not duplicate any records across pages
        """
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

        # Examine target file
        sync_records = runner.get_records_from_target_output()
        sync_record_count = runner.examine_target_output_file(
            self, conn_id, expected_streams, self.expected_primary_keys())

        # Test by stream
        for stream in expected_streams:
            if stream == "deleted_objects": # Deleted Objects stream does not have Pagination support
                continue
            with self.subTest(stream=stream):

                expected_count = self.minimum_record_count_by_stream().get(stream)
                record_count = sync_record_count.get(stream, 0)

                sync_messages = sync_records.get(stream, {'messages': []}).get('messages')

                primary_key = self.expected_primary_keys().get(stream).pop()

                # Verify the sync meets or exceeds the default record count
                self.assertLessEqual(expected_count, record_count)

                # Verify the number or records exceeds the max_results (api limit)
                if self.is_report_stream(stream):
                    #Tap is making API call in 30 days window for reports stream
                    pagination_threshold = 30
                else:
                    pagination_threshold = int(self.get_properties().get(page_size_key))
                self.assertGreater(record_count, pagination_threshold,
                                   msg="Record count not large enough to gaurantee pagination.")

                # Verify we did not duplicate any records across pages
                records_pks_set = {message.get('data').get(primary_key) for message in sync_messages}
                records_pks_list = [message.get('data').get(primary_key) for message in sync_messages]
                self.assertCountEqual(records_pks_set, records_pks_list,
                                      msg="We have duplicate records for {}".format(stream))
