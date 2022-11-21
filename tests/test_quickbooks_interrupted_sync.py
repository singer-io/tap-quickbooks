from datetime import datetime as dt
from tap_tester import runner, connections, menagerie
from base import TestQuickbooksBase
from singer.utils import strptime_to_utc

class TestQuickbooksInterruptedSyncTest(TestQuickbooksBase):

    def assertIsDateFormat(self, value, str_format):
        """
        Assertion Method that verifies a string value is a formatted datetime with
        the specified format.
        """
        try:
            dt.strptime(value, str_format)
        except ValueError as err:
            raise AssertionError(f"Value: {value} does not conform to expected format: {str_format}") from err

    def test_run(self):
        """
        Scenario: A sync job is interrupted. The state is saved with `currently_syncing`.
                  The next sync job kicks off and the tap picks back up on that `currently_syncing` stream.
        Expected State Structure:
            {
                "currently_syncing": "stream-name",
                "bookmarks": {
                    "stream-name-1": {"LastUpdatedTime": "2021-06-10T11:46:26-07:00"},
                    "stream-name-2": {"LastUpdatedTime": "2021-06-10T11:46:26-07:00"}
                }
            }
        Test Cases:
        - Verify an interrupted sync can resume based on the `currently_syncing` and stream level bookmark value.
        - Verify only records with replication-key values greater than or equal to the stream level bookmark are
            replicated on the resuming sync for the interrupted stream.
        - Verify the yet-to-be-synced streams are replicated following the interrupted stream in the resuming sync.
        """

        self.start_date = "2020-01-01T00:00:00Z"
        start_date_datetime = dt.strptime(self.start_date, "%Y-%m-%dT%H:%M:%SZ")

        conn_id = self.ensure_connection(original=False)

        expected_streams = {"accounts", "bill_payments", "payments", "vendors"}

        # Run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # Verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, msg="unable to locate schemas for connection {}".format(conn_id))

        # Select only the expected streams tables
        catalog_entries = [ce for ce in found_catalogs if ce['tap_stream_id'] in expected_streams]

        # Catalog selection
        self.select_all_streams_and_fields(conn_id, catalog_entries)

        # Run a sync job using orchestrator
        sync_job_name = runner.run_sync_mode(self, conn_id)
        synced_records_full_sync = runner.get_records_from_target_output()

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        full_sync_state = menagerie.get_state(conn_id)


        ##########################################################################
        # Update State Between Syncs
        ##########################################################################

        interrupt_stream = "payments"
        pending_streams = {"vendors"}
        self.create_interrupt_sync_state(full_sync_state, interrupt_stream, pending_streams, synced_records_full_sync)

        # State to run 2nd sync
        #   payments: currently syncing
        #   accounts and bill_payments: synced records successfully
        #   vendors: remaining to sync

        state = self.create_interrupt_sync_state(full_sync_state, interrupt_stream, pending_streams, synced_records_full_sync)
        # Set state for 2nd sync
        menagerie.set_state(conn_id, state)

        # Run sync after interruption
        sync_job_name = runner.run_sync_mode(self, conn_id)
        synced_records_interrupted_sync = runner.get_records_from_target_output()
        record_count_by_stream_interrupted_sync = runner.examine_target_output_file(
            self, conn_id, self.expected_streams(), self.expected_primary_keys())

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        final_state = menagerie.get_state(conn_id)
        currently_syncing = final_state.get('currently_syncing')

        # Checking resuming the sync resulted in a successfully saved state
        with self.subTest():

            # Verify sync is not interrupted by checking currently_syncing in the state for sync
            self.assertIsNone(currently_syncing)

            # Verify bookmarks are saved
            self.assertIsNotNone(final_state.get('bookmarks'))

            # Verify final_state is equal to uninterrupted sync's state
            # (This is what the value would have been without an interruption and proves resuming succeeds)
            self.assertDictEqual(final_state, full_sync_state)

        # Stream level assertions
        for stream in expected_streams:
            with self.subTest(stream=stream):
                
                # Gather actual results
                full_records = [message['data'] for message in synced_records_full_sync.get(stream, {}).get('messages', [])]
                interrupted_records = [message['data'] for message in synced_records_interrupted_sync.get(stream, {}).get('messages', [])]
                interrupted_record_count = record_count_by_stream_interrupted_sync.get(stream, 0)

                # Final bookmark after interrupted sync
                final_stream_bookmark = final_state['bookmarks'][stream]['LastUpdatedTime']

                # NOTE: All streams are INCREMENTAL streams for this Tap.

                # Verify final bookmark saved the match formatting standards for resuming sync
                self.assertIsNotNone(final_stream_bookmark)
                self.assertIsInstance(final_stream_bookmark, str)
                self.assertIsDateFormat(final_stream_bookmark, "%Y-%m-%dT%H:%M:%S%z") # Bookmark is being saved with timezone

                if stream == state['currently_syncing']:

                    # get the bookmarked value from state for the currently syncing stream
                    interrupted_stream_datetime = strptime_to_utc(state['bookmarks'][stream]['LastUpdatedTime']).replace(tzinfo=None)

                    # - Verify resuming sync only replicates records with replication key values greater or
                    #       equal to the state for streams that were replicated during the interrupted sync.
                    # - Verify the interrupted sync replicates the expected record set all interrupted records are in full records
                    for record in interrupted_records:
                        rec_time = dt.strptime(record.get('MetaData').get('LastUpdatedTime'), "%Y-%m-%dT%H:%M:%S.%fZ")
                        self.assertGreaterEqual(rec_time, interrupted_stream_datetime)

                        self.assertIn(record, full_records, msg='Incremental table record in interrupted sync not found in full sync')

                    # Record count for all streams of interrupted sync match expectations
                    full_records_after_interrupted_bookmark = 0
                    for record in full_records:
                        rec_time = dt.strptime(record.get('MetaData').get('LastUpdatedTime'), "%Y-%m-%dT%H:%M:%S.%fZ")
                        if rec_time >= interrupted_stream_datetime:
                            full_records_after_interrupted_bookmark += 1

                    self.assertEqual(full_records_after_interrupted_bookmark, interrupted_record_count, \
                                        msg='Expected {} records in each sync'.format(full_records_after_interrupted_bookmark))

                else:
                    # Get the date to start 2nd sync for non-interrupted streams
                    synced_stream_bookmark = state['bookmarks'].get(stream, {}).get('LastUpdatedTime')
                    if synced_stream_bookmark:
                        synced_stream_datetime = dt.strptime(self.convert_state_to_utc(synced_stream_bookmark), "%Y-%m-%dT%H:%M:%SZ")
                    else:
                        synced_stream_datetime = start_date_datetime

                    # Verify we replicated some records for the non-interrupted streams
                    self.assertGreater(interrupted_record_count, 0)

                    # - Verify resuming sync only replicates records with replication key values greater or equal to
                    #       the state for streams that were replicated during the interrupted sync.
                    # - Verify resuming sync replicates all records that were found in the full sync (non-interupted)
                    for record in interrupted_records:
                        rec_time = dt.strptime(record.get('MetaData').get('LastUpdatedTime'), "%Y-%m-%dT%H:%M:%S.%fZ")
                        self.assertGreaterEqual(rec_time, synced_stream_datetime)

                        self.assertIn(record, full_records, msg='Unexpected record replicated in resuming sync.')
