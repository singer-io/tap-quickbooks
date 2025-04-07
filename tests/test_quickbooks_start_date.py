import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

from base import TestQuickbooksBase

class TestQuickbooksStartDate(TestQuickbooksBase):

    def expected_streams(self):
        """All streams are under test"""
        # Skipping stream deleted_objects due to data unavailability
        return self.expected_check_streams().difference({'budgets', 'deleted_objects'})

    def get_properties(self, original=True):
        if original:
            return {
                'start_date' : '2016-06-02T00:00:00Z',
                'sandbox': 'true'
            }
        else:
            return {
                'start_date' : '2020-08-24T00:00:00Z',
                'sandbox': 'true'
            }


    def test_run(self):
        """
        Test that the start_date configuration is respected
        - verify that a sync with a later start date has at least one record synced
        and less records than the 1st sync with a previous start date
        - verify that each stream has less records than the earlier start date sync
        - verify all data from later start data has bookmark values >= start_date
        """
        # SYNC 1
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
        first_sync_records = runner.get_records_from_target_output()
        first_sync_record_count = runner.examine_target_output_file(
            self, conn_id, expected_streams, self.expected_primary_keys())

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # SYNC 2
        conn_id = self.ensure_connection(original=False)

        # Run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # Select only the Accounts table
        found_catalogs = menagerie.get_catalogs(conn_id)
        catalog_entries = [ce for ce in found_catalogs if ce['tap_stream_id'] in expected_streams]
        self.select_all_streams_and_fields(conn_id, catalog_entries)

        # Run in Sync mode
        sync_job_name = runner.run_sync_mode(self, conn_id)
        second_sync_records = runner.get_records_from_target_output()
        second_sync_record_count = runner.examine_target_output_file(
            self, conn_id, expected_streams, self.expected_primary_keys())

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Test by stream
        for stream in expected_streams:
            with self.subTest(stream=stream):
                # record counts
                first_sync_count = first_sync_record_count.get(stream, 0)
                expected_first_sync_count = self.minimum_record_count_by_stream().get(stream)
                second_sync_count = second_sync_record_count.get(stream, 0)

                # record messages
                first_sync_messages = first_sync_records.get(stream, {'messages': []}).get('messages')
                second_sync_messages = second_sync_records.get(stream, {'messages': []}).get('messages')

                # start dates
                start_date_1 = self.get_properties()['start_date']
                start_date_1_epoch = self.strptime_to_timestamp(start_date_1)
                start_date_2 = self.get_properties(original=False)['start_date']
                start_date_2_epoch = self.strptime_to_timestamp(start_date_2)

                expected_primary_keys = self.expected_primary_keys()[stream]

                primary_keys_list_1 = [tuple(message.get('data').get(expected_pk) for expected_pk in expected_primary_keys)
                                       for message in first_sync_records.get(stream, {}).get('messages', {})
                                       if message.get('action') == 'upsert']
                primary_keys_list_2 = [tuple(message.get('data').get(expected_pk) for expected_pk in expected_primary_keys)
                                       for message in second_sync_records.get(stream, {}).get('messages', {})
                                       if message.get('action') == 'upsert']
                primary_keys_sync_1 = set(primary_keys_list_1)
                primary_keys_sync_2 = set(primary_keys_list_2)

                # Verify by primary key the records replicated in the 2nd sync are part of the 1st sync
                self.assertTrue(primary_keys_sync_2.issubset(primary_keys_sync_1),
                                msg="Records in the 2nd sync are not a subset of the 1st sync")

                # Verify by stream that our first sync meets or exceeds the default record count
                self.assertLessEqual(expected_first_sync_count, first_sync_count)

                # Verify by stream more records were replicated in the first sync, with an older start_date than the second
                self.assertGreaterEqual(first_sync_count, second_sync_count)

                # Verify by stream that all records have a rep key that is equal to or greater than that sync's start_date
                for message in first_sync_messages:
                    if self.is_report_stream(stream):
                        rep_key_value = message.get('data').get('ReportDate')
                    else :
                        rep_key_value = message.get('data').get('MetaData').get('LastUpdatedTime')
                    self.assertGreaterEqual(self.strptime_to_timestamp(rep_key_value), start_date_1_epoch,
                                            msg="A record was replicated with a replication key value prior to the start date")
                for message in second_sync_messages:
                    if self.is_report_stream(stream):
                        rep_key_value = message.get('data').get('ReportDate')
                    else :
                        rep_key_value = message.get('data').get('MetaData').get('LastUpdatedTime')
                    self.assertGreaterEqual(self.strptime_to_timestamp(rep_key_value), start_date_2_epoch,
                                            msg="A record was replicated with a replication key value prior to the start date")

class TestQuickbooksBudgetStartDate(TestQuickbooksStartDate):
    def expected_streams(self):
        """All streams are under test"""
        # Skipping stream deleted_objects due to data unavailability
        return {'budgets'}

    def get_properties(self, original=True):
        if original:
            return {
                'start_date' : '2016-06-02T00:00:00Z',
                'sandbox': 'true'
            }
        else:
            return {
                'start_date' : '2025-04-04T00:00:00Z',
                'sandbox': 'true'
            }
