import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
import re

from base import TestQuickbooksBase


class TestQuickbooksBookmarks(TestQuickbooksBase):
    def name(self):
        return "tap_tester_quickbooks_combined_test"


    def expected_replication_key_values(self):
        md_rep_keys = self.metadata_level_rep_keys()


    def expected_streams(self):
        return {'accounts'}


    def simulated_states_by_stream(self):
        return {'accounts': '2020-08-25T13:17:36-07:00'}


    def test_run(self):
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
            self, conn_id, self.expected_streams(), self.expected_primary_keys())
        first_sync_state = menagerie.get_state(conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # UPDATE STATE BETWEEN SYNCS
        new_state = dict()
        new_state['bookmarks'] = {key: {'LastUpdatedTime': value} for key, value in self.simulated_states_by_stream().items()}
        menagerie.set_state(conn_id, new_state)

        # SYNC 2
        sync_job_name = runner.run_sync_mode(self, conn_id)
        second_sync_records = runner.get_records_from_target_output()
        second_sync_record_count = runner.examine_target_output_file(
            self, conn_id, self.expected_streams(), self.expected_primary_keys())
        second_sync_state = menagerie.get_state(conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Test by stream
        for stream in self.expected_streams():
            with self.subTest(stream=stream):
                # record counts
                first_sync_count = first_sync_record_count.get(stream, 0)
                expected_first_sync_count = self.minimum_record_count_by_stream().get(stream) # TODO drop?
                second_sync_count = second_sync_record_count.get(stream, 0)

                # record messages
                first_sync_messages = first_sync_records.get(stream, {'messages': []}).get('messages')
                second_sync_messages = second_sync_records.get(stream, {'messages': []}).get('messages')

                # replication key is an object (MetaData.LastUpdatedTime) in sync records
                # but just the sub level pk is used in setting sync state
                top_level_rk = 'MetaData'
                sub_level_rk = 'LastUpdatedTime'

                # sync states (top level objects)
                first_state_key_value = first_sync_state.get('bookmarks').get(stream)
                second_state_key_value = second_sync_state.get('bookmarks').get(stream)

                # Verify the first sync sets a state value of the expected form
                self.assertIsNotNone(first_state_key_value)
                self.assertIsNotNone(first_state_key_value.get(sub_level_rk))

                # Verify the second sync sets a state value of the expected form
                self.assertIsNotNone(second_state_key_value)
                self.assertIsNotNone(second_state_key_value.get(sub_level_rk))

                # sync states (values)
                first_state_value = first_state_key_value.get(sub_level_rk)
                second_state_value = second_state_key_value.get(sub_level_rk)

                # Verify the second sync state is Equal to the first sync state value
                self.assertEqual(second_state_value, first_state_value)

                # Verify the second sync records respect the previous (simulated) bookmark value
                simulated_state_value = new_state['bookmarks'][stream][sub_level_rk]
                for message in second_sync_messages:
                    rk_value = message.get('data').get(top_level_rk).get(sub_level_rk)
                    self.assertGreaterEqual(rk_value, simulated_state_value,
                                            msg="Second sync records do not repect the previous bookmark.")

                # BUG | https://stitchdata.atlassian.net/browse/SRCE-3821
                # Verify the first sync bookmark value is the max replication key value for a given stream
                # for message in first_sync_messages:
                #     rk_value = message.get('data').get(top_level_rk).get(sub_level_rk)
                #     self.assertLessEqual(rk_value, first_state_value,
                #                          msg="First sync state was set incorrectly, a record with a greater rep key value was synced")

                # BUG | https://stitchdata.atlassian.net/browse/SRCE-3821
                # Verify the second sync bookmark value is the max replication key value for a given stream
                # for message in second_sync_messages:
                #     rk_value = message.get('data').get(top_level_rk).get(sub_level_rk)
                #     self.assertLessEqual(rk_value, second_state_value,
                #                          msg="Second sync state was set incorrectly, a record with a greater rep key value was synced")

                # Verify the number of records in the 2nd sync is less then the first
                self.assertLessEqual(second_sync_count, first_sync_count) # TODO can this be assertLess

                # Verify at least 1 record was replicated in the second sync
                self.assertGreater(0, second_sync_count, msg="We are not fully testing bookmarking for {}".format(stream))
