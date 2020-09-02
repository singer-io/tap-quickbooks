import datetime
import dateutil.parser
import pytz

import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

from base import TestQuickbooksBase


class TestQuickbooksBookmarks(TestQuickbooksBase):
    def name(self):
        return "tap_tester_quickbooks_combined_test"

    def expected_streams(self):
        return self.expected_check_streams().difference({
            'budgets'
        })

    def convert_state_to_utc(self, date_str):
        """
        Convert a saved bookmark value of the form '2020-08-25T13:17:36-07:00' to
        a string formatted utc datetime,
        in order to compare aginast json formatted datetime values
        """
        date_object = dateutil.parser.parse(date_str)
        date_object_utc = date_object.astimezone(tz=pytz.UTC)
        return datetime.datetime.strftime(date_object_utc, "%Y-%m-%dT%H:%M:%SZ")

    def calculated_states_by_stream(self, current_state):
        """
        Look at the bookmarks from a previous sync and set a new bookmark
        value that is 1 day prior. This ensures the subsequent sync will replicate
        at least 1 record but, fewer records than the previous sync.
        """
        stream_to_current_state = {stream : bookmark.get('LastUpdatedTime')
                           for stream, bookmark in current_state['bookmarks'].items()}
        stream_to_calculated_state = {stream: "" for stream in self.expected_streams()}

        for stream, state in stream_to_current_state.items():
            # convert state from string to datetime object
            state_as_datetime = dateutil.parser.parse(state)
            # subtract 1 day from the state
            calculated_state_as_datetime = state_as_datetime - datetime.timedelta(days=1)
            # convert back to string and format
            calculated_state = str(calculated_state_as_datetime).replace(' ', 'T')
            stream_to_calculated_state[stream] = calculated_state

        return stream_to_calculated_state


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
        first_sync_bookmarks = menagerie.get_state(conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # UPDATE STATE BETWEEN SYNCS
        new_state = dict()
        # new_state['bookmarks'] = {key: {'LastUpdatedTime': value} for key, value in self.simulated_states_by_stream().items()}
        new_state['bookmarks'] = {key: {'LastUpdatedTime': value}
                                  for key, value in self.calculated_states_by_stream(first_sync_bookmarks).items()}
        menagerie.set_state(conn_id, new_state)

        # SYNC 2
        sync_job_name = runner.run_sync_mode(self, conn_id)
        second_sync_records = runner.get_records_from_target_output()
        second_sync_record_count = runner.examine_target_output_file(
            self, conn_id, self.expected_streams(), self.expected_primary_keys())
        second_sync_bookmarks = menagerie.get_state(conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Test by stream
        for stream in self.expected_streams():
            with self.subTest(stream=stream):
                # record counts
                first_sync_count = first_sync_record_count.get(stream, 0)
                second_sync_count = second_sync_record_count.get(stream, 0)

                # record messages
                first_sync_messages = first_sync_records.get(stream, {'messages': []}).get('messages')
                second_sync_messages = second_sync_records.get(stream, {'messages': []}).get('messages')

                # replication key is an object (MetaData.LastUpdatedTime) in sync records
                # but just the sub level replication key is used in setting bookmarks
                top_level_replication_key = 'MetaData'
                sub_level_replication_key = 'LastUpdatedTime'

                # bookmarked states (top level objects)
                first_bookmark_key_value = first_sync_bookmarks.get('bookmarks').get(stream)
                second_bookmark_key_value = second_sync_bookmarks.get('bookmarks').get(stream)

                # Verify the first sync sets a bookmark of the expected form
                self.assertIsNotNone(first_bookmark_key_value)
                self.assertIsNotNone(first_bookmark_key_value.get(sub_level_replication_key))

                # Verify the second sync sets a bookmark of the expected form
                self.assertIsNotNone(second_bookmark_key_value)
                self.assertIsNotNone(second_bookmark_key_value.get(sub_level_replication_key))

                # bookmarked states (actual values)
                first_bookmark_value = first_bookmark_key_value.get(sub_level_replication_key)
                second_bookmark_value = second_bookmark_key_value.get(sub_level_replication_key)
                # bookmarked values as utc for comparing against records
                first_bookmark_value_utc = self.convert_state_to_utc(first_bookmark_value)
                second_bookmark_value_utc = self.convert_state_to_utc(second_bookmark_value)

                # Verify the second sync bookmark is Equal to the first sync bookmark
                self.assertEqual(second_bookmark_value, first_bookmark_value) # assumes no changes to data during test

                # Verify the second sync records respect the previous (simulated) bookmark value
                simulated_bookmark_value = new_state['bookmarks'][stream][sub_level_replication_key]
                for message in second_sync_messages:
                    replication_key_value = message.get('data').get(top_level_replication_key).get(sub_level_replication_key)
                    self.assertGreaterEqual(replication_key_value, simulated_bookmark_value,
                                            msg="Second sync records do not repect the previous bookmark.")

                # Verify the first sync bookmark value is the max replication key value for a given stream
                for message in first_sync_messages:
                    replication_key_value = message.get('data').get(top_level_replication_key).get(sub_level_replication_key)
                    self.assertLessEqual(replication_key_value, first_bookmark_value_utc,
                                         msg="First sync bookmark was set incorrectly, a record with a greater rep key value was synced")

                # Verify the second sync bookmark value is the max replication key value for a given stream
                for message in second_sync_messages:
                    replication_key_value = message.get('data').get(top_level_replication_key).get(sub_level_replication_key)
                    self.assertLessEqual(replication_key_value, second_bookmark_value_utc,
                                         msg="Second sync bookmark was set incorrectly, a record with a greater rep key value was synced")

                # Verify the number of records in the 2nd sync is less then the first
                self.assertLess(second_sync_count, first_sync_count)

                # Verify at least 1 record was replicated in the second sync
                self.assertGreater(second_sync_count, 0, msg="We are not fully testing bookmarking for {}".format(stream))
