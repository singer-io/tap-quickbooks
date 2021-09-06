"""
Test that the start_date is respected for some streams
"""
import datetime
import dateutil.parser
import pytz

import tap_tester.connections as connections
import tap_tester.menagerie as menagerie
import tap_tester.runner as runner

from base import TestQuickbooksBase


class TestStartDateAndBookmark(TestQuickbooksBase):

    def name(self):
        return "tap_tester_quickbooks_combined_test"

    def expected_streams(self):
        return self.expected_check_streams().difference({
            'budgets'
        })

    def get_properties(self, original=True):
        if original:
            return {
                'start_date': '2016-06-02T00:00:00Z',
                'sandbox': 'true'
            }

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
        stream_to_current_state = {stream: bookmark.get('LastUpdatedTime')
                                   for stream, bookmark in current_state['bookmarks'].items()}
        stream_to_calculated_state = {
            stream: "" for stream in self.expected_streams()}

        for stream, state in stream_to_current_state.items():
            # convert state from string to datetime object
            state_as_datetime = dateutil.parser.parse(state)
            # subtract 1 day from the state
            calculated_state_as_datetime = state_as_datetime - \
                datetime.timedelta(days=1)
            # convert back to string and format
            calculated_state = str(
                calculated_state_as_datetime).replace(' ', 'T')
            stream_to_calculated_state[stream] = calculated_state

        return stream_to_calculated_state

    def test_run(self):
        # SYNC
        conn_id = self.ensure_connection()

        # Run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # Verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        state = {}

        # Run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # Verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(
            found_catalogs), 0, msg="unable to locate schemas for connection {}".format(conn_id))

        # Select only the expected streams tables
        expected_streams = self.expected_streams()
        catalog_entries = [
            ce for ce in found_catalogs if ce['tap_stream_id'] in expected_streams]
        self.select_all_streams_and_fields(conn_id, catalog_entries)

        catalog_entries = [
            ce for ce in found_catalogs if ce['tap_stream_id'] in expected_streams]
        self.select_all_streams_and_fields(conn_id, catalog_entries)

        state_streams = self.expected_streams()
        for state_stream in state_streams:
            state[state_stream] = datetime.datetime.strftime(
                datetime.datetime.today() + datetime.timedelta(days=1), "%Y-%m-%dT00:00:00Z")

        menagerie.set_state(conn_id, state)

        # UPDATE STATE BETWEEN SYNCS
        new_state = dict()
        original_bookmark_value = '2021-06-01T00:00:00Z'
        new_state['bookmarks'] = {stream: {'LastUpdatedTime': original_bookmark_value}
                                  for stream in expected_streams}
        menagerie.set_state(conn_id, new_state)

        sync_job_name = runner.run_sync_mode(self, conn_id)
        sync_records = runner.get_records_from_target_output()
        sync_record_count = runner.examine_target_output_file(
            self, conn_id, self.expected_streams(), self.expected_primary_keys())
        sync_bookmarks = menagerie.get_state(conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        for stream in self.expected_streams():
            with self.subTest(stream=stream):
                # record messages
                sync_messages = sync_records.get(
                    stream, {'messages': []}).get('messages')

                # replication key is an object (MetaData.LastUpdatedTime) in sync records
                # but just the sub level replication key is used in setting bookmarks
                top_level_replication_key = 'MetaData'
                sub_level_replication_key = 'LastUpdatedTime'

                # bookmarked states (top level objects)
                bookmark_key_value = sync_bookmarks.get(
                    'bookmarks').get(stream)

                # Verify the first sync bookmark value is the max replication key value for a given stream
                for message in sync_messages:
                    replication_key_value = message.get('data').get(
                        top_level_replication_key).get(sub_level_replication_key)
                    self.assertLess(original_bookmark_value, replication_key_value,
                                    msg="Record with lesser replication key than bookmark was found.")
