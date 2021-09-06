"""
Test that the start_date is respected for some streams
"""
import datetime
import dateutil.parser
import pytz
import singer
import tap_tester.connections as connections
import tap_tester.menagerie as menagerie
import tap_tester.runner as runner

from base import TestQuickbooksBase


class TestFutureDate(TestQuickbooksBase):

    def name(self):
        return "tap_tester_quickbooks_combined_test"

    def expected_streams(self):
        return self.expected_check_streams().difference({
            'budgets'
        })

    def calculated_states_by_stream(self):
        """
        Increment today's date by 1 day and set it as the current state
        """
        state_streams = self.expected_streams()
        stream_to_calculated_state = {
            stream: "" for stream in self.expected_streams()}
        for state_stream in state_streams:
            calculated_state = datetime.datetime.strftime(
                datetime.datetime.today() + datetime.timedelta(days=1), "%Y-%m-%dT00:00:00Z")
            stream_to_calculated_state[state_stream] = calculated_state
        return stream_to_calculated_state

    def test_run(self):
        # SYNC
        conn_id = self.ensure_connection()

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

        # # add next day as state for all streams
        state_streams = self.expected_streams()

        new_state = dict()
        new_state['bookmarks'] = {key: {'LastUpdatedTime': value}
                                  for key, value in self.calculated_states_by_stream().items()}
        menagerie.set_state(conn_id, new_state)

        runner.run_sync_mode(self, conn_id)
        runner.get_records_from_target_output()
        runner.examine_target_output_file(
            self, conn_id, self.expected_streams(), self.expected_primary_keys())
        # second_sync_bookmarks = menagerie.get_state(conn_id)

        # get the state after running sync mode
        latest_state = menagerie.get_state(conn_id)

        # verify the child streams have the state in latest state
        for stream in state_streams:
            self.assertIsNone(latest_state.get(stream))
