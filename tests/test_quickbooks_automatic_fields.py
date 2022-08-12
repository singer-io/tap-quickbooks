import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
import re

from base import TestQuickbooksBase

page_size_key = 'max_results'

class TestQuickbooksAutomaticFields(TestQuickbooksBase):

    @staticmethod
    def get_selected_fields_from_metadata(metadata):
        selected_fields = set()
        for field in metadata:
            is_field_metadata = len(field['breadcrumb']) > 1
            inclusion_automatic_or_selected = (field['metadata']['inclusion'] == 'automatic'
                                               or field['metadata']['selected'] is True)
            if is_field_metadata and inclusion_automatic_or_selected:
                selected_fields.add(field['breadcrumb'][1])
        return selected_fields

    def get_properties(self):
        return {
            'start_date' : '2016-06-02T00:00:00Z',
            'sandbox': 'true',
            page_size_key: '10'
        }

    def expected_streams(self):
        """
        All streams are under test with the exception of 'budgets' which
        has a workaround to skip the invalid assertion. See if block in
        test_run for details
        """
        return self.expected_check_streams()


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
        self.select_all_streams_and_fields(conn_id, catalog_entries, select_all_fields=False)

        # Verify our selection worked as expected
        catalogs_selection = menagerie.get_catalogs(conn_id)
        for cat in catalogs_selection:
            catalog_entry = menagerie.get_annotated_schema(conn_id, cat['stream_id'])

            # Verify the expected stream tables are selected
            selected = catalog_entry.get('annotated-schema').get('selected')
            print("Validating selection on {}: {}".format(cat['stream_name'], selected))
            if cat['stream_name'] not in expected_streams:
                self.assertFalse(selected, msg="Stream selected, but not testable.")
                continue # Skip remaining assertions if we aren't selecting this stream
            self.assertTrue(selected, msg="Stream not selected.")

            # Verify only automatic fields are selected
            expected_automatic_fields = self.expected_automatic_fields().get(cat['tap_stream_id'])
            selected_fields = self.get_selected_fields_from_metadata(catalog_entry['metadata'])
            self.assertEqual(expected_automatic_fields, selected_fields)

        # Run a sync job using orchestrator
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Get records that reached the target
        sync_record_count = runner.examine_target_output_file(
            self, conn_id, self.expected_streams(), self.expected_primary_keys())
        synced_records = runner.get_records_from_target_output()

        # Assert the records for each stream
        for stream in self.expected_streams():
            with self.subTest(stream=stream):
                data = synced_records.get(stream)
                record_messages_keys = [set(row['data'].keys()) for row in data['messages']]
                expected_keys = self.expected_automatic_fields().get(stream)

                expected_count = self.minimum_record_count_by_stream().get(stream)
                record_count = sync_record_count.get(stream, 0)

                # Verify that only the automatic fields are sent to the target
                for actual_keys in record_messages_keys:
                    self.assertEqual(
                        actual_keys.symmetric_difference(expected_keys), set(),
                        msg="Expected automatic fields and nothing else.")

                # Verify the sync meets or exceeds the default record count
                self.assertLessEqual(expected_count, record_count)

                if stream == 'budgets': # Skip the pagination assertion for this stream
                    # This stream returns a single record of the current budget state
                    # and will never exceed our pagination size (max_results) in this test
                    # so we can verify auto fields works, but only for 1 page of data
                    continue

                # Verify the number or records exceeds the max_results (api limit)
                pagination_threshold = int(self.get_properties().get(page_size_key))
                self.assertGreater(record_count, pagination_threshold,
                                   msg="Record count not large enough to gaurantee pagination.")
