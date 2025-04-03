import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
import re

from base import TestQuickbooksBase
from tap_tester.logger import LOGGER

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

    def test_run(self):
        """
        - Verify we can deselect all fields except when inclusion=automatic, which is handled by base.py methods
        - Verify that only the automatic fields are sent to the target.
        - Verify that all replicated records have unique primary key values.
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
        # Skipping stream deleted_objects due to data unavailability
        expected_streams = self.expected_check_streams() - {'deleted_objects'}
        catalog_entries = [ce for ce in found_catalogs if ce['tap_stream_id'] in expected_streams]
        self.select_all_streams_and_fields(conn_id, catalog_entries, False)

        # Verify our selection worked as expected
        catalogs_selection = menagerie.get_catalogs(conn_id)
        for cat in catalogs_selection:
            catalog_entry = menagerie.get_annotated_schema(conn_id, cat['stream_id'])

            # Verify the expected stream tables are selected
            selected = catalog_entry.get('annotated-schema').get('selected')
            LOGGER.info(f"Validating selection on {cat['stream_name']}: {selected}")
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
            self, conn_id, expected_streams, self.expected_primary_keys())
        synced_records = runner.get_records_from_target_output()

        # Assert the records for each stream
        for stream in expected_streams:
            with self.subTest(stream=stream):
                expected_primary_keys = self.expected_primary_keys()[stream]
                expected_keys = self.expected_automatic_fields().get(stream)

                data = synced_records.get(stream,{})
                record_messages_keys = [set(row.get('data').keys()) for row in data.get('messages',{})]

                primary_keys_list = [tuple(message.get('data', {}).get(expected_pk) for expected_pk in expected_primary_keys)
                                    for message in data.get('messages', [])
                                    if message.get('action') == 'upsert']
                unique_primary_keys_list = set(primary_keys_list)

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

                # Verify that all replicated records have unique primary key values.
                self.assertEqual(len(primary_keys_list),
                                len(unique_primary_keys_list),
                                msg="Replicated record does not have unique primary key values.")
