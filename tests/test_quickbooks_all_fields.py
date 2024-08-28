from tap_tester import runner, menagerie
from base import TestQuickbooksBase

class TestQuickbooksAllFields(TestQuickbooksBase):
    """Test case to verify we are replicating all fields data from the Tap"""

    # remove fields that are replicated when you have account for that specific region
    locale_fields = {
        'transfers': [
            'TransactionLocationType', # FRANCE locale field
        ],
        'journal_entries': [
            'GlobalTaxCalculation', # AUSTRALIA locale field
            'TransactionLocationType', # FRANCE locale field
            'JournalCodeRef' # FRANCE locale field
        ],
        'refund_receipts': [
            'patternProperties', # already added in the schema but not found in the API doc
            'GlobalTaxCalculation', # AUSTRALIA, UK, CANADA, INDIA locale field
            'TransactionLocationType', # FRANCE locale field
        ],
        'purchases': [
            'IncludeInAnnualTPAR', # AUSTRALIA locale field
            'patternProperties', # already added in the schema but not found in the API doc
            'GlobalTaxCalculation', # AUSTRALIA, UK, CANADA, INDIA locale field
            'TransactionLocationType', # FRANCE locale field
        ],
        'deposits': [
            'GlobalTaxCalculation', # AUSTRALIA, UK, INDIA, CANADA locale field
            'TransactionLocationType', # FRANCE locale field
        ],
        'vendors': [
            'GSTRegistrationType', # INDIA locale field
            'TaxReportingBasis', # FRANCE locale field
            'VendorPaymentBankDetail', # AUSTRALIA locale field
            'patternProperties', # already added in the schema but not found in the API doc
            'HasTPAR', # AUSTRALIA locale field
            'APAccountRef', # FRANCE locale field
            'BusinessNumber', # INDIA locale field
            'GSTIN', # INDIA locale field
            'T4AEligible', # CANADA locale field
            'T5018Eligible', # CANADA locale field
        ],
        'items': [
            'ServiceType', # INDIA locale field
            'patternProperties', # already added in the schema but not found in the API doc
            'ReverseChargeRate', # INDIA locale field
            'AbatementRate', # INDIA locale field
            'ItemCategoryType', # FRANCE locale field
            'UQCId', # INDIA locale field
            'UQCDisplayText', # INDIA locale field
        ],
        'customers': [
            'GSTRegistrationType', # INDIA locale field
            'ARAccountRef', # FRANCE locale field
            'BusinessNumber', # INDIA locale field
            'PrimaryTaxIdentifier', # INDIA, CANADA, UK, AUSTRALIA locale field
            'GSTIN', # INDIA locale field
            'SecondaryTaxIdentifier', # INDIA, UK locale field
        ],
        'bill_payments': [
            'APAccountRef', # FRANCE locale field
            'TransactionLocationType', # FRANCE locale field
        ],
        'tax_rates': [
            'OriginalTaxRate' # CANADA locale field
        ],
        'bills': [
            'IncludeInAnnualTPAR', # AUSTRALIA locale field
            'TransactionLocationType', # FRANCE locale field
            'TxnTaxDetail', # INDIA, CANADA, UK, AUSTRALIA locale field
            'GlobalTaxCalculation' # INDIA, CANADA, UK, AUSTRALIA locale field
        ],
        'invoices': [
            'patternProperties', # already added in the schema but not found in the API doc
            'GlobalTaxCalculation', # AUSTRALIA locale field
            'TransactionLocationType', # FRANCE locale field
        ],
        'sales_receipts': [
            'patternProperties', # already added in the schema but not found in the API doc
            'GlobalTaxCalculation', # AUSTRALIA locale field
            'TransactionLocationType', # FRANCE locale field
        ],
        'tax_agencies': [
            'LastFileDate' # INDIA, CANADA, UK, AUSTRALIA, FRANCE locale field
        ],
        'credit_memos': [
            'patternProperties', # already added in the schema but not found in the API doc
            'InvoiceRef', # INDIA locale field
            'GlobalTaxCalculation', # AUSTRALIA locale field
            'TransactionLocationType', # FRANCE locale field
        ],
        'accounts': [
            'TxnLocationType', # FRANCE locale field
            'TaxCodeRef', # INDIA, CANADA, UK, AUSTRALIA locale field
            'AccountAlias' # FRANCE locale field
        ],
        'payments': [
            'ARAccountRef', # FRANCE locale field
            'TransactionLocationType', # FRANCE locale field
        ],
        'purchase_orders': [
            'GlobalTaxCalculation', # AUSTRALIA, UK, INDIA, CANADA locale field
            'TransactionLocationType', # FRANCE locale field
        ],
        'estimates': [
            'patternProperties', # already added in the schema but not found in the API doc
            'GlobalTaxCalculation', # AUSTRALIA, UK, INDIA, CANADA locale field
            'TransactionLocationType', # FRANCE locale field
        ],
        'vendor_credits': [
            'IncludeInAnnualTPAR', # AUSTRALIA locale field
            'GlobalTaxCalculation', # AUSTRALIA, UK, INDIA, CANADA locale field
            'TransactionLocationType', # FRANCE locale field
        ],
        'time_activities': [
            'patternProperties', # already added in the schema but not found in the API doc
            'TransactionLocationType', # FRANCE locale field
        ]
    }

    # fields for which data is not generated
    fields_to_remove = {
        'items': [
            'PurchaseTaxCodeRef', 'SalesTaxCodeRef', 'SalesTaxIncluded', 'PurchaseTaxIncluded'
        ],
        'purchase_orders': [
            'ShipTo', 'DueDate', 'SalesTermRef', 'ClassRef', 'TxnTaxDetail'
        ],
        'deposits': [
            'RecurDataRef', 'TxnSource',
        ],
        'journal_entries': [
            'RecurDataRef', 'TaxRateRef'
        ],
        'tax_rates': [
            'EffectiveTaxRate'
        ],
        'refund_receipts': [
            'RecurDataRef', 'CheckPayment', 'PaymentType', 'ShipAddr'
        ],
        'terms': [
            'DiscountDayOfMonth'
        ],
        'vendor_credits': [
            'RecurDataRef'
        ],
        'tax_agencies': [
            'TaxRegistrationNumber'
        ],
        'estimates': [
            'RecurDataRef', 'DueDate', 'SalesTermRef'
        ],
        'vendors': [
            'OtherContactInfo'
        ],
        'payments': [
            'CreditCardPayment', 'TaxExemptionRef', 'TxnSource'
        ],
        'sales_receipts': [
            'TxnSource'
        ],
        'bill_payments': [
            'ProcessBillPayment', 'PrivateNote'
        ],
        'customers': [
            'OpenBalanceDate'
        ],
        'purchases': [
            'RecurDataRef', 'TxnTaxDetail', 'TxnSource'
        ],
        'credit_memos': [
            'RecurDataRef', 'PaymentMethodRef', 'SalesTermRef'
        ],
        'invoices': [
            'InvoiceLink', 'TxnSource'
        ],
        'employees': [
            'Organization'
        ]
    }

    def test_run(self):
        """
        Testing that all fields mentioned in the catalog are synced from the tap
        - Verify no unexpected streams were replicated
        - Verify that more than just the automatic fields are replicated for each stream
        """
        # Skipping stream deleted_objects due to data unavailability
        expected_streams = self.expected_check_streams() - {'deleted_objects'}

        # instantiate connection
        conn_id = self.ensure_connection()

        # run check mode
        check_job_name = runner.run_check_mode(self, conn_id)
        # Verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        # get catalog
        found_catalogs = menagerie.get_catalogs(conn_id)
        catalog_entries = [ce for ce in found_catalogs if ce['tap_stream_id'] in expected_streams]
        # perform table and field selection
        self.select_all_streams_and_fields(conn_id, catalog_entries)

        # grab metadata after performing table-and-field selection to set expectations
        stream_to_all_catalog_fields = dict() # used for asserting all fields are replicated
        for catalog in found_catalogs:
            stream_id, stream_name = catalog['stream_id'], catalog['stream_name']
            catalog_entry = menagerie.get_annotated_schema(conn_id, stream_id)
            fields_from_field_level_md = [md_entry['breadcrumb'][1] for md_entry in catalog_entry['metadata']
                                          if md_entry['breadcrumb'] != []]
            stream_to_all_catalog_fields[stream_name] = set(fields_from_field_level_md)

        # Run a sync job using orchestrator
        sync_job_name = runner.run_sync_mode(self, conn_id)
        synced_records = runner.get_records_from_target_output()
        record_count_by_stream = runner.examine_target_output_file(
            self, conn_id, expected_streams, self.expected_primary_keys())

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Verify no unexpected streams were replicated
        synced_stream_names = set(synced_records.keys())
        self.assertSetEqual(expected_streams, synced_stream_names)

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # expected values
                expected_automatic_keys = self.expected_primary_keys()[stream] | self.expected_replication_keys()[stream]

                # get all expected keys
                expected_all_keys = stream_to_all_catalog_fields[stream]

                # collect actual values
                messages = synced_records.get(stream)

                actual_all_keys = set()
                # collect actual values
                for message in messages['messages']:
                    if message['action'] == 'upsert':
                        actual_all_keys.update(message['data'].keys())

                # Verify that you get some records for each stream
                self.assertGreater(record_count_by_stream.get(stream, -1), 0)

                # verify all fields for a stream were replicated
                self.assertGreater(len(expected_all_keys), len(expected_automatic_keys))
                self.assertTrue(expected_automatic_keys.issubset(expected_all_keys), msg=f'{expected_automatic_keys-expected_all_keys} is not in "expected_all_keys"')

                # remove some fields as data cannot be generated / retrieved
                fields = self.fields_to_remove.get(stream, []) + self.locale_fields.get(stream, [])
                for field in fields:
                    expected_all_keys.remove(field)

                self.assertSetEqual(expected_all_keys, actual_all_keys)
