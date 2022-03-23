from tap_tester import runner, menagerie
from base import TestQuickbooksBase

class TestQuickbooksAllFields(TestQuickbooksBase):

    # fields which are deprecated or data is not generated
    fields_to_remove = {
        'time_activities': [
            'BreakHours BreakMinutes',
            'patternProperties',
            'TransactionLocationType',
            'PayrollItemRef'
        ],
        'sales_receipts': [
            'patternProperties',
            'TxnSource',
            'RecurDataRef',
            'CreditCardPayment',
            'ShipFromAddr',
            'HomeBalance',
            'TransactionLocationType',
            'ClassRef',
            'ApplyTaxAfterDiscount',
            'ExchangeRate',
            'HomeTotalAmt',
            'LinkedTxn'
        ],
        'vendors': [
            'patternProperties',
            'TaxReportingBasis',
            'APAccountRef',
            'HasTPAR',
            'BillRate',
            'VendorPaymentBankDetail',
            'OtherContactInfo',
            'GSTRegistrationType',
            'GSTIN'
        ],
        'journal_entries': [
            'TaxRateRef',
            'JournalCodeRef',
            'RecurDataRef',
            'TotalAmt',
            'GlobalTaxCalculation',
            'TransactionLocationType',
            'HomeTotalAmt',
            'ExchangeRate'
        ],
        'purchases': [
            'patternProperties',
            'TxnSource',
            'RecurDataRef',
            'LinkedTxn',
            'Credit',
            'TransactionLocationType',
            'IncludeInAnnualTPAR',
            'ExchangeRate',
            'DocNumber'
        ],
        'tax_codes': [
            'TaxCodeConfigType',
            'Hidden'
        ],
        'bill_payments': [
            'DepartmentRef',
            'CreditCardPayment',
            'APAccountRef',
            'ProcessBillPayment',
            'TransactionLocationType',
            'ExchangeRate'
        ],
        'departments': [
            'ParentRef'
        ],
        'employees': [
            'Organization',
            'V4IDPseudonym'
        ],
        'vendor_credits': [
            'RecurDataRef',
            'TransactionLocationType',
            'Balance',
            'IncludeInAnnualTPAR',
            'ExchangeRate',
            'LinkedTxn'
        ],
        'refund_receipts': [
            'patternProperties',
            'TxnSource',
            'DepartmentRef',
            'RecurDataRef',
            'CreditCardPayment',
            'PaymentType',
            'HomeBalance',
            'ShipAddr',
            'TransactionLocationType',
            'CheckPayment',
            'ApplyTaxAfterDiscount',
            'ClassRef',
            'ExchangeRate',
            'HomeTotalAmt',
            'TaxExemptionRef'
        ],
        'tax_agencies': [
            'LastFileDate',
            'TaxAgencyConfig'
        ],
        'payments': [
            'TxnSource',
            'CreditCardPayment',
            'TransactionLocationType',
            'ARAccountRef',
            'ExchangeRate',
            'TaxExemptionRef'
        ],
        'deposits': [
            'TxnSource',
            'RecurDataRef',
            'TxnTaxDetail',
            'GlobalTaxCalculation',
            'TransactionLocationType',
            'HomeTotalAmt',
            'ExchangeRate'
        ],
        'items': [
            'patternProperties',
            'QtyOnHand',
            'ReverseChargeRate',
            'UQCId',
            'PrefVendorRef',
            'AbatementRate',
            'AssetAccountRef',
            'TaxClassificationRef',
            'Sku',
            'ClassRef',
            'UQCDisplayText',
            'InvStartDate',
            'ReorderPoint',
            'ServiceType',
            'ItemCategoryType'
        ],
        'purchase_orders': [
            'ShipTo',
            'SalesTermRef',
            'RecurDataRef',
            'EmailStatus',
            'TransactionLocationType',
            'DueDate',
            'POEmail',
            'ClassRef',
            'ExchangeRate'
        ],
        'bills': [
            'DepartmentRef',
            'RecurDataRef',
            'HomeBalance',
            'TransactionLocationType',
            'IncludeInAnnualTPAR',
            'ExchangeRate'
        ],
        'accounts': [
            'AcctNum',
            'AccountAlias',
            'TaxCodeRef',
            'TxnLocationType'
        ],
        'credit_memos': [
            'SalesTermRef',
            'patternProperties',
            'DepartmentRef',
            'RecurDataRef',
            'PaymentMethodRef',
            'HomeBalance',
            'ApplyTaxAfterDiscount',
            'TransactionLocationType',
            'ClassRef',
            'HomeTotalAmt',
            'InvoiceRef',
            'ExchangeRate',
            'TaxExemptionRef'
        ],
        'customers': [
            'BusinessNumber',
            'DefaultTaxCodeRef',
            'IsProject',
            'SecondaryTaxIdentifier',
            'PrimaryTaxIdentifier',
            'OpenBalanceDate',
            'ARAccountRef',
            'CustomerTypeRef',
            'GSTRegistrationType',
            'ResaleNum',
            'TaxExemptionReasonId',
            'GSTIN'
        ],
        'terms': [
            'DiscountDayOfMonth',
            'DiscountPercent'
        ],
        'transfers': [
            'TransactionLocationType',
            'ExchangeRate',
            'RecurDataRef'
        ],
        'estimates': [
            'SalesTermRef',
            'patternProperties',
            'RecurDataRef',
            'ShipFromAddr',
            'AcceptedDate',
            'TransactionLocationType',
            'DueDate',
            'ClassRef',
            'ApplyTaxAfterDiscount',
            'AcceptedBy',
            'ExchangeRate',
            'HomeTotalAmt',
            'LinkedTxn',
            'TaxExemptionRef'
        ],
        'invoices': [
            'RecurDataRef',
            'ApplyTaxAfterDiscount',
            'ExchangeRate',
            'HomeTotalAmt',
            'patternProperties',
            'FreeFormAddress',
            'BillEmailCc',
            'InvoiceLink',
            'ClassRef',
            'TaxExemptionRef',
            'DepositToAccountRef',
            'TxnSource',
            'Deposit',
            'HomeBalance',
            'TransactionLocationType',
            'ShipFromAddr',
            'BillEmailBcc',
        ]
    }

    def name(self):
        return "tap_tester_quickbooks_combined_test"

    def test_run(self):
        """
        Testing that all fields mentioned in the catalog are synced from the tap
        - Verify no unexpected streams were replicated
        - Verify that more than just the automatic fields are replicated for each stream
        """
        expected_streams = self.expected_check_streams()

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
                fields = self.fields_to_remove.get(stream) or []
                for field in fields:
                    expected_all_keys.remove(field)

                self.assertSetEqual(expected_all_keys, actual_all_keys)
