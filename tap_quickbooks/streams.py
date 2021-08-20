import tap_quickbooks.query_builder as query_builder

import singer

class Stream:
    endpoint = '/v3/company/{realm_id}/query'
    key_properties = ['Id']
    replication_method = 'INCREMENTAL'
    # replication keys is LastUpdatedTime, nested under metadata
    replication_keys = ['MetaData']
    additional_where = None

    def __init__(self, client, config, state):
        self.client = client
        self.config = config
        self.state = state


    def sync(self):
        start_position = 1
        max_results = int(self.config.get('max_results', '200'))

        bookmark = singer.get_bookmark(self.state, self.stream_name, 'LastUpdatedTime', self.config.get('start_date'))

        while True:
            query = query_builder.build_query(self.table_name, bookmark, start_position, max_results, additional_where=self.additional_where)

            resp = self.client.get(self.endpoint, params={"query": query}).get('QueryResponse',{})

            results = resp.get(self.table_name, [])
            for rec in results:
                yield rec

            if results:
                self.state = singer.write_bookmark(self.state, self.stream_name, 'LastUpdatedTime', rec.get('MetaData').get('LastUpdatedTime'))
                singer.write_state(self.state)

            if len(results) < max_results:
                break
            start_position += max_results

        singer.write_state(self.state)


class Accounts(Stream):
    stream_name = 'accounts'
    table_name = 'Account'
    additional_where = "Active IN (true, false)"


class Budgets(Stream):
    stream_name = 'budgets'
    table_name = 'Budget'
    additional_where = "Active IN (true, false)"


class Classes(Stream):
    stream_name = 'classes'
    table_name = 'Class'
    additional_where = "Active IN (true, false)"


class CreditMemos(Stream):
    stream_name = 'credit_memos'
    table_name = 'CreditMemo'


class BillPayments(Stream):
    stream_name = 'bill_payments'
    table_name = 'BillPayment'


class SalesReceipts(Stream):
    stream_name = 'sales_receipts'
    table_name = 'SalesReceipt'


class Purchases(Stream):
    stream_name = 'purchases'
    table_name = 'Purchase'


class Payments(Stream):
    stream_name = 'payments'
    table_name = 'Payment'


class PurchaseOrders(Stream):
    stream_name = 'purchase_orders'
    table_name = 'PurchaseOrder'


class PaymentMethods(Stream):
    stream_name = 'payment_methods'
    table_name = 'PaymentMethod'
    additional_where = "Active IN (true, false)"


class JournalEntries(Stream):
    stream_name = 'journal_entries'
    table_name = 'JournalEntry'


class Items(Stream):
    stream_name = 'items'
    table_name = 'Item'
    additional_where = "Active IN (true, false)"


class Invoices(Stream):
    stream_name = 'invoices'
    table_name = 'Invoice'


class Customers(Stream):
    stream_name = 'customers'
    table_name = 'Customer'
    additional_where = "Active IN (true, false)"


class RefundReceipts(Stream):
    stream_name = 'refund_receipts'
    table_name = 'RefundReceipt'


class Deposits(Stream):
    stream_name = 'deposits'
    table_name = 'Deposit'


class Departments(Stream):
    stream_name = 'departments'
    table_name = 'Department'
    additional_where = "Active IN (true, false)"


class Employees(Stream):
    stream_name = 'employees'
    table_name = 'Employee'
    additional_where = "Active IN (true, false)"


class Estimates(Stream):
    stream_name = 'estimates'
    table_name = 'Estimate'


class Bills(Stream):
    stream_name = 'bills'
    table_name = 'Bill'


class TaxAgencies(Stream):
    stream_name = 'tax_agencies'
    table_name = 'TaxAgency'


class TaxCodes(Stream):
    stream_name = 'tax_codes'
    table_name = 'TaxCode'
    additional_where = "Active IN (true, false)"


class TaxRates(Stream):
    stream_name = 'tax_rates'
    table_name = 'TaxRate'
    additional_where = "Active IN (true, false)"


class Terms(Stream):
    stream_name = 'terms'
    table_name = 'Term'
    additional_where = "Active IN (true, false)"


class TimeActivities(Stream):
    stream_name = 'time_activities'
    table_name = 'TimeActivity'


class Transfers(Stream):
    stream_name = 'transfers'
    table_name = 'Transfer'


class VendorCredits(Stream):
    stream_name = 'vendor_credits'
    table_name = 'VendorCredit'


class Vendors(Stream):
    stream_name = 'vendors'
    table_name = 'Vendor'
    additional_where = "Active IN (true, false)"

class DeletedObjects(Stream):
    endpoint = '/v3/company/{realm_id}/cdc'
    stream_name = 'deleted_objects'
    table_name = 'DeletedObjects'
    key_properties = ['Id', 'Type']
    max_date = None
    is_deleted_object_found = False

    # Change tracking is not supported for TimeActivities, TaxAgencies, TaxCodes and TaxRates
    # Reference: https://developer.intuit.com/app/developer/qbo/docs/develop/explore-the-quickbooks-online-api/change-data-capture
    deleted_entities = ['Account', 'BillPayment', 'Bill', 'Budget', 'Class', 'CreditMemo',
                        'Customer', 'Department', 'Deposit', 'Employee', 'Estimate', 'Invoice',
                        'Item', 'JournalEntry', 'PaymentMethod', 'Payment', 'PurchaseOrder', 'Purchase',
                        'RefundReceipt', 'SalesReceipt', 'Term', 'Transfer', 'VendorCredit', 'Vendor']

    def sync(self):

        bookmark = singer.get_bookmark(self.state, self.stream_name, 'LastUpdatedTime', self.config.get('start_date'))
        self.max_date = bookmark
        params = {
            'entities': ','.join(self.deleted_entities),
            'changedSince': bookmark
        }

        # Get change tracking for all the entities in single call
        resp = self.client.get(self.endpoint, params=params).get('CDCResponse',[{}])[0].get('QueryResponse', [{}])

        # Calculate number of objects found in response 
        total_objects = 0
        for entities in resp:
            for entity, values in entities.items():
                if isinstance(values, list):
                    total_objects += len(values)

        # Change tracking API return maximum 1000 object changes. 
        # So if objects are not less than 1000 then make individual call for every entity
        # Reference: https://developer.intuit.com/app/developer/qbo/docs/develop/explore-the-quickbooks-online-api/change-data-capture

        if total_objects < 1000:
            yield from self.parse_data_and_write(resp)
        else:
            for entity in self.deleted_entities:
                params = {
                    'entities': entity,
                    'changedSince': bookmark
                }
                resp = self.client.get(self.endpoint, params=params).get('CDCResponse',[{}])[0].get('QueryResponse', [{}])
                yield from self.parse_data_and_write(resp)

        # Write bookmark if any deleted object found
        if self.is_deleted_object_found:
            self.state = singer.write_bookmark(self.state, self.stream_name, 'LastUpdatedTime', self.max_date)

        singer.write_state(self.state)

    def parse_data_and_write(self, response):
        '''
            Parse change tracking response and return every deleted entity 
        '''
        for entities in response:
            for entity, values in entities.items():
                if isinstance(values, list):
                    for rec in values:
                        if rec.get('status', None) == 'Deleted':
                            self.is_deleted_object_found = True
                            rec['Type'] = entity
                            self.max_date = max(self.max_date, rec.get('MetaData').get('LastUpdatedTime'))
                            yield rec


STREAM_OBJECTS = {
    "accounts": Accounts,
    "bill_payments": BillPayments,
    "bills": Bills,
    "budgets": Budgets,
    "classes": Classes,
    "credit_memos": CreditMemos,
    "customers": Customers,
    "departments": Departments,
    "deposits": Deposits,
    "employees": Employees,
    "estimates": Estimates,
    "invoices": Invoices,
    "items": Items,
    "journal_entries": JournalEntries,
    "payment_methods": PaymentMethods,
    "payments": Payments,
    "purchase_orders": PurchaseOrders,
    "purchases": Purchases,
    "refund_receipts": RefundReceipts,
    "sales_receipts": SalesReceipts,
    "tax_agencies": TaxAgencies,
    "tax_codes": TaxCodes,
    "tax_rates": TaxRates,
    "terms": Terms,
    "time_activities": TimeActivities,
    "transfers": Transfers,
    "vendor_credits": VendorCredits,
    "vendors": Vendors,
    "deleted_objects": DeletedObjects
}
