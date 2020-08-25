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
                state = singer.write_bookmark(self.state, self.stream_name, 'LastUpdatedTime', rec.get('MetaData').get('LastUpdatedTime'))
                singer.write_state(state)

            if len(results) < max_results:
                break
            start_position += max_results

        singer.write_state(state)



class Accounts(Stream):
    stream_name = 'accounts'
    table_name = 'Account'
    additional_where = "Active IN (true, false)"


class Invoices(Stream):
    stream_name = 'invoices'
    table_name = 'Invoice'


class Items(Stream):
    stream_name = 'items'
    table_name = 'Item'


class Budgets(Stream):
    stream_name = 'budgets'
    table_name = 'Budget'


class Classes(Stream):
    stream_name = 'classes'
    table_name = 'Class'


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



class JournalEntries(Stream):
    stream_name = 'journal_entries'
    table_name = 'JournalEntry'

class Items(Stream):
    stream_name = 'items'
    table_name = 'Item'

class Invoices(Stream):
    stream_name = 'invoices'
    table_name = 'Invoice'

class Customers(Stream):
    stream_name = 'customers'
    table_name = 'Customer'

class RefundReceipts(Stream):
    stream_name = 'refund_receipts'
    table_name = 'RefundReceipt'

class Deposits(Stream):
    stream_name = 'deposits'
    table_name = 'Deposit'

class Departments(Stream):
    stream_name = 'departments'
    table_name = 'Department'

class Employees(Stream):
    stream_name = 'employees'
    table_name = 'Employee'

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

class TaxRates(Stream):
    stream_name = 'tax_rates'
    table_name = 'TaxRate'

class Terms(Stream):
    stream_name = 'terms'
    table_name = 'Term'

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


STREAM_OBJECTS = {
    "accounts": Accounts,
    "invoices": Invoices,
    "items": Items,
    "budgets": Budgets,
    "classes": Classes,
    "credit_memos": CreditMemos,
    "bill_payments": BillPayments,
    "sales_receipts": SalesReceipts,
    "purchases": Purchases,
    "payments": Payments,
    "purchase_orders": PurchaseOrders,
    "payment_methods": PaymentMethods,
    "journal_entries": JournalEntries,
    "items": Items,
    "invoices": Invoices,
    "customers": Customers,
    "refund_receipts": RefundReceipts,
    "deposits": Deposits,
    "departments": Departments,
    "employees": Employees,
    "estimates": Estimates,
    "bills": Bills,
    "tax_agencies": TaxAgencies,
    "tax_codes": TaxCodes,
    "tax_rates": TaxRates,
    "terms": Terms,
    "time_activities": TimeActivities,
    "transfers": Transfers,
    "vendor_credits": VendorCredits,
    "vendors": Vendors
}
