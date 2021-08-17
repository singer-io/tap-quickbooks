import tap_quickbooks.query_builder as query_builder

import singer
from singer import utils
from singer.utils import strptime_to_utc, strftime
from datetime import timedelta

DATE_WINDOW_SIZE = 29

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

class ReportStream(Stream):
    parsed_metadata = {
        'dates': [],
        'data': []
    }
    key_properties = ['ReportDate']
    replication_method = 'INCREMENTAL'
    # replication keys is ReportDate, manually created from data
    replication_keys = ['ReportDate']

    def sync(self):

        is_start_date_used = False
        params = {
            'summarize_column_by': 'Days'
        }

        start_dttm_str = singer.get_bookmark(self.state, self.stream_name, 'LastUpdatedTime')
        if start_dttm_str is None:
            start_dttm_str = self.config.get('start_date')
            is_start_date_used = True

        start_dttm = strptime_to_utc(start_dttm_str)
        end_dttm = start_dttm + timedelta(days=DATE_WINDOW_SIZE)
        now_dttm = utils.now()
        
        # Fetch records for minimum 30 days 
        # if bookmark from state file is used and it's less than 30 days away
        # Fetch records for start_date to current date 
        # if start_date is used and it'sless than 30 days away
        if end_dttm > now_dttm:
            end_dttm = now_dttm
            if not is_start_date_used:
                start_dttm = end_dttm - timedelta(days=DATE_WINDOW_SIZE)

        while start_dttm < now_dttm:
            self.parsed_metadata = {
                'dates': [],
                'data': []
            }

            start_tm_str = strftime(start_dttm)[0:10]
            end_tm_str = strftime(end_dttm)[0:10]

            params["start_date"] = start_tm_str
            params["end_date"] = end_tm_str

            resp = self.client.get(self.endpoint, params=params)
            self.parse_report_metadata(resp)

            reports = self.day_wise_reports()
            if reports:
                for report in reports:
                    yield report
                self.state = singer.write_bookmark(self.state, self.stream_name, 'LastUpdatedTime', strptime_to_utc(report.get('ReportDate')).isoformat())
                singer.write_state(self.state)

            start_dttm = end_dttm
            end_dttm = start_dttm + timedelta(days=DATE_WINDOW_SIZE)

            if end_dttm > now_dttm:
                end_dttm = now_dttm

        singer.write_state(self.state)

    def parse_report_metadata(self, pileOfRows):
        '''
            Restructure data from report response on daily basis and update self.parsed_metadata dictionary
            {
                "dates": ["2021-07-01", "2021-07-02", "2021-07-03"],
                "data": [ {
                    "name": "Total Income",
                    "values": ["4.00", "4.00", "4.00", "12.00"]
                }, {
                    "name": "Gross Profit",
                    "values": ["4.00", "4.00", "4.00", "12.00"]
                }, {
                    "name": "Total Expenses",
                    "values": ["1.00", "1.00", "1.00", "3.00"]
                }, {
                    "name": "Net Income",
                    "values": ["3.00", "3.00", "3.00", "9.00"]
                }]
            }
        '''

        if isinstance(pileOfRows, list):
            for x in pileOfRows:
                self.parse_report_metadata(x)

        else:

            if 'Rows' in pileOfRows.keys():
                self.parse_report_metadata(pileOfRows['Rows'])

            if 'Row' in pileOfRows.keys():
                self.parse_report_metadata(pileOfRows['Row'])

            if 'Summary' in pileOfRows.keys():
                self.parse_report_metadata(pileOfRows['Summary'])

            if 'ColData' in pileOfRows.keys():
                d = dict()
                d['name'] = pileOfRows['ColData'][0]['value']
                vals = []
                for x in pileOfRows['ColData'][1:]:
                    vals.append(x['value'])
                d['values'] = vals
                self.parsed_metadata['data'].append(d)

            if 'Columns' in pileOfRows.keys():
                self.parse_report_metadata(pileOfRows['Columns'])

            if 'Column' in pileOfRows.keys():
                for x in pileOfRows['Column']:
                    if 'MetaData' in x.keys():
                        for md in x['MetaData']:
                            if md['Name'] in ['StartDate']:
                                self.parsed_metadata['dates'].append(md['Value'])

    def day_wise_reports(self):
        '''
            Return record for every day formed using output of parse_report_metadata
        '''
        for index, date in enumerate(self.parsed_metadata['dates']):
            report = dict()
            report['ReportDate'] = date
            report['AccountingMethod'] = 'Accrual'
            report['Details'] = {}

            for data in self.parsed_metadata['data']:
                report['Details'][data['name']] = data['values'][index]

            yield report


class ProfitAndLossReport(ReportStream):
    stream_name = 'profit_loss_report'
    endpoint = '/v3/company/{realm_id}/reports/ProfitAndLoss'

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
    "profit_loss_report": ProfitAndLossReport
}
