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
        # TODO: 1 or bookmarked start_position?
        start_position = 1
        max_results = self.config.get('max_results', 200)

        bookmark = singer.get_bookmark(self.state, self.stream_id, 'LastUpdatedTime') or self.config.get('start_date')

        while True:
            query = query_builder.build_query(self.table_name, bookmark, start_position, max_results, additional_where=self.additional_where)
            resp = self.client.get(self.endpoint, params={"query": query}).get('QueryResponse',{})

            results = resp.get(self.table_name, [])
            for rec in results:
                yield rec

            if results:
            # Write state after each page is yielded
            # TODO: Check start_position ideas
                state = singer.write_bookmark(self.state, self.stream_id, 'LastUpdatedTime', rec.get('MetaData').get('LastUpdatedTime'))
                state = singer.write_bookmark(self.state, self.stream_id, 'start_position', resp['startPosition'] + resp['maxResults'])
                singer.write_state(state)

            if len(results) < max_results:
                break
            start_position += max_results


# theory:
# never change LastUpdatedTime during the pagination
# increase start_position each loop by += max-results
# loop until count records is less than maxresults?
# save bookmark whenever it changes
# bookmarking should also save start-position in the case that you loop and never more time forward

class Accounts(Stream):
    stream_id = 'accounts'
    stream_name = 'accounts'
    table_name = 'Account'
    additional_where = "Active IN (true, false)"


class Invoices(Stream):
    stream_id = 'invoices'
    stream_name = 'invoices'
    table_name = 'Invoice'


class Items(Stream):
    stream_id = 'items'
    stream_name = 'items'
    table_name = 'Item'


class Budgets(Stream):
    stream_id = 'budgets'
    stream_name = 'budgets'
    table_name = 'Budget'


class Classes(Stream):
    stream_id = 'classes'
    stream_name = 'classes'
    table_name = 'Class'


class CreditMemos(Stream):
    stream_id = 'credit_memos'
    stream_name = 'credit_memos'
    table_name = 'CreditMemo'


class BillPayments(Stream):
    stream_id = 'bill_payments'
    stream_name = 'bill_payments'
    table_name = 'BillPayment'


class SalesReceipts(Stream):
    stream_id = 'sales_receipts'
    stream_name = 'sales_receipts'
    table_name = 'SalesReceipt'


class Purchases(Stream):
    stream_id = 'purchases'
    stream_name = 'purchases'
    table_name = 'Purchase'


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
}
