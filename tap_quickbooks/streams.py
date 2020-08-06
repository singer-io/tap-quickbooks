class Stream:
    def __init__(self, client, config, state):
        self.client = client
        self.config = config
        self.state = state

    def format_query(self, wheres, startposition, maxresults):
        bookmark = self.state.get(self.stream_id, {}).get('bookmark') or self.config.get('start_date') # had to update start_date in config
        wheres += ["Metadata.LastUpdatedTime >= " + "'" +  bookmark + "'"]

        orders = ["Metadata.LastUpdatedTime ASC", "STARTPOSITION " + str(startposition), "MAXRESULTS " + str(maxresults)]
        where_clause = "WHERE " + " AND ".join(wheres)
        order_clause = "ORDER BY " + " ".join(orders)
        query = "SELECT * FROM " + self.query_name + " " + where_clause + " " + order_clause
        return query


# Query Building Notes
#max-results 200
# startpostiion start at 1
#startposition—The starting count of the response for pagination.
#maxresults—The number of entity elements in the <QueryResponse> element.

# "Metadata.LastUpdatedTime >= '%s' " time
# "ORDER BY Metadata.LastUpdatedTime ASC STARTPOSITION %s MAXRESULTS %s"
#                start-pos
#                max-results

# theory:
# never change LastUpdatedTime during the pagination
# increase startposition each loop by += max-results
# loop until count records is less than maxresults?
# save bookmark whenever it changes

# bookmarking should also save start-position in the case that you loop and never more time forward

class Accounts(Stream):
    stream_id = 'accounts'
    stream_name = 'accounts'
    endpoint = '/v3/company/{}/query'
    key_properties = ['Id']
    replication_method = 'INCREMENTAL'
    replication_keys = ['LastUpdatedTime']

    # capitalization appears to not matter
    query_name = 'Account'

    # def sync(self):
    #     additional_wheres = ["Active IN (true, false)"]
    #     additional_orders = []
    #     query = self.format_query(additional_wheres, additional_orders)
    #     for rec in self.client.get(self.endpoint.format(self.client.realm_id), params={"query": "SELECT * FROM Account WHERE Active IN (true, false)"}).get('QueryResponse', {}).get('Account'):
    #         yield rec

    def sync(self):
        additional_wheres = ["Active IN (true, false)"]
        startposition = 1
        maxresults = 90 # this could be global maybe
        query = self.format_query(additional_wheres, startposition, maxresults)

        # TODO: We can do better here
        results = self.client.get(self.endpoint.format(self.client.realm_id), params={"query": query}).get('QueryResponse',{}).get('Account', [])
        for rec in results:
            yield rec

        while len(results) == maxresults:
            startposition += len(results)
            query = self.format_query(additional_wheres,  startposition, maxresults)
            results = self.client.get(self.endpoint.format(self.client.realm_id), params={"query": query}).get('QueryResponse',{}).get('Account', [])
            for rec in results:
                yield rec


class Invoices(Stream):
    stream_id = 'invoices'
    stream_name = 'invoices'
    endpoint = ''
    key_properties = ['Id']
    replication_method = 'INCREMENTAL'
    replication_keys = ['LastUpdatedTime']


class Items(Stream):
    stream_id = 'items'
    stream_name = 'items'
    endpoint = ''
    key_properties = ['Id']
    replication_method = 'INCREMENTAL'
    replication_keys = ['LastUpdatedTime']


STREAM_OBJECTS = {
    "accounts": Accounts,
    "invoices": Invoices,
    "items": Items
}
