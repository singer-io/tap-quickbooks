class Stream:
    def __init__(self, client, config, state):
        self.client = client
        self.config = config
        self.state = state

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

    def sync(self):
        for rec in self.client.get(self.endpoint.format(self.client.realm_id), params={"query": "SELECT * FROM Account WHERE Active IN (true, false)"}).get('QueryResponse', {}).get('Account'):
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
