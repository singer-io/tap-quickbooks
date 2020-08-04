class Stream:
    def __init__(self, client, config, state):
        self.client = client
        self.config = config
        self.state = state


class Accounts(Stream):
    stream_id = 'accounts'
    stream_name = 'accounts'
    endpoint = ''
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    replication_keys = ['LastUpdatedTime']
    

class Invoices(Stream):
    stream_id = 'accounts'
    stream_name = 'accounts'
    endpoint = ''
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    replication_keys = ['LastUpdatedTime']


class Items(Stream):    
    stream_id = 'accounts'
    stream_name = 'accounts'
    endpoint = ''
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    replication_keys = ['LastUpdatedTime']


STREAM_OBJECTS = {
    "accounts": Accounts,
    "invoices": Invoices,
    "items": Items
}
