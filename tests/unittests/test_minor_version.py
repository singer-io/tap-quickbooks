import unittest
from unittest import mock

from tap_quickbooks import QuickbooksClient
from tap_quickbooks.streams import Accounts

@mock.patch('tap_quickbooks.client.OAuth2Session.request')
@mock.patch('tap_quickbooks.client.QuickbooksClient.get')
class TestMinorVersion(unittest.TestCase):
    '''
    Tests to verify minor_version works as expected.
    '''

    def test_minor_version(self,mocked_get, mocked_request):
        """ 
            Tests to ensure that minor_version is set as expected in client.py
        """
        config = {"start_date": "test_start_date", "refresh_token": "test_token", "client_id": "test_client_id", "client_secret": "test_client_secret", "user_agent": "test_ua", "realm_id": "test_ri"}
        client = QuickbooksClient("", config)

        # Verify minor version is passed in API query request
        params={"query": "SELECT * FROM CompanyInfo","minorversion":65}
        mocked_get.assert_called_with("/v3/company/test_ri/query", params=params)

    def test_sync_minor_version(self, mocked_get,mocked_request):
        """ 
            Tests to ensure that minor_version works as expected while syncing.
        """
        config = {"start_date": "test_start_date", "refresh_token": "test_token", "client_id": "test_client_id", "client_secret": "test_client_secret", "user_agent": "test_ua", "realm_id": "test_ri"}
        client = QuickbooksClient("", config)
        state = {"bookmarks": {"accounts": {"LastUpdatedTime": "2012-08-21T00:00:00Z"}}}
        mock_data = {'QueryResponse':{'Account':[{'data':'value','MetaData':{'LastUpdatedTime':'2012-08-21T00:00:00Z'}}]}}
        mocked_get.return_value = mock_data

        stream = Accounts(client, config, state)
        list(stream.sync())

        # Verify minor version is passed in API query request
        params={'query': 'SELECT * FROM CompanyInfo', 'minorversion': 65}
        mocked_get.assert_any_call('/v3/company/test_ri/query', params=params)
