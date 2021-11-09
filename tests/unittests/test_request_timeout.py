import unittest
from unittest import mock
from unittest.case import TestCase
from requests.exceptions import Timeout
import tap_quickbooks
from tap_quickbooks import LOGGER, QuickbooksClient

class TestBackoffError(unittest.TestCase):
    '''
    Test that backoff logic works properly.
    '''
    @mock.patch('tap_quickbooks.client.OAuth2Session.request')
    @mock.patch('tap_quickbooks.client.QuickbooksClient.get')
    def test_request_timeout_and_backoff_on_make_request(self, mock_get, mock_send):
        """
        Check whether the request backoffs properly for 5 times in case of Timeout error.
        """
        mock_send.side_effect = Timeout
        with self.assertRaises(Timeout):
            client = QuickbooksClient("", {"start_date": "dummy_start_date", "refresh_token": "dummy_token", "client_id": "dummy_client_id", "client_secret": "dummy_client_secret", "user_agent": "dummy_ua", "realm_id": "dummy_ri"})
            client._make_request('GET', '/v3/company/dummy_ri/query')
        self.assertEquals(mock_send.call_count, 5)

class MockResponse():
    def __init__(self, resp, status_code, content=[""], headers=None, raise_error=False, text={}):
        self.json_data = resp
        self.status_code = status_code
        self.content = content
        self.headers = headers
        self.raise_error = raise_error
        self.text = text
        self.reason = "error"

    def prepare(self):
        return (self.json_data, self.status_code, self.content, self.headers, self.raise_error)

    def json(self):
        return self.text

class TestRequestTimeoutValue(unittest.TestCase):
    '''
    Test that request timeout parameter works properly in various cases
    '''
    @mock.patch('tap_quickbooks.client.OAuth2Session.request', return_value = MockResponse("", status_code=200))
    @mock.patch('tap_quickbooks.client.QuickbooksClient.get')
    def test_config_provided_request_timeout(self, mock_get, mock_request):
        """ 
            Unit tests to ensure that request timeout is set based on config value
        """
        config = {"start_date": "dummy_start_date", "refresh_token": "dummy_token", "client_id": "dummy_client_id", "client_secret": "dummy_client_secret", "user_agent": "dummy_ua", "realm_id": "dummy_ri", "request_timeout": 100}
        client = QuickbooksClient("", config)
        client._make_request('GET', '/v3/company/dummy_ri/query')
        headers = {
            'Accept': 'application/json',
            'User-Agent': "dummy_ua"
        }
        
        mock_request.assert_called_with("GET", "https://quickbooks.api.intuit.com/v3/company/dummy_ri/query", headers=headers, params=None, data=None, timeout=100.0)

    @mock.patch('tap_quickbooks.client.OAuth2Session.request', return_value = MockResponse("", status_code=200))
    @mock.patch('tap_quickbooks.client.QuickbooksClient.get')
    def test_default_value_request_timeout(self, mock_get, mock_request):
        """ 
            Unit tests to ensure that request timeout is set based default value
        """
        config = {"start_date": "dummy_start_date", "refresh_token": "dummy_token", "client_id": "dummy_client_id", "client_secret": "dummy_client_secret", "user_agent": "dummy_ua", "realm_id": "dummy_ri"}
        client = QuickbooksClient("", config)
        client._make_request('GET', '/v3/company/dummy_ri/query')
        headers = {
            'Accept': 'application/json',
            'User-Agent': "dummy_ua"
        }
        
        mock_request.assert_called_with("GET", "https://quickbooks.api.intuit.com/v3/company/dummy_ri/query", headers=headers, params=None, data=None, timeout=300)

    @mock.patch('tap_quickbooks.client.OAuth2Session.request', return_value = MockResponse("", status_code=200))
    @mock.patch('tap_quickbooks.client.QuickbooksClient.get')
    def test_config_provided_empty_request_timeout(self, mock_get, mock_request):
        """ 
            Unit tests to ensure that request timeout is set based on default value if empty value is given in config
        """
        config = {"start_date": "dummy_start_date", "refresh_token": "dummy_token", "client_id": "dummy_client_id", "client_secret": "dummy_client_secret", "user_agent": "dummy_ua", "realm_id": "dummy_ri", "request_timeout": ""}
        client = QuickbooksClient("", config)
        client._make_request('GET', '/v3/company/dummy_ri/query')
        headers = {
            'Accept': 'application/json',
            'User-Agent': "dummy_ua"
        }
        
        mock_request.assert_called_with("GET", "https://quickbooks.api.intuit.com/v3/company/dummy_ri/query", headers=headers, params=None, data=None, timeout=300)

    @mock.patch('tap_quickbooks.client.OAuth2Session.request', return_value = MockResponse("", status_code=200))
    @mock.patch('tap_quickbooks.client.QuickbooksClient.get')
    def test_config_provided_string_request_timeout(self, mock_get, mock_request):
        """ 
            Unit tests to ensure that request timeout is set based on config string value
        """
        config = {"start_date": "dummy_start_date", "refresh_token": "dummy_token", "client_id": "dummy_client_id", "client_secret": "dummy_client_secret", "user_agent": "dummy_ua", "realm_id": "dummy_ri", "request_timeout": "100"}
        client = QuickbooksClient("", config)
        client._make_request('GET', '/v3/company/dummy_ri/query')
        headers = {
            'Accept': 'application/json',
            'User-Agent': "dummy_ua"
        }
        
        mock_request.assert_called_with("GET", "https://quickbooks.api.intuit.com/v3/company/dummy_ri/query", headers=headers, params=None, data=None, timeout=100)

    @mock.patch('tap_quickbooks.client.OAuth2Session.request', return_value = MockResponse("", status_code=200))
    @mock.patch('tap_quickbooks.client.QuickbooksClient.get')
    def test_config_provided_float_request_timeout(self, mock_get, mock_request):
        """ 
            Unit tests to ensure that request timeout is set based on config float value
        """
        config = {"start_date": "dummy_start_date", "refresh_token": "dummy_token", "client_id": "dummy_client_id", "client_secret": "dummy_client_secret", "user_agent": "dummy_ua", "realm_id": "dummy_ri", "request_timeout": 100.8}
        client = QuickbooksClient("", config)
        client._make_request('GET', '/v3/company/dummy_ri/query')
        headers = {
            'Accept': 'application/json',
            'User-Agent': "dummy_ua"
        }
        
        mock_request.assert_called_with("GET", "https://quickbooks.api.intuit.com/v3/company/dummy_ri/query", headers=headers, params=None, data=None, timeout=100.8)



