import unittest
from unittest import mock
from parameterized import parameterized
from requests.exceptions import Timeout, ConnectionError
from tap_quickbooks.client import QuickbooksClient, Quickbooks5XXException,\
    QuickbooksClient,QuickbooksBadRequestError,QuickbooksAuthenticationError,QuickbooksForbiddenError\
        ,QuickbooksNotFoundError, QuickbooksInternalServerError,QuickbooksServiceUnavailableError

class TestBackoffOAuth2SessionInitialization(unittest.TestCase):
    config = {'refresh_token':'token','client_id':'id','client_secret':'secret','user_agent':'agent','realm_id':'realm_id','sandbox':True}
    
    @parameterized.expand([
        ['quickbooks_400_exception',QuickbooksBadRequestError,[False, 3]],
        ['quickbooks_401_exception',QuickbooksAuthenticationError,[True, 3]],
        ['quickbooks_403_exception',QuickbooksForbiddenError,[True, 3]],
        ['quickbooks_404_exception',QuickbooksNotFoundError,[False, 3]],
        ['quickbooks_500_exception',QuickbooksInternalServerError,[False, 3]],
        ['quickbooks_502_exception',Quickbooks5XXException,[False, 3]],
        ['quickbooks_503_exception',QuickbooksServiceUnavailableError,[False, 3]],
        ['requests_connection_error',ConnectionError,[False, 3]],
        ['timeout_error',Timeout,[False, 5]],
    ])
    @mock.patch('time.sleep')
    @mock.patch('tap_quickbooks.client.QuickbooksClient._write_config')
    @mock.patch('requests_oauthlib.OAuth2Session.request')
    @mock.patch('tap_quickbooks.client.LOGGER.info')
    def test_backoff_for_OAuth2Session(self,test_name,test_exception,expected_data,mocked_logger,mocked_oauth_request,mocked_write_config,mocked_sleep):
        """Test backoff for QuickbooksClient initialization"""
        mocked_oauth_request.side_effect = test_exception('exception')
        
        with self.assertRaises(test_exception) as e:
            QuickbooksClient('path',self.config)

        self.assertEqual(mocked_oauth_request.call_count,expected_data[1])
        if expected_data[0]:
            mocked_logger.assert_called_with("Error initializing QuickbooksClient during token refresh, please reauthenticate.")

    @parameterized.expand([
        ['quickbooks_400_exception',QuickbooksBadRequestError,3],
        ['quickbooks_401_exception',QuickbooksAuthenticationError,3],
        ['quickbooks_403_exception',QuickbooksForbiddenError,3],
        ['quickbooks_404_exception',QuickbooksNotFoundError,3],
        ['quickbooks_500_exception',QuickbooksInternalServerError,3],
        ['quickbooks_502_exception',Quickbooks5XXException,3],
        ['quickbooks_503_exception',QuickbooksServiceUnavailableError,3],
        ['requests_connection_error',ConnectionError,3],
        ['timeout_error',Timeout,5],
    ])
    @mock.patch('time.sleep')
    @mock.patch('tap_quickbooks.client.QuickbooksClient._write_config')
    @mock.patch('requests_oauthlib.OAuth2Session.request')
    @mock.patch('tap_quickbooks.client.QuickbooksClient.get')
    def test_backoff_for_make_request(self,test_name,test_exception,test_backoff_count,mocked_get,mocked_oauth_request,mocked_write_config,mocked_sleep):
        """Test backoff for _make_request call"""
        
        client_obj = QuickbooksClient('path',self.config)

        mocked_oauth_request.side_effect  = test_exception('exception')
        
        with self.assertRaises(test_exception) as e:
            client_obj._make_request('GET','endpoint')

        self.assertEqual(mocked_oauth_request.call_count,test_backoff_count)