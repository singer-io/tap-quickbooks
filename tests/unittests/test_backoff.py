import unittest
from unittest import mock
from parameterized import parameterized
from requests.exceptions import Timeout, ConnectionError
from tap_quickbooks.client import Quickbooks4XXException, QuickbooksClient, Quickbooks5XXException,\
    QuickbooksClient, QuickbooksBadRequestError, QuickbooksAuthenticationError, QuickbooksForbiddenError, QuickbooksNotFoundError, QuickbooksInternalServerError, QuickbooksServiceUnavailableError
from test_exception_handling import get_response

class TestBackoffOAuth2SessionInitialization(unittest.TestCase):
    config = {
        'refresh_token': 'token',
        'client_id': 'id',
        'client_secret': 'secret',
        'user_agent': 'agent',
        'realm_id': 'realm_id',
        'sandbox': True
    }

    @parameterized.expand([
        ['quickbooks_400_exception', [400, QuickbooksBadRequestError], [False, 3]],
        ['quickbooks_401_exception', [401, QuickbooksAuthenticationError], [True, 3]],
        ['quickbooks_402_exception', [402, Quickbooks4XXException], [False, 3]],
        ['quickbooks_403_exception', [403, QuickbooksForbiddenError], [True, 3]],
        ['quickbooks_404_exception', [404, QuickbooksNotFoundError], [False, 3]],
        ['quickbooks_500_exception', [500, QuickbooksInternalServerError], [False, 3]],
        ['quickbooks_502_exception', [502, Quickbooks5XXException], [False, 3]],
        ['quickbooks_503_exception', [503, QuickbooksServiceUnavailableError], [False, 3]],
        ['requests_connection_error', [None, ConnectionError], [False, 3]],
        ['timeout_error', [None, Timeout], [False, 5]],
    ])
    @mock.patch('time.sleep')
    @mock.patch('tap_quickbooks.client.QuickbooksClient._write_config')
    @mock.patch('requests_oauthlib.OAuth2Session.request')
    @mock.patch('tap_quickbooks.client.LOGGER.info')
    def test_backoff_for_OAuth2Session(self, test_name, test_data, expected_data, mocked_logger, mocked_oauth_request, mocked_write_config, mocked_sleep):
        """Test backoff for QuickbooksClient initialization"""
        # Mock request and raise error
        if test_data[0]:
            mocked_oauth_request.return_value = get_response(test_data[0])
        else:
            mocked_oauth_request.side_effect = test_data[1]('exception')

        # Verify we raise expected error
        with self.assertRaises(test_data[1]) as e:
            QuickbooksClient('path', self.config)

        # Verify the call count
        self.assertEqual(mocked_oauth_request.call_count, expected_data[1])

        # Verify we log the re-authentication message for 401 and 403 errors
        if expected_data[0]:
            mocked_logger.assert_called_with(
                "Error initializing QuickbooksClient during token refresh, please reauthenticate.")

    @parameterized.expand([
        ['quickbooks_400_exception', [400, QuickbooksBadRequestError], 3],
        ['quickbooks_401_exception', [401, QuickbooksAuthenticationError], 3],
        ['quickbooks_402_exception', [402, Quickbooks4XXException], 3],
        ['quickbooks_403_exception', [403, QuickbooksForbiddenError], 3],
        ['quickbooks_404_exception', [404, QuickbooksNotFoundError], 3],
        ['quickbooks_500_exception', [500, QuickbooksInternalServerError], 3],
        ['quickbooks_502_exception', [502, Quickbooks5XXException], 3],
        ['quickbooks_503_exception', [503, QuickbooksServiceUnavailableError], 3],
        ['requests_connection_error', [None, ConnectionError], 3],
        ['timeout_error', [None, Timeout], 5],
    ])
    @mock.patch('time.sleep')
    @mock.patch('tap_quickbooks.client.QuickbooksClient._write_config')
    @mock.patch('requests_oauthlib.OAuth2Session.request')
    @mock.patch('tap_quickbooks.client.QuickbooksClient.get')
    def test_backoff_for_make_request(self, test_name, test_data, test_backoff_count, mocked_get, mocked_oauth_request, mocked_write_config, mocked_sleep):
        """Test backoff for _make_request call"""

        client_obj = QuickbooksClient('path', self.config)

        # Mock request and raise error
        if test_data[0]:
            mocked_oauth_request.return_value = get_response(test_data[0])
        else:
            mocked_oauth_request.side_effect = test_data[1]('exception')

        # Verify we raise expected error
        with self.assertRaises(test_data[1]) as e:
            client_obj._make_request('GET', 'endpoint')

        # Verify we backoff for the desired count
        self.assertEqual(mocked_oauth_request.call_count, test_backoff_count)
