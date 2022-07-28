import unittest
from unittest import mock
from parameterized import parameterized
from tap_quickbooks.client import Quickbooks5XXException, QuickbooksClient,QuickbooksBadRequestError,\
    QuickbooksAuthenticationError,QuickbooksForbiddenError,QuickbooksNotFoundError,\
        QuickbooksInternalServerError,QuickbooksServiceUnavailableError

class MockResponse:
    def __init__(self,status_code,json):
        self.status_code = status_code
        self.txt = json

    def json(self):
        return self.txt

def get_response(status_code,json={}):
    return MockResponse(status_code,json)

class TestExceptionHandling(unittest.TestCase):
    config = {'refresh_token':'token','client_id':'id','client_secret':'secret','user_agent':'agent','realm_id':'realm_id','sandbox':True}
    
    @parameterized.expand([
        ['400_error',[400,{'Fault': {'Error':[{'Message':'Bad request','code':'400','Detail':'Bad request for the URL'}]}},QuickbooksBadRequestError],'HTTP-error-code: 400, Error: Message: Bad request, Quickbooks Error code: 400, Detail: Bad request for the URL'],
        ['401_error',[401,{'fault': {'error':[{'Message':'Not Authorized','code':'401','Detail':'Not aurthorized to access'}]}},QuickbooksAuthenticationError],'HTTP-error-code: 401, Error: Message: Not Authorized, Quickbooks Error code: 401, Detail: Not aurthorized to access'],
        ['403_error',[403,{'Fault': {'Error':[{'Message':'Forbidden','code':'403','Detail':'Forbidden for the URL'}]}},QuickbooksForbiddenError],'HTTP-error-code: 403, Error: Message: Forbidden, Quickbooks Error code: 403, Detail: Forbidden for the URL'],
        ['404_error',[404,{'Fault': {'Error':[{'Message':'Not found','code':'404','Detail':'Not found for the URL'}]}},QuickbooksNotFoundError],'HTTP-error-code: 404, Error: Message: Not found, Quickbooks Error code: 404, Detail: Not found for the URL'],
        ['500_error',[500,{'Fault': {'Error':[{'Message':'Inernal Server error','code':'500','Detail':'The service is unrechable'}]}},QuickbooksInternalServerError],'HTTP-error-code: 500, Error: Message: Inernal Server error, Quickbooks Error code: 500, Detail: The service is unrechable'],
        ['501_error',[501,{'Fault': {'Error':[{'Message':'Not implememted','code':'501','Detail':'Not implemnted for the URL'}]}},Quickbooks5XXException],'HTTP-error-code: 501, Error: Message: Not implememted, Quickbooks Error code: 501, Detail: Not implemnted for the URL'],
        ['503_error',[503,{'Fault': {'Error':[{'Message':'Service unavailable','code':'503','Detail':'The service is not available'}]}},QuickbooksServiceUnavailableError],'HTTP-error-code: 503, Error: Message: Service unavailable, Quickbooks Error code: 503, Detail: The service is not available'],
    ])
    @mock.patch('time.sleep')
    @mock.patch('tap_quickbooks.client.QuickbooksClient._write_config')
    @mock.patch('tap_quickbooks.client.QuickbooksClient.get')
    @mock.patch('requests_oauthlib.OAuth2Session.request')
    def test_API_exception_handling(self,test_name,test_data,exp,mocked_oauth_request,mocked_get,mocked_write_config,mock_sleep):
        """Test API exception handling for specific error"""

        client_obj = QuickbooksClient('path',self.config)
        mocked_oauth_request.return_value = get_response(test_data[0],test_data[1])

        with self.assertRaises(test_data[2]) as e:
            client_obj._make_request('GET','endpoint')
        self.assertEqual(str(e.exception),exp)

    @parameterized.expand([
        ['400_error',[400,QuickbooksBadRequestError],"HTTP-error-code: 400, Error: The request can't be fulfilled due to bad syntax."],
        ['401_error',[401,QuickbooksAuthenticationError],"HTTP-error-code: 401, Error: Authentication or authorization failed. Usually, this means the token in use won't work for API calls since it's either expired or revoked."],
        ['403_error',[403,QuickbooksForbiddenError],"HTTP-error-code: 403, Error: The URL exists, but it's restricted. External developers can't use or consume resources from this URL."],
        ['404_error',[404,QuickbooksNotFoundError],"HTTP-error-code: 404, Error: Couldn't find the requested resource or URL, or it doesn't exist."],
        ['500_error',[500,QuickbooksInternalServerError],"HTTP-error-code: 500, Error: A server error occurred while processing the request."],
        ['501_error',[501,Quickbooks5XXException],"HTTP-error-code: 501, Error: Unknown Error"],
        ['503_error',[503,QuickbooksServiceUnavailableError],"HTTP-error-code: 503, Error: The service is temporarily unavailable at the Server side."],
    ])
    @mock.patch('time.sleep')
    @mock.patch('tap_quickbooks.client.QuickbooksClient._write_config')
    @mock.patch('tap_quickbooks.client.QuickbooksClient.get')
    @mock.patch('requests_oauthlib.OAuth2Session.request')
    def test_custom_exception_handling(self,test_name,test_data,exp,mocked_oauth_request,mocked_get,mocked_write_config,mock_sleep):
        """Test custom exception handling for specific error"""

        client_obj = QuickbooksClient('path',self.config)
        mocked_oauth_request.return_value = get_response(test_data[0])

        with self.assertRaises(test_data[1]) as e:
            client_obj._make_request('GET','endpoint')
        self.assertEqual(str(e.exception),exp)
