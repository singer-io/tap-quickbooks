import json
import backoff
import requests
import singer

from requests_oauthlib import OAuth2Session
from requests.exceptions import Timeout


LOGGER = singer.get_logger()
REQUEST_TIMEOUT = 300

PROD_ENDPOINT_BASE = "https://quickbooks.api.intuit.com"
SANDBOX_ENDPOINT_BASE = "https://sandbox-quickbooks.api.intuit.com"
TOKEN_REFRESH_URL = 'https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer'

class QuickbooksError(Exception):
    """Base class for Quickbooks exceptions"""

class Quickbooks5XXException(Exception):
    """Base class for 5XX errors"""

class Quickbooks4XXException(Exception):
    """Base class for 4XX errors"""

class QuickbooksAuthenticationError(Quickbooks4XXException):
    """Exception class for 401 error"""

class QuickbooksBadRequestError(Quickbooks4XXException):
    """Exception class for 400 error"""

class QuickbooksForbiddenError(Quickbooks4XXException):
    """Exception class for 403 error"""

class QuickbooksNotFoundError(Quickbooks4XXException):
    """Exception class for 404 error"""

class QuickbooksTooManyRequestsError(Quickbooks4XXException):
    """Exception class for 429 error"""

class QuickbooksInternalServerError(Quickbooks5XXException):
    """Exception class for 500 error"""

class QuickbooksServiceUnavailableError(Quickbooks5XXException):
    """Exception class for 503 error"""

# Documentation: https://developer.intuit.com/app/developer/qbo/docs/develop/troubleshooting/error-codes
ERROR_CODE_EXCEPTION_MAPPING = {
    400: {
        "exception": QuickbooksBadRequestError,
        "message": "The request can't be fulfilled due to bad syntax."
    },
    401: {
        "exception": QuickbooksAuthenticationError,
        "message": "Authentication or authorization failed. Usually, this means the token in use won't work for API calls since it's either expired or revoked."
    },
    403: {
        "exception": QuickbooksForbiddenError,
        "message": "The URL exists, but it's restricted. External developers can't use or consume resources from this URL."
    },
    404: {
        "exception": QuickbooksNotFoundError,
        "message": "Couldn't find the requested resource or URL, or it doesn't exist."
    },
    429: {
        "exception": QuickbooksTooManyRequestsError,
        "message": "Rate limit exceeded. Too many requests in a given amount of time."
    },
    500: {
        "exception": QuickbooksInternalServerError,
        "message": "A server error occurred while processing the request."
    },
    503: {
        "exception": QuickbooksServiceUnavailableError,
        "message": "The service is temporarily unavailable at the Server side.",
    }
}

def get_exception_for_error_code(error_code):
    """Function to retrieve exceptions based on error_code"""

    exception = ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {}).get('exception')
    # If the error code is not from the listed error codes then return Quickbooks5XXException, Quickbooks4XXException or QuickbooksError respectively
    if not exception:
        if error_code >= 500:
            return Quickbooks5XXException
        if error_code >= 400:
            return Quickbooks4XXException
        return QuickbooksError
    return exception

def raise_for_error(response):
    """Function to raise an error by extracting the message from the error response"""

    error_code = response.status_code

    try:
        response_json = response.json()
    except Exception:
        response_json = {}

    if response_json.get('Fault', response_json.get('fault')):

        errors = response_json.get('Fault', {}).get('Error', response_json.get('fault', {}).get('error'))
        # Prepare the message with detail if there is single error
        if len(errors) == 1:
            errors = errors[0]
            msg = errors.get('Message', errors.get('message'))
            detail = errors.get('Detail', errors.get('detail'))
            internal_error_code = errors.get('code')
            error_message  = 'Message: {}, Quickbooks Error code: {}, Detail: {}'.format(msg, internal_error_code, detail)
        else:
            error_message = errors
    else:
        error_message = ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {}).get('message', 'Unknown Error')

    message = 'HTTP-error-code: {}, Error: {}'.format(error_code, error_message)
    ex = get_exception_for_error_code(error_code)

    raise ex(message) from None

class QuickbooksClient():
    def __init__(self, config_path, config, dev_mode = False):
        token = {
            'refresh_token': config['refresh_token'],
            'token_type': 'Bearer',
            # Set a fake access_token and expires_in to a negative number to force the client to reauthenticate
            'access_token': "wrong",
            'expires_in': '-30'
        }
        extra = {
            'client_id': config['client_id'],
            'client_secret': config['client_secret']
        }

        self.sandbox = False
        if config.get('sandbox') in ['true', 'True', True]:
            self.sandbox = True

        self.user_agent = config['user_agent']
        self.realm_id = config['realm_id']
        self.config_path = config_path
        self.config = config
        self.create_session(dev_mode, token, extra)
        # Latest minorversion is '75' according to doc, https://developer.intuit.com/app/developer/qbo/docs/learn/explore-the-quickbooks-online-api/minor-versions
        self.minor_version = 75
        try:
            # Make an authenticated request to any endpoint with minorversion=75 to validate the client object
            self.get('/v3/company/{}/query'.format(self.realm_id), params={"query": "SELECT * FROM CompanyInfo", "minorversion": self.minor_version})
        except Exception as e:
            LOGGER.info("Error initializing QuickbooksClient during token refresh, please reauthenticate.")
            raise e

    def create_session(self, dev_mode, token, extra):
        """
        If dev mode is enabled then session is created with the existing tokens.
        Else session is created with refreshed tokens.
        """
        if dev_mode:
            self.access_token = self.config.get('access_token')

            if not self.access_token:
                raise Exception("Access token config property is missing")

            dev_mode_token = {
                "refresh_token": self.config.get('refresh_token'),
                # Using the existing access_token for dev mode
                "access_token": self.access_token,
                'token_type': 'Bearer'
            }

            self.session = OAuth2Session(self.config['client_id'],
                                         token=dev_mode_token)
        else:
            self.session = OAuth2Session(self.config['client_id'],
                                         token=token,
                                         auto_refresh_url=TOKEN_REFRESH_URL,
                                         auto_refresh_kwargs=extra,
                                         token_updater=self._write_config)
    def _write_config(self, token):
        LOGGER.info("Credentials Refreshed")

        # Update config at config_path
        with open(self.config_path) as file:
            config = json.load(file)

        config['refresh_token'] = token['refresh_token']
        config['access_token'] = token['access_token']
        with open(self.config_path, 'w') as file:
            json.dump(config, file, indent=2)

    @backoff.on_exception(backoff.expo, Timeout, max_tries=5, factor=2)
    @backoff.on_exception(backoff.constant,
                          (QuickbooksTooManyRequestsError,
                           Quickbooks5XXException,
                           Quickbooks4XXException,
                           requests.ConnectionError),
                          max_tries=3,
                          interval=60)
    @singer.utils.ratelimit(495, 60)
    def _make_request(self, method, endpoint, headers=None, params=None, data=None):
        # Sandbox requests need to be made against the Sandbox endpoint base
        if self.sandbox:
            full_url = SANDBOX_ENDPOINT_BASE + endpoint
        else:
            full_url = PROD_ENDPOINT_BASE + endpoint

        full_url = full_url.format(realm_id=self.realm_id)
        LOGGER.info(
            "%s - Making request to %s endpoint %s, with params %s",
            full_url,
            method.upper(),
            endpoint,
            params,
        )

        default_headers = {
            'Accept': 'application/json',
            'User-Agent': self.user_agent
        }

        if headers:
            headers = {**default_headers, **headers}
        else:
            headers = {**default_headers}
        # Set request timeout to config param `request_timeout` value.
        request_timeout = self.config.get('request_timeout')
        # if request_timeout is other than 0,"0" or "" then use request_timeout
        if request_timeout and float(request_timeout):
            request_timeout = float(request_timeout)
        else: # If value is 0,"0" or "" then set default to 300 seconds.
            request_timeout = REQUEST_TIMEOUT
        response = self.session.request(method, full_url, headers=headers, params=params, data=data, timeout = request_timeout)

        # TODO: Check error status, rate limit, etc.
        if response.status_code != 200:
            raise_for_error(response)

        return response.json()


    def get(self, url, headers=None, params=None):
        return self._make_request("GET", url, headers=headers, params=params)
