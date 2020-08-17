import json
import backoff
import requests
import singer

from requests_oauthlib import OAuth2Session


LOGGER = singer.get_logger()

PROD_ENDPOINT_BASE = "https://quickbooks.api.intuit.com"
SANDBOX_ENDPOINT_BASE = "https://sandbox-quickbooks.api.intuit.com"
TOKEN_REFRESH_URL = 'https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer'


class QuickbooksAuthenticationError(Exception):
    pass


class QuickbooksClient():
    def __init__(self, config_path, config):
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

        if config['sandbox'] in ['true', 'True', True]:
            self.sandbox = True

        self.user_agent = config['user_agent']
        self.realm_id = config['realm_id']
        self.config_path = config_path
        self.session = OAuth2Session(config['client_id'],
                                     token=token,
                                     auto_refresh_url=TOKEN_REFRESH_URL,
                                     auto_refresh_kwargs=extra,
                                     token_updater=self._write_config)
        try:
            # Make an authenticated request after creating the object to any endpoint
            self.get('/v3/company/{}/query'.format(self.realm_id), params={"query": "SELECT * FROM CompanyInfo"})
        except Exception as e:
            LOGGER.info("Error initializing QuickbooksClient during token refresh, please reauthenticate.")
            raise QuickbooksAuthenticationError(e)

    def _write_config(self, token):
        LOGGER.info("Credentials Refreshed")

        # Update config at config_path
        with open(self.config_path) as file:
            config = json.load(file)

        config['refresh_token'] = token['refresh_token']

        with open(self.config_path, 'w') as file:
            json.dump(config, file, indent=2)


    @backoff.on_exception(backoff.constant,
                          (requests.exceptions.HTTPError),
                          max_tries=3,
                          interval=10)
    def _make_request(self, method, endpoint, headers=None, params=None, data=None):
        # Sandbox requests need to be made against the Sandbox endpoint base
        if self.sandbox:
            full_url = SANDBOX_ENDPOINT_BASE + endpoint
        else:
            full_url = PROD_ENDPOINT_BASE + endpoint

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

        response = self.session.request(method, full_url, headers=headers, params=params, data=data)

        response.raise_for_status()

        # TODO: Check error status, rate limit, etc.
        return response.json()


    def get(self, url, headers=None, params=None):
        return self._make_request("GET", url, headers=headers, params=params)
