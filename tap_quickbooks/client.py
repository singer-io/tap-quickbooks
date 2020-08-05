import json
import backoff
import requests
import singer

from requests_oauthlib import OAuth2Session

LOGGER = singer.get_logger()
PROD_ENDPOINT_BASE = "https://quickbooks.api.intuit.com"
SANDBOX_ENDPOINT_BASE = "https://sandbox-quickbooks.api.intuit.com"
TOKEN_REFRESH_URL = 'https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer'

def get_auth_header(client_id, client_secret):
    """Gets authorization header 
    
    :param client_id: Client ID
    :param client_secret: Client Secret
    :return: Authorization header
    """
    from base64 import b64encode

    auth_header = '{0}:{1}'.format(client_id, client_secret)

    # we use py3
    auth_header = auth_header.encode('utf-8')
    return ' '.join(['Basic', b64encode(auth_header).decode('utf-8')])


class OurOAuth2Session(OAuth2Session):
    def refresh_token(
        self,
        token_url,
        refresh_token=None,
        body="",
        auth=None,
        timeout=None,
        headers=None,
        verify=True,
        proxies=None,
        **kwargs
    ):
        refresh_token = refresh_token or self.token.get("refresh_token")
        client_id = self.auto_refresh_kwargs['client_id']
        client_secret = self.auto_refresh_kwargs['client_secret']
        import ipdb; ipdb.set_trace()
        1+1
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': get_auth_header(client_id, client_secret)
        }

        body = {
            'grant_type': 'refresh_token',
            'refresh_token': refresh_token
        }

        #send_request('POST', self.token_endpoint, headers, self, body=urlencode(body), session=self)
        from urllib.parse import urlencode
        
        r = self.post(
            token_url,
            data=urlencode(body),
            timeout=timeout,
            headers=headers,
            verify=verify,
            withhold_token=True,
            proxies=proxies,
        )
        self.token = self._client.parse_request_body_response(r.text, scope=self.scope)
        if not "refresh_token" in self.token:
            log.debug("No new refresh token given. Re-using old.")
            self.token["refresh_token"] = refresh_token
        return self.token

    
class QuickbooksAuthenticationError(Exception):
    pass


class QuickbooksClient():
    def __init__(self, config_path, config):
        token = {
            'access_token': "wrong",
            'refresh_token': config['refresh_token'],
            'token_type': 'Bearer',
            # Set expires_in to a negative number to force the client to reauthenticate
            'expires_in': '-30'
        }
        extra = {
            'client_id': config['client_id'],
            'client_secret': config['client_secret']
        }
        self.realm_id = config['realm_id']
        self.config_path = config_path
        self.session = OurOAuth2Session(config['client_id'],
                                     token=token,
                                     auto_refresh_url=TOKEN_REFRESH_URL,
                                     auto_refresh_kwargs=extra,
                                     token_updater=self._write_config)
        try:
            # Make an authenticated request after creating the object to any endpoint
            resp = self.get('/v3/company/{}/companyinfo'.format(self.realm_id)).get('CompanyInfo', {})
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
        full_url = PROD_ENDPOINT_BASE + endpoint
        LOGGER.info(
            "%s - Making request to %s endpoint %s, with params %s",
            full_url,
            method.upper(),
            endpoint,
            params,
        )

        # TODO: We should merge headers with some default headers like user_agent
        response = self.session.request(method, full_url, headers=headers, params=params, data=data)

        import ipdb; ipdb.set_trace()
        1+1
        response.raise_for_status()
        # TODO: Check error status, rate limit, etc.
        return response.json()


    def get(self, url, headers=None, params=None):
        return self._make_request("GET", url, headers=headers, params=params)
