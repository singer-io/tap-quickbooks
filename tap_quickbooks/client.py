import json
import backoff
import requests
import singer

from requests_oauthlib import OAuth2Session

LOGGER = singer.get_logger()
ENDPOINT_BASE = "https://services.adroll.com/api/v1/"
TOKEN_REFRESH_URL = 'https://services.adroll.com/auth/token'


class AdrollAuthenticationError(Exception):
    pass


class QuickbooksClient():
    def __init__(self, config_path, config):
        pass

    def _write_config(self, token):
        LOGGER.info("Credentials Refreshed")

        # Update config at config_path
        with open(self.config_path) as file:
            config = json.load(file)

        config['refresh_token'] = token['refresh_token']

        with open(self.config_path, 'w') as file:
            json.dump(config, file, indent=2)

