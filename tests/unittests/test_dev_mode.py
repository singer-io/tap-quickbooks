import json
from datetime import timedelta
from singer.utils import now, strftime
import unittest
from unittest.mock import patch, MagicMock
import os
from tap_quickbooks.client import QuickbooksClient
import singer
LOGGER = singer.get_logger()


class Test_ClientDevMode(unittest.TestCase):
    """Test the dev mode functionality."""
    def setUp(self):
        """Creates a sample config for test execution"""
        # Data to be written
        self.mock_config = {
                "user_agent": "test_user_agent",
                "access_token": "sample_access_token",
                "refresh_token": "sample_refresh_token",
                "client_id": "sample_client_id",
                "client_secret": "sample_client_secret",
                "realm_id": "1234567890",
                "expires_at": strftime(now() + timedelta(hours=1))
                }
        self.tmp_config_filename = "sample_quickbooks_config.json"

        # Serializing json
        json_object = json.dumps(self.mock_config, indent=4)
        # Writing to sample_quickbooks_config.json
        with open(self.tmp_config_filename, "w") as outfile:
            outfile.write(json_object)

    def tearDown(self):
        """Deletes the sample config"""
        if os.path.isfile(self.tmp_config_filename):
            os.remove(self.tmp_config_filename)

    @patch("tap_quickbooks.client.QuickbooksClient._write_config")
    @patch("requests_oauthlib.OAuth2Session.request", return_value=MagicMock(status_code=200))
    def test_client_with_dev_mode(self, mock_request, mock_write_config):
        """Checks the dev mode implementation and verifies write config functionality is not called"""
        params = {"config_path": self.tmp_config_filename, "config": self.mock_config, "dev_mode": True}
        QuickbooksClient(**params)

        # _write_config function should never be called as it will update the config
        self.assertEquals(mock_write_config.call_count, 0)

    @patch("requests_oauthlib.OAuth2Session.request", return_value=MagicMock(status_code=200))
    def test_client_dev_mode_missing_access_token(self, mock_request):
        """Exception should be raised if missing access token"""

        del self.mock_config["access_token"]
        params = {"config_path": self.tmp_config_filename, "config": self.mock_config, "dev_mode": True}

        with self.assertRaises(Exception):
            QuickbooksClient(**params)
