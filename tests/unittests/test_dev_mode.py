import json
from datetime import timedelta
from singer.utils import now, strftime
import unittest
from unittest.mock import patch, MagicMock

from tap_quickbooks.client import QuickbooksClient


class Test_ClientDevMode(unittest.TestCase):
    """Test the dev mode functionality."""

    # Data to be written
    mock_config = {
    "access_token": "sample_access_token",
    "refresh_token": "sample_refresh_token",
    "client_id": "sample_client_id",
    "client_secret": "sample_client_secret",
    "expires_at": strftime(now() + timedelta(hours=1))
    }
    # Serializing json
    json_object = json.dumps(mock_config, indent=4)
    tmp_config_filename = "sample_quickbooks_config.json"

    # Writing to sample_quickbooks_config.json
    with open(tmp_config_filename, "w") as outfile:
        outfile.write(json_object)

    @patch("tap_quickbooks.client.QuickbooksClient._write_config")
    @patch("requests_oauthlib.OAuth2Session.request", return_value=MagicMock(status_code=200))
    def test_client_with_dev_mode(self, mock_request, mock_write_config):
        """Checks the dev mode implementation works with existing token"""

        params = {"config_path": self.tmp_config_filename, "config": self.mock_config, "dev_mode": True}

        QuickbooksClient(**params)

        # _write_config function should never be called as it will update the config
        self.assertEquals(mock_write_config.call_count, 0)
