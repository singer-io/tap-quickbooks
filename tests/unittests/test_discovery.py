import unittest
from unittest import mock

from tap_quickbooks.client import QuickbooksForbiddenError
from tap_quickbooks.discover import _apply_access_checks, do_discover


class _AllowedStream:
    def __init__(self, client=None, config=None, state=None):
        del client, config, state

    def check_access(self):
        return True


class _DeniedStream:
    def __init__(self, client=None, config=None, state=None):
        del client, config, state

    def check_access(self):
        return False


class TestDiscoveryAccessChecks(unittest.TestCase):
    def test_apply_access_checks_removes_inaccessible_streams(self):
        schemas = {
            'allowed_stream': {'type': 'object'},
            'denied_stream': {'type': 'object'},
        }
        field_metadata = {
            'allowed_stream': {'meta': 1},
            'denied_stream': {'meta': 1},
        }

        stream_map = {
            'allowed_stream': _AllowedStream,
            'denied_stream': _DeniedStream,
        }

        fake_client = mock.Mock()
        fake_client.config = {'start_date': '2022-01-01T00:00:00Z'}

        with mock.patch('tap_quickbooks.discover.STREAMS', stream_map):
            _apply_access_checks(fake_client, schemas, field_metadata)

        self.assertEqual(set(schemas.keys()), {'allowed_stream'})
        self.assertEqual(set(field_metadata.keys()), {'allowed_stream'})

    def test_apply_access_checks_raises_when_all_denied(self):
        schemas = {'denied_stream': {'type': 'object'}}
        field_metadata = {'denied_stream': {'meta': 1}}

        stream_map = {'denied_stream': _DeniedStream}

        fake_client = mock.Mock()
        fake_client.config = {'start_date': '2022-01-01T00:00:00Z'}

        with mock.patch('tap_quickbooks.discover.STREAMS', stream_map):
            with self.assertRaises(QuickbooksForbiddenError):
                _apply_access_checks(fake_client, schemas, field_metadata)

    def test_do_discover_returns_catalog(self):
        fake_client = mock.Mock()
        fake_client.config = {'start_date': '2022-01-01T00:00:00Z'}

        with mock.patch('tap_quickbooks.discover._apply_access_checks'):
            catalog = do_discover(fake_client)

        self.assertGreater(len(catalog.streams), 0)

    def test_do_discover_skips_access_checks_when_disabled(self):
        fake_client = mock.Mock()
        fake_client.config = {'start_date': '2022-01-01T00:00:00Z'}

        with mock.patch('tap_quickbooks.discover._apply_access_checks') as mock_check:
            catalog = do_discover(fake_client, check_access=False)

        mock_check.assert_not_called()
        self.assertGreater(len(catalog.streams), 0)

    def test_do_discover_runs_access_checks_by_default(self):
        fake_client = mock.Mock()
        fake_client.config = {'start_date': '2022-01-01T00:00:00Z'}

        with mock.patch('tap_quickbooks.discover._apply_access_checks') as mock_check:
            do_discover(fake_client)

        mock_check.assert_called_once()
