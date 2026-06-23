import unittest
from unittest import mock

from tap_quickbooks.client import QuickbooksForbiddenError
from tap_quickbooks.discover import _apply_access_checks, do_discover


class _AllowedStream:
    parent = None

    def __init__(self, client=None, config=None, state=None):
        del client, config, state

    def check_access(self):
        return True


class _DeniedStream:
    parent = None

    def __init__(self, client=None, config=None, state=None):
        del client, config, state

    def check_access(self):
        return False


class _ChildStream:
    parent = 'denied_parent'

    def __init__(self, client=None, config=None, state=None):
        del client, config, state

    def check_access(self):
        return True


class TestDiscoveryAccessChecks(unittest.TestCase):
    def test_apply_access_checks_removes_inaccessible_and_children(self):
        schemas = {
            'allowed_parent': {'type': 'object'},
            'denied_parent': {'type': 'object'},
            'child_stream': {'type': 'object'},
        }
        field_metadata = {
            'allowed_parent': {'meta': 1},
            'denied_parent': {'meta': 1},
            'child_stream': {'meta': 1},
        }

        stream_map = {
            'allowed_parent': _AllowedStream,
            'denied_parent': _DeniedStream,
            'child_stream': _ChildStream,
        }

        fake_client = mock.Mock()
        fake_client.config = {'start_date': '2022-01-01T00:00:00Z'}

        with mock.patch('tap_quickbooks.discover.STREAMS', stream_map):
            _apply_access_checks(fake_client, schemas, field_metadata)

        self.assertEqual(set(schemas.keys()), {'allowed_parent'})
        self.assertEqual(set(field_metadata.keys()), {'allowed_parent'})

    def test_apply_access_checks_raises_when_all_denied(self):
        schemas = {'denied_parent': {'type': 'object'}}
        field_metadata = {'denied_parent': {'meta': 1}}

        stream_map = {'denied_parent': _DeniedStream}

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
