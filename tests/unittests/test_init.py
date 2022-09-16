from singer.catalog import Catalog
import unittest
from unittest import mock
from tap_quickbooks import main, do_discover
from parameterized import parameterized

class Parse_Args:
    """Mocked parse args"""

    def __init__(self, discover=False, state=None, catalog=None, properties=None, config_path=None) -> None:
        self.discover = discover
        self.state = state
        self.config = {}
        self.catalog = catalog
        self.properties = properties
        self.config_path = config_path

class TestQuickbooksInit(unittest.TestCase):

    @parameterized.expand([  # test_name, [discover_flag, catalog_flag], [do_discover, do_sync]
        ['do_discover_called', [True, True], [True, False]],
        ['do_sync_called', [False, True], [False, True]],
        ['do_discover_and_do_sync_called', [False, False], [True, True]]
    ])
    @mock.patch('singer.parse_args')
    @mock.patch('tap_quickbooks.write_catalog')
    @mock.patch('tap_quickbooks.do_discover')
    @mock.patch('tap_quickbooks.do_sync')
    @mock.patch('tap_quickbooks.QuickbooksClient')
    def test_init(self, test_name, test_data, exp, mocked_client, mocked_sync, mocked_discover, mock_catalog, mock_args):
        """Test init with various flag scenarios"""

        # Mock parse_args and return value
        mock_args.return_value = Parse_Args(
            discover=test_data[0], catalog=test_data[1])

        main()
        self.assertEqual(mocked_discover.called, exp[0])
        self.assertEqual(mocked_sync.called, exp[1])

    def test_discover_coverage(self):
        """Test catalog obtained from discover is the instance of singer Catalog"""

        catalog = do_discover()
        self.assertIsInstance(catalog, Catalog)

    @mock.patch('singer.parse_args')
    @mock.patch('tap_quickbooks.QuickbooksClient')
    def test_init_exception(self, mock_client, mock_args):
        """Test init raises an exception on passing properties flag """

        # Mock parse_args and return value
        mock_args.return_value = Parse_Args(catalog=False, properties=True)

        # Verify we raise expected error
        with self.assertRaises(Exception) as e:
            main()

        expected_message = "DEPRECATED: Use of the 'properties' parameter is not supported. Please use --catalog instead"

        # Verify the error message
        self.assertEqual(str(e.exception), expected_message)
