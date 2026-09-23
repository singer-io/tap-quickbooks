import unittest
from unittest import mock

from tap_quickbooks.client import QuickbooksBadRequestError, QuickbooksForbiddenError
from tap_quickbooks.streams import Accounts, DeletedObjects, ProfitAndLossReport


class TestStreamCheckAccess(unittest.TestCase):
    def test_returns_true_when_batch_post_succeeds(self):
        client = mock.Mock()
        client.minor_version = 75
        stream = Accounts(client=client, config={'start_date': '2022-01-01T00:00:00Z'}, state={})

        self.assertTrue(stream.check_access())
        self.assertTrue(client.post.called)

    def test_returns_false_on_forbidden_error(self):
        client = mock.Mock()
        client.minor_version = 75
        client.post.side_effect = QuickbooksForbiddenError('HTTP-error-code: 403, Error: Forbidden')
        stream = Accounts(client=client, config={'start_date': '2022-01-01T00:00:00Z'}, state={})

        self.assertFalse(stream.check_access())

    def test_does_not_swallow_non_forbidden_errors(self):
        client = mock.Mock()
        client.minor_version = 75
        client.post.side_effect = QuickbooksBadRequestError('HTTP-error-code: 400, Error: Bad Request')
        stream = Accounts(client=client, config={'start_date': '2022-01-01T00:00:00Z'}, state={})

        with self.assertRaises(QuickbooksBadRequestError):
            stream.check_access()

    def test_returns_false_without_client(self):
        stream = Accounts(client=None, config={'start_date': '2022-01-01T00:00:00Z'}, state={})

        self.assertFalse(stream.check_access())


class TestReportStreamCheckAccess(unittest.TestCase):
    def test_returns_true_when_get_succeeds(self):
        client = mock.Mock()
        stream = ProfitAndLossReport(client=client, config={'start_date': '2022-01-01T00:00:00Z'}, state={})

        self.assertTrue(stream.check_access())
        self.assertTrue(client.get.called)

    def test_returns_false_on_forbidden_error(self):
        client = mock.Mock()
        client.get.side_effect = QuickbooksForbiddenError('HTTP-error-code: 403, Error: Forbidden')
        stream = ProfitAndLossReport(client=client, config={'start_date': '2022-01-01T00:00:00Z'}, state={})

        self.assertFalse(stream.check_access())

    def test_does_not_swallow_non_forbidden_errors(self):
        client = mock.Mock()
        client.get.side_effect = QuickbooksBadRequestError('HTTP-error-code: 400, Error: Bad Request')
        stream = ProfitAndLossReport(client=client, config={'start_date': '2022-01-01T00:00:00Z'}, state={})

        with self.assertRaises(QuickbooksBadRequestError):
            stream.check_access()


class TestDeletedObjectsCheckAccess(unittest.TestCase):
    def test_returns_true_when_get_succeeds(self):
        client = mock.Mock()
        stream = DeletedObjects(client=client, config={'start_date': '2022-01-01T00:00:00Z'}, state={})

        self.assertTrue(stream.check_access())
        self.assertTrue(client.get.called)

    def test_returns_false_on_forbidden_error(self):
        client = mock.Mock()
        client.get.side_effect = QuickbooksForbiddenError('HTTP-error-code: 403, Error: Forbidden')
        stream = DeletedObjects(client=client, config={'start_date': '2022-01-01T00:00:00Z'}, state={})

        self.assertFalse(stream.check_access())

    def test_does_not_swallow_non_forbidden_errors(self):
        client = mock.Mock()
        client.get.side_effect = QuickbooksBadRequestError('HTTP-error-code: 400, Error: Bad Request')
        stream = DeletedObjects(client=client, config={'start_date': '2022-01-01T00:00:00Z'}, state={})

        with self.assertRaises(QuickbooksBadRequestError):
            stream.check_access()


if __name__ == '__main__':
    unittest.main()
