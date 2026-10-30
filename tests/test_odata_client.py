import unittest

import requests

from sales_pipeline.odata_client import ODataClient, ODataError

BASE = "https://example.org/Svc.svc"


class FakeResponse:
    def __init__(self, payload, status=200, json_error=False):
        self._payload, self.status_code, self._json_error = payload, status, json_error

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code}", response=self)

    def json(self):
        if self._json_error:
            raise ValueError("not json")
        return self._payload


class FakeSession:
    def __init__(self, responses):
        self.responses, self.urls = list(responses), []

    def get(self, url, timeout=None):
        self.urls.append(url)
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def client(responses, **kw):
    session = FakeSession(responses)
    sleeps = []
    kw.setdefault("sleep", sleeps.append)  # never really sleep in tests
    c = ODataClient(BASE, session=session, **kw)
    c.sleeps = sleeps
    return c, session


class PagingTests(unittest.TestCase):
    def test_follows_next_link_until_exhausted(self):
        c, s = client([
            FakeResponse({"value": [{"id": 1}, {"id": 2}], "@odata.nextLink": f"{BASE}/Things?$skiptoken=2"}),
            FakeResponse({"value": [{"id": 3}]}),
        ])
        self.assertEqual([r["id"] for r in c.iter_records("Things")], [1, 2, 3])
        self.assertEqual(len(s.urls), 2)
        self.assertTrue(s.urls[1].endswith("$skiptoken=2"))

    def test_query_options_keep_dollar_sign_unescaped(self):
        c, s = client([FakeResponse({"value": []})])
        list(c.iter_records("Products", {"$expand": "Category", "$orderby": "ProductID"}))
        self.assertEqual(s.urls[0], f"{BASE}/Products?$expand=Category&$orderby=ProductID")

    def test_relative_next_link_is_resolved(self):
        c, s = client([
            FakeResponse({"value": [{"id": 1}], "@odata.nextLink": "Things?$skiptoken=1"}),
            FakeResponse({"value": []}),
        ])
        list(c.iter_records("Things"))
        self.assertEqual(s.urls[1], f"{BASE}/Things?$skiptoken=1")

    def test_http_scheme_in_next_link_is_normalised_to_base_scheme(self):
        c, s = client([
            FakeResponse({"value": [], "@odata.nextLink": "http://example.org/Svc.svc/Things?$skiptoken=9"}),
            FakeResponse({"value": []}),
        ])
        list(c.iter_records("Things"))
        self.assertTrue(s.urls[1].startswith("https://example.org/"))

    def test_next_link_to_other_host_is_refused(self):
        c, _ = client([FakeResponse({"value": [], "@odata.nextLink": "https://evil.example.net/x"})])
        with self.assertRaises(ODataError):
            list(c.iter_records("Things"))

    def test_endless_paging_is_stopped(self):
        loop = {"value": [{"id": 1}], "@odata.nextLink": f"{BASE}/Things?$skiptoken=1"}
        c, _ = client([FakeResponse(loop) for _ in range(10)], max_pages=3)
        with self.assertRaises(ODataError):
            list(c.iter_records("Things"))


class PageRetryTests(unittest.TestCase):
    OK = {"value": [{"id": 1}]}

    def test_transient_503_then_success_returns_data_with_exponential_backoff(self):
        c, s = client([FakeResponse({}, status=503), FakeResponse({}, status=503), FakeResponse(self.OK)])
        self.assertEqual(list(c.iter_records("Things")), [{"id": 1}])
        self.assertEqual(c.sleeps, [2.0, 4.0])

    def test_connection_error_is_retried(self):
        c, _ = client([requests.ConnectionError("reset"), FakeResponse(self.OK)])
        self.assertEqual(len(list(c.iter_records("Things"))), 1)

    def test_body_read_failure_is_retried(self):
        # the gap urllib3's Retry does not cover: connection drops WHILE the body is streamed
        c, _ = client([requests.exceptions.ChunkedEncodingError("IncompleteRead"), FakeResponse(self.OK)])
        self.assertEqual(len(list(c.iter_records("Things"))), 1)

    def test_truncated_json_is_retried(self):
        c, _ = client([FakeResponse(None, json_error=True), FakeResponse(self.OK)])
        self.assertEqual(len(list(c.iter_records("Things"))), 1)

    def test_timeout_is_retried(self):
        c, _ = client([requests.ReadTimeout("slow"), FakeResponse(self.OK)])
        self.assertEqual(len(list(c.iter_records("Things"))), 1)

    def test_client_error_404_is_not_retried(self):
        c, s = client([FakeResponse({}, status=404)])
        with self.assertRaises(requests.HTTPError):
            list(c.iter_records("Things"))
        self.assertEqual((len(s.urls), c.sleeps), (1, []))

    def test_failure_on_a_later_page_does_not_restart_earlier_pages(self):
        page1 = {"value": [{"id": 1}], "@odata.nextLink": f"{BASE}/Things?$skiptoken=1"}
        c, s = client([FakeResponse(page1), FakeResponse({}, status=502), FakeResponse({"value": [{"id": 2}]})])
        self.assertEqual([r["id"] for r in c.iter_records("Things")], [1, 2])
        self.assertEqual(s.urls.count(f"{BASE}/Things"), 1)  # page 1 fetched exactly once


class ErrorHandlingTests(unittest.TestCase):
    def test_missing_value_list_raises(self):
        c, _ = client([FakeResponse({"oops": 1})])
        with self.assertRaises(ODataError):
            list(c.iter_records("Things"))

    def test_non_json_raises_after_all_attempts(self):
        c, s = client([FakeResponse(None, json_error=True) for _ in range(4)])
        with self.assertRaises(ODataError):
            list(c.iter_records("Things"))
        self.assertEqual(len(s.urls), 4)

    def test_persistent_server_error_raises_after_all_attempts(self):
        c, s = client([FakeResponse({}, status=500) for _ in range(4)])
        with self.assertRaises(requests.HTTPError):
            list(c.iter_records("Things"))
        self.assertEqual(len(s.urls), 4)

    def test_default_session_has_retries_for_transient_errors(self):
        adapter = ODataClient(BASE).session.get_adapter("https://example.org")
        retry = adapter.max_retries
        self.assertEqual(retry.total, 5)
        self.assertIn(503, retry.status_forcelist)


if __name__ == "__main__":
    unittest.main()
