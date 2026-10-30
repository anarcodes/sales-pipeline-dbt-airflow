"""Small OData V4 client: retries with backoff, timeouts, server-driven paging.

Pagination follows `@odata.nextLink`. The link is only followed if it stays on the same host as
the configured base URL, so a compromised/misbehaving response cannot redirect our requests.
"""
from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterator, Mapping
from typing import Any
from urllib.parse import quote, urlencode, urljoin, urlparse, urlunparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

NEXT_LINK = "@odata.nextLink"
RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})


class ODataError(RuntimeError):
    """The API answered, but not in a way we can use."""


class ODataTransientError(ODataError):
    """Looks like a temporary problem (e.g. truncated body); worth another attempt."""


def is_transient(exc: Exception) -> bool:
    """Network hiccups, timeouts, broken/truncated bodies, 429/5xx -> retry. 4xx and contract errors -> don't."""
    if isinstance(exc, ODataTransientError):
        return True
    if isinstance(exc, requests.HTTPError):
        return getattr(exc.response, "status_code", None) in RETRYABLE_STATUS
    return isinstance(
        exc,
        (
            requests.ConnectionError,
            requests.Timeout,
            requests.exceptions.ChunkedEncodingError,
            requests.exceptions.RetryError,
        ),
    )


class ODataClient:
    def __init__(
        self,
        base_url: str,
        timeout: tuple[float, float] = (10.0, 60.0),
        session: requests.Session | None = None,
        max_pages: int = 1000,
        max_attempts: int = 4,
        backoff_seconds: float = 2.0,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self._base = urlparse(self.base_url)
        self.timeout = timeout
        self.max_pages = max_pages  # circuit breaker against endless paging loops
        self.max_attempts = max_attempts  # per PAGE (a failure on page 37 does not restart page 1)
        self.backoff_seconds = backoff_seconds
        self._sleep = sleep
        self.session = session or self._build_session()

    @staticmethod
    def _build_session() -> requests.Session:
        retry = Retry(
            total=5,
            backoff_factor=1.0,  # sleeps 0s, 2s, 4s, 8s, ... between attempts
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET"}),
            respect_retry_after_header=True,
        )
        session = requests.Session()
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.headers.update(
            {"Accept": "application/json", "User-Agent": "sales-pipeline/1.0"}
        )
        return session

    @staticmethod
    def build_query(params: Mapping[str, str] | None) -> str:
        """Encode query options but keep `$`, `,`, `(`, `)` readable (OData system options)."""
        if not params:
            return ""
        return urlencode(params, safe="$,()'/", quote_via=quote)

    def iter_records(
        self, entity_set: str, params: Mapping[str, str] | None = None
    ) -> Iterator[dict[str, Any]]:
        """Yield every record of an entity set, following server-driven paging."""
        url: str | None = f"{self.base_url}/{entity_set}"
        query = self.build_query(params)
        if query:
            url = f"{url}?{query}"
        page = 0
        while url:
            page += 1
            if page > self.max_pages:
                raise ODataError(f"{entity_set}: more than {self.max_pages} pages, aborting")
            payload = self._get_json(url)
            records = payload.get("value")
            if not isinstance(records, list):
                raise ODataError(f"{entity_set}: response has no 'value' list (url={url})")
            logger.info("%s: page %d -> %d records", entity_set, page, len(records))
            yield from records
            url = self._resolve_next_link(payload.get(NEXT_LINK), url)

    def _get_json(self, url: str) -> dict[str, Any]:
        """GET one page with application-level retries.

        urllib3's Retry (see _build_session) only covers the connection/status phase. Errors while the
        response BODY is being read (ChunkedEncodingError, truncated JSON) bypass it, so we retry here too.
        """
        for attempt in range(1, self.max_attempts + 1):
            try:
                return self._get_json_once(url)
            except Exception as exc:
                if not is_transient(exc) or attempt == self.max_attempts:
                    raise
                delay = self.backoff_seconds * 2 ** (attempt - 1)
                logger.warning(
                    "Transient error on %s (attempt %d/%d): %s: %s - retrying in %.0fs",
                    url, attempt, self.max_attempts, type(exc).__name__, exc, delay,
                )
                self._sleep(delay)
        raise AssertionError("unreachable")  # pragma: no cover

    def _get_json_once(self, url: str) -> dict[str, Any]:
        response = self.session.get(url, timeout=self.timeout)
        response.raise_for_status()
        try:
            payload = response.json()
        except ValueError as exc:
            raise ODataTransientError(f"Non-JSON/truncated response from {url}") from exc
        if not isinstance(payload, dict):
            raise ODataError(f"Unexpected JSON root type from {url}: {type(payload).__name__}")
        return payload

    def _resolve_next_link(self, link: str | None, current_url: str) -> str | None:
        if not link:
            return None
        absolute = urlparse(urljoin(current_url, link))
        if absolute.hostname != self._base.hostname:
            raise ODataError(f"Refusing to follow nextLink to another host: {absolute.hostname}")
        # Servers behind proxies sometimes emit http:// links although we called https://.
        return urlunparse(absolute._replace(scheme=self._base.scheme))
