"""
Make web requests with rate limiting and retrying.
"""

import collections.abc
import functools
import logging
import typing
import math

import requests
import requests.utils
import requests_ratelimiter
import pyrate_limiter
import retryhttp  # type: ignore[import-untyped]

import simdjson

import crossref_lmdb
import crossref_lmdb.items
import crossref_lmdb.filt


LOGGER = logging.getLogger("crossref_lmdb")

DEFAULT_LIMITER = pyrate_limiter.Limiter(
    pyrate_limiter.RequestRate(
        limit=60,
        interval=60,
    ),
)


class WebRequester:

    def __init__(
        self,
        limiter: pyrate_limiter.Limiter = DEFAULT_LIMITER,
        headers: dict[str, str] | None = None,
        max_delay_s: float | None = 60 * 60,
        per_host: bool = False,
        limit_statuses: collections.abc.Iterable[int] = (429, 500),
        max_retry_attempts: int = 10,
    ) -> None:
        """
        Interface for making HTTP requests with rate limiting and retrying.

        Parameters
        ----------
        limiter
            Rate limiter settings.
        headers
            Any headers to add to the request.
        max_delay_s
            Maximum time, in seconds, that a request can be delayed because of
            the retry algorithm.
        per_host
            Whether the limiter is applied to the hostname, rather than to the
            instance.
        limit_statuses
            The status codes that invoke rate limiting beyond the set limits.
        max_retry_attempts
            How many attempts at a retry before failure.
        """

        self._session = requests_ratelimiter.LimiterSession(
            limiter=limiter,
            max_delay=max_delay_s,
            per_host=per_host,
            limit_statuses=limit_statuses,
        )

        self.max_retry_attempts = max_retry_attempts

        self.retry_wrapper = retryhttp.retry(max_attempt_number=self.max_retry_attempts)

        if headers is not None:
            self._session.headers = {**self._session.headers, **headers}

    def get(self, url: str) -> requests.Response:
        """
        Perform a GET request.

        Parameters
        ----------
        url
            The URL to request.

        Returns
        -------
            The request response.
        """

        getter = functools.partial(
            self._session.get,
            timeout=60,
        )

        retry_get = self.retry_wrapper(getter)
        response: requests.Response = retry_get(url=url)

        return response


class CrossRefWebAPI:

    def __init__(
        self,
        email_address: str,
    ) -> None:
        """
        Interface to the CrossRef web API.
        """

        self.email_address = email_address

        self.base_url = "https://api.crossref.org/"

        self._session = WebRequester(
            headers={"User-Agent": self.user_agent},
        )

    @property
    def user_agent(self) -> str:
        """
        Value to send as the "User-Agent" header.
        """

        lib_name = (
            f"crossref-lmdb/{crossref_lmdb.__version__} "
            + f"({crossref_lmdb._project_url})"
            + f"; mailto:{self.email_address}"
        )

        requests_ua = requests.utils.default_headers()["User-Agent"]

        lib_name += f" {requests_ua}"

        return lib_name

    def set_rate_limit(self) -> None:
        """
        Determine the API rate limits by sending a test request and inspecting
        the response headers.
        """

        default_msg = "Rate limit could not be identified; using defaults"

        try:
            response = self._session.get(url=self.base_url)
        except Exception:
            LOGGER.warning(default_msg)
            return

        if (
            "x-ratelimit-limit" not in response.headers
            or "x-ratelimit-interval" not in response.headers
        ):
            LOGGER.warning(default_msg)
            return

        n_calls = int(response.headers["x-ratelimit-limit"])
        period_s = int(response.headers["x-ratelimit-interval"][:-1])

        limiter = pyrate_limiter.Limiter(
            pyrate_limiter.RequestRate(limit=n_calls, interval=period_s)
        )

        LOGGER.info(f"Set CrossRef rate limits to {n_calls} calls per {period_s} s")

        self._session = WebRequester(
            headers={"User-Agent": self.user_agent},
            limiter=limiter,
        )

    def call(
        self,
        query: str,
    ) -> requests.Response:
        """
        Make a call to the API.

        Parameters
        ----------
        query
            Query string, which is used to form the URL; no leading slash.

        Returns
        -------
            The API response.
        """

        url = f"{self.base_url}{query}"

        response = self._session.get(url=url)

        response.raise_for_status()

        return response


class WebSource(crossref_lmdb.items.ItemSource):

    def __init__(
        self,
        email_address: str,
        from_date: str,
        show_progress: bool,
        filter_func: crossref_lmdb.filt.FilterFunc | None,
        filter_arg: str | None,
    ) -> None:

        self.email_address = email_address
        self._show_progress = show_progress
        self._filter_func = filter_func
        self.from_date = from_date
        self.filter_arg = filter_arg

        self._total_items: int = -1

        self.n_rows = 1_000

        self.client = CrossRefWebAPI(email_address=self.email_address)

        self.client.set_rate_limit()

    @property
    def total_items(self) -> int:
        if self._total_items == -1:
            self._total_items = self._request_total_items()

        return self._total_items

    @property
    def total_units(self) -> str:
        return "pages"

    @property
    def show_progress(self) -> bool:
        return self._show_progress

    @property
    def filter_func(self) -> crossref_lmdb.filt.FilterFunc | None:
        return self._filter_func

    def _request_total_items(self) -> int:

        # do an initial brief query to get a total
        total_query = self.form_query(
            from_date=self.from_date,
            filter_arg=self.filter_arg,
            cursor="*",
            only_doi=True,
            n_rows=1,
        )

        LOGGER.info(
            f"Querying the CrossRef web API using '{total_query}' to identify "
            + "the number of update records"
        )

        total_response = self.client.call(query=total_query)

        total_message = total_response.json()["message"]

        total_results: int = total_message["total-results"]
        total_msg = (
            f"A total of {total_results:,} items have been updated "
            + f"since {self.from_date}"
        )

        LOGGER.info(total_msg)

        n_pages = math.ceil(total_results / self.n_rows)

        return n_pages

    def iter_unfiltered_items_data(self) -> typing.Iterable[bytes]:

        cursor = "*"

        more_pages = True

        while more_pages:

            query = self.form_query(
                from_date=self.from_date,
                filter_arg=self.filter_arg,
                cursor=cursor,
                n_rows=self.n_rows,
            )

            LOGGER.debug("Querying CrossRef API")
            response = self.client.call(query=query)
            LOGGER.debug("Received response from CrossRef API")

            parser = simdjson.Parser()

            data = parser.parse(response.content)

            if not isinstance(data, simdjson.Object):
                raise ValueError()

            message = data["message"]

            if not isinstance(message, simdjson.Object):
                raise ValueError()

            message_bytes = typing.cast(
                bytes,
                message.mini,
            )

            LOGGER.debug("Yielding data")

            yield message_bytes

            items = message["items"]

            if not isinstance(items, simdjson.Array):
                raise ValueError()

            n_items = len(items)

            LOGGER.debug(f"Number of items in this page: {n_items}")

            more_pages = n_items > 0

            if not more_pages:
                break

            cursor = str(message["next-cursor"])

    @staticmethod
    def form_query(
        from_date: str,
        filter_arg: str | None = None,
        n_rows: int = 1_000,
        cursor: str = "*",
        only_doi: bool = False,
        sort_results: bool = True,
    ) -> str:

        query = "works?"

        filt = f"filter=from-index-date:{from_date}"

        if filter_arg is not None:
            filt += "," + filter_arg

        rows = f"rows={n_rows}"

        cursor = f"cursor={cursor}"

        select = "select=DOI" if only_doi else None

        sort: str | None = "sort=indexed" if sort_results else None

        order: str | None = "order=asc" if sort_results else None

        params = [
            param
            for param in (filt, rows, cursor, select, sort, order)
            if param is not None
        ]

        return query + "&".join(params)
