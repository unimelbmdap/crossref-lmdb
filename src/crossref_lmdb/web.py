"""
Make web requests with rate limiting and retrying.
"""

import collections.abc
import functools
import logging

import requests
import requests.utils
import requests_ratelimiter
import pyrate_limiter
import retryhttp  # type: ignore[import-untyped]

import crossref_lmdb

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

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

        self.retry_wrapper = retryhttp.retry(
            max_attempt_number=self.max_retry_attempts
        )

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

        self.default_limit = pyrate_limiter.Limiter(
            pyrate_limiter.RequestRate(
                limit=50,
                interval=1
            )
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

    def get_rate_limit(self) -> pyrate_limiter.Limiter:
        """
        Determine the API rate limits by sending a test request and inspecting
        the response headers.

        Returns
        -------
            A limiter with the settings applied.
        """

        default_msg = "Rate limit could not be identified; using defaults"

        try:
            response = self._session.get(url=self.base_url)
        except Exception:
            LOGGER.warning(default_msg)
            return self.default_limit

        if (
            "x-ratelimit-limit" not in response.headers
            or "x-ratelimit-interval" not in response.headers
        ):
            LOGGER.warning(default_msg)
            return self.default_limit

        n_calls = int(response.headers["x-ratelimit-limit"])
        period_s = int(response.headers["x-ratelimit-interval"][:-1])

        limit = pyrate_limiter.Limiter(
            pyrate_limiter.RequestRate(
                limit=n_calls,
                interval=period_s
            )
        )

        LOGGER.info(
            f"Set CrossRef rate limits to {n_calls} calls per {period_s} s"
        )

        return limit

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
