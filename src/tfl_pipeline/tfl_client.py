"""Thin client for the TfL Unified API (https://api.tfl.gov.uk)."""

import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException
from urllib3.util.retry import Retry

from .config import TFL_BASE_URL


class TflClient:
    """Client for the TfL Unified API."""

    def __init__(self, app_key: str | None = None, timeout: float = 30.0):
        self.app_key = app_key
        self.timeout = timeout
        self.session = requests.Session()
        # Retry temporary failures (rate limiting, server errors) with exponential backoff.
        retries = Retry(
            total=3,
            backoff_factor=2,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
        )
        self.session.mount("http://", HTTPAdapter(max_retries=retries))
        self.session.mount("https://", HTTPAdapter(max_retries=retries))

    def _get(self, endpoint: str, params: dict | None = None) -> list[dict]:
        """Make a GET request to the TfL API. `endpoint` must start with '/'."""
        url = f"{TFL_BASE_URL}{endpoint}"
        # Copy so the caller's dict is never modified.
        params = dict(params or {})
        if self.app_key:
            params["app_key"] = self.app_key
        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except RequestException as e:
            raise RuntimeError(f"Error making request to {url}: {e}") from e

    def arrivals(self, mode: str) -> list[dict]:
        """Get arrival information for a specific mode."""
        return self._get(f"/Mode/{mode}/Arrivals", {"count": -1})

    def line_status(self, modes: tuple[str, ...]) -> list[dict]:
        """Get the current status of every line in the given modes."""
        return self._get(f"/Line/Mode/{','.join(modes)}/Status")
