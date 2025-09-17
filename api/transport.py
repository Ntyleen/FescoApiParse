"""Transport layer for FESCO API interactions."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Mapping, Optional

import aiohttp

from config.settings import Config
from utils.logging import get_logger
from utils.messages import msg
from utils.metrics import API_REQUESTS, API_RETRIES


class TransportError(Exception):
    """Base class for transport related exceptions."""

    def __init__(self, message: str, *, status: Optional[int] = None):
        super().__init__(message)
        self.status = status


class TransportClientError(TransportError):
    """Raised for HTTP 4xx responses that should not be retried."""


class TransportAuthError(TransportClientError):
    """Authentication or authorisation failure."""


class TransportServerError(TransportError):
    """Raised for HTTP 5xx responses. These are retryable."""


class TransportContentTypeError(TransportError):
    """Raised when a response does not contain JSON payload."""


@dataclass(slots=True)
class TransportResponse:
    """Successful response returned by :class:`TransportLayer`."""

    status: int
    headers: Mapping[str, str]
    data: Any


class TransportLayer:
    """Low-level HTTP layer built on top of :mod:`aiohttp`."""

    def __init__(self, config: Config):
        self._config = config
        self._logger = get_logger("fesco_tracker.transport")
        self._base_url = config.api.base_url.rstrip("/") + "/"
        self._timeout = aiohttp.ClientTimeout(total=config.api.timeout_seconds)
        self._retry_attempts = config.api.retry_attempts
        self._retry_backoff = config.api.retry_backoff_seconds
        self._headers = {
            "Authorization": f"{config.api.token_type} {config.auth_token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": config.api.user_agent,
        }

    # ------------------------------------------------------------------
    def create_connector(self) -> aiohttp.TCPConnector:
        """Return configured :class:`aiohttp.TCPConnector`."""

        return aiohttp.TCPConnector(
            limit_per_host=self._config.api.max_parallel,
            ttl_dns=self._config.api.dns_cache_ttl,
            keepalive_timeout=self._config.api.keepalive_timeout,
        )

    # ------------------------------------------------------------------
    async def get_json(
        self,
        session: aiohttp.ClientSession,
        endpoint: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
    ) -> TransportResponse:
        """Perform an HTTP GET returning a JSON payload."""

        return await self._request_with_retry(session, "GET", endpoint, params=params)

    # ------------------------------------------------------------------
    async def _request_with_retry(
        self,
        session: aiohttp.ClientSession,
        method: str,
        endpoint: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
    ) -> TransportResponse:
        url = endpoint if endpoint.startswith("http") else f"{self._base_url}{endpoint.lstrip('/') }"
        attempt = 0
        last_error: Exception | None = None
        while attempt < self._retry_attempts:
            attempt += 1
            self._logger.debug(
                msg(
                    "api.request.attempt",
                    attempt=attempt,
                    method=method,
                    endpoint=url,
                )
            )
            try:
                response = await self._request_once(session, method, url, params=params)
                self._logger.info(
                    msg("api.request.success", status=response.status, endpoint=url)
                )
                return response
            except TransportServerError as exc:  # retryable
                last_error = exc
                if attempt >= self._retry_attempts:
                    raise
                delay = self._retry_backoff * (2 ** (attempt - 1))
                self._logger.warning(
                    msg("api.request.retry", delay=delay, reason=str(exc))
                )
                API_RETRIES.inc()
                await asyncio.sleep(delay)
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_error = exc
                if attempt >= self._retry_attempts:
                    API_REQUESTS.labels(status="network_error").inc()
                    raise TransportError(str(exc)) from exc
                delay = self._retry_backoff * (2 ** (attempt - 1))
                self._logger.warning(
                    msg("api.request.retry", delay=delay, reason=str(exc))
                )
                API_RETRIES.inc()
                await asyncio.sleep(delay)
            except TransportClientError:
                API_REQUESTS.labels(status="client_error").inc()
                raise
            except TransportError as exc:
                last_error = exc
                if attempt >= self._retry_attempts:
                    API_REQUESTS.labels(status="error").inc()
                    raise
                delay = self._retry_backoff * (2 ** (attempt - 1))
                self._logger.warning(
                    msg("api.request.retry", delay=delay, reason=str(exc))
                )
                API_RETRIES.inc()
                await asyncio.sleep(delay)

        if last_error:
            API_REQUESTS.labels(status="error").inc()
            raise TransportError(str(last_error)) from last_error
        API_REQUESTS.labels(status="error").inc()
        raise TransportError("Unknown transport error")

    # ------------------------------------------------------------------
    async def _request_once(
        self,
        session: aiohttp.ClientSession,
        method: str,
        url: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
    ) -> TransportResponse:
        async with session.request(
            method,
            url,
            params=params,
            headers=self._headers,
            timeout=self._timeout,
        ) as response:
            if response.status == 401 or response.status == 403:
                text = await response.text()
                API_REQUESTS.labels(status=str(response.status)).inc()
                raise TransportAuthError(text, status=response.status)
            if response.status >= 500:
                text = await response.text()
                API_REQUESTS.labels(status=str(response.status)).inc()
                raise TransportServerError(text, status=response.status)
            if response.status >= 400:
                text = await response.text()
                API_REQUESTS.labels(status=str(response.status)).inc()
                raise TransportClientError(text, status=response.status)

            content_type = response.headers.get("content-type", "").lower()
            if "application/json" not in content_type:
                text = await response.text()
                API_REQUESTS.labels(status="invalid_content").inc()
                raise TransportContentTypeError(
                    f"Unexpected Content-Type: {content_type}", status=response.status
                )

            data = await response.json()
            API_REQUESTS.labels(status=str(response.status)).inc()
            return TransportResponse(
                status=response.status,
                headers=dict(response.headers),
                data=data,
            )
