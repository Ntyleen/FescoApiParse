"""Business layer wrapping the raw transport client."""
from __future__ import annotations

from typing import Any, Mapping, Optional

import aiohttp

from cache import CacheBackend
from config.settings import Config
from models.processing_stats import ProcessingStats
from utils.logging import get_logger
from utils.messages import msg
from utils.metrics import CACHE_HITS

from .exceptions import AuthenticationError, ApiRequestError
from .transport import (
    TransportAuthError,
    TransportClientError,
    TransportContentTypeError,
    TransportError,
    TransportLayer,
    TransportResponse,
)


class BusinessLayer:
    """Encapsulates caching, statistics and error handling."""

    def __init__(
        self,
        config: Config,
        cache: CacheBackend,
        stats: ProcessingStats,
        transport: TransportLayer,
    ) -> None:
        self._config = config
        self._cache = cache
        self._stats = stats
        self._transport = transport
        self._logger = get_logger("fesco_tracker.business")
        self._cache_ttl = int(config.cache.ttl_hours * 3600)

    # ------------------------------------------------------------------
    async def fetch(
        self,
        session: aiohttp.ClientSession,
        endpoint: str,
        *,
        cache_key: Optional[str] = None,
        params: Optional[Mapping[str, Any]] = None,
    ) -> TransportResponse:
        cached_data: Any | None = None
        if cache_key is not None:
            cached_data = await self._cache.get(cache_key)
            if cached_data is not None:
                self._stats.cached_requests += 1
                self._logger.debug(msg("api.cache.hit", key=cache_key))
                CACHE_HITS.inc()
                return TransportResponse(
                    status=200,
                    headers={},
                    data=cached_data,
                )
            self._logger.debug(msg("api.cache.miss", key=cache_key))

        try:
            response = await self._transport.get_json(
                session,
                endpoint,
                params=params,
            )
        except TransportAuthError as exc:
            raise AuthenticationError(str(exc)) from exc
        except (TransportClientError, TransportContentTypeError) as exc:
            raise ApiRequestError(str(exc)) from exc
        except TransportError as exc:
            raise ApiRequestError(str(exc)) from exc

        if cache_key is not None:
            if cached_data != response.data:
                await self._cache.set(cache_key, response.data, self._cache_ttl)
            else:
                self._logger.debug(
                    msg("api.cache.unchanged", key=cache_key)
                )
        return response

    # ------------------------------------------------------------------
    async def store_negative_cache(self, container_number: str) -> None:
        key = f"no_order:{container_number}"
        await self._cache.set(
            key,
            {"no_order": True},
            self._cache_ttl,
        )
        self._logger.debug(msg("api.cache.negative_store", container=container_number))

    # ------------------------------------------------------------------
    async def negative_cache_hit(self, container_number: str) -> bool:
        key = f"no_order:{container_number}"
        exists = await self._cache.exists(key)
        if exists:
            self._logger.debug(
                msg("api.cache.negative_hit", container=container_number)
            )
        return bool(exists)
