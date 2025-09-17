"""High level FESCO API client built on top of layered architecture."""
from __future__ import annotations

from typing import Any, Dict, Optional

import aiohttp

from cache import CacheBackend
from config.settings import Config
from models.processing_stats import ProcessingStats
from utils.logging import get_logger
from utils.messages import msg

from .business import BusinessLayer
from .exceptions import AuthenticationError, ApiRequestError, FescoApiError
from .transport import TransportLayer


class FescoApiClient:
    """Facade combining transport and business layers."""

    def __init__(
        self,
        config: Config,
        cache: CacheBackend,
        stats: ProcessingStats,
        *,
        transport: TransportLayer | None = None,
        business: BusinessLayer | None = None,
    ) -> None:
        self.config = config
        self.cache = cache
        self.stats = stats
        self.logger = get_logger("fesco_tracker.api")
        self.transport = transport or TransportLayer(config)
        self.business = business or BusinessLayer(config, cache, stats, self.transport)

        self.logger.info(msg("api.init"))
        self.logger.debug(msg("api.base_url", base_url=config.api.base_url))
        self.logger.debug(msg("api.timeout", timeout=config.api.timeout_seconds))
        self.logger.debug(msg("api.max_parallel", max_parallel=config.api.max_parallel))

    # ------------------------------------------------------------------
    async def _make_request(
        self,
        session: aiohttp.ClientSession,
        endpoint: str,
        cache_key: Optional[str] = None,
        **params: Any,
    ) -> Dict[str, Any]:
        response = await self.business.fetch(
            session,
            endpoint,
            cache_key=cache_key,
            params=params or None,
        )
        data = response.data
        if not isinstance(data, dict):
            raise ApiRequestError("Ожидался объект JSON")
        return data

    # ------------------------------------------------------------------
    async def find_order_by_container(
        self,
        session: aiohttp.ClientSession,
        container_number: str,
    ) -> Optional[str]:
        container_logger = get_logger(
            f"fesco_tracker.api.container.{container_number}"
        )
        container_logger.debug("🔍 Поиск заявки по контейнеру")

        negative_hit = await self.business.negative_cache_hit(container_number)
        if negative_hit:
            return None

        cache_key = f"order_lookup:{container_number}"
        try:
            data = await self._make_request(
                session,
                "orders",
                cache_key=cache_key,
                text=container_number,
            )
        except FescoApiError as exc:
            container_logger.error(f"❌ Ошибка поиска заявки: {exc}")
            return None

        for item in data.get("data", []):
            order_id = item.get("orderId") or item.get("number")
            if order_id:
                self.stats.orders_resolved += 1
                container_logger.info(
                    f"✅ Найдена заявка: {order_id} из {container_number}"
                )
                return str(order_id)

        await self.business.store_negative_cache(container_number)
        return None

    # ------------------------------------------------------------------
    async def get_order_tracking(
        self,
        session: aiohttp.ClientSession,
        order_id: str,
    ) -> Dict[str, Any]:
        cache_key = f"order_track:{order_id}"
        data = await self._make_request(
            session,
            "tracking/fit",
            cache_key=cache_key,
            numbers=order_id,
        )

        containers_count = sum(
            len(order_item.get("containers", []))
            for order_item in data.get("data", [])
        )
        self.logger.debug(f"📦 Получено данных по {containers_count} контейнерам")
        return data

    # ------------------------------------------------------------------
    async def get_container_tracking(
        self,
        session: aiohttp.ClientSession,
        order_id: str,
        container_number: str,
    ) -> Dict[str, Any]:
        container_logger = get_logger(
            f"fesco_tracker.api.container.{container_number}"
        )
        cache_key = f"container_track:{order_id}:{container_number}"
        data = await self._make_request(
            session,
            "tracking/fit/container",
            cache_key=cache_key,
            orderNumber=order_id,
            containerNumber=container_number,
        )
        events_count = len(data.get("data", []))
        container_logger.debug(f"📊 Получено {events_count} событий")
        return data
