import asyncio
import os
import sys
from pathlib import Path
from typing import Any, Dict, Optional

import aiohttp
import pytest
import pytest_asyncio
from aiohttp import web

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from api.api_client import FescoApiClient
from api.exceptions import ApiRequestError, AuthenticationError
from cache.cache_base import CacheBackend
from config.settings import Config, FirebirdDatabaseConfig
from models.processing_stats import ProcessingStats


class InMemoryCache(CacheBackend):
    def __init__(self):
        self.store: Dict[str, Any] = {}

    async def get(self, key: str) -> Optional[Dict[str, Any]]:
        return self.store.get(key)

    async def set(self, key: str, data: Dict[str, Any], ttl_seconds: int = 3600) -> None:
        self.store[key] = data

    async def delete(self, key: str) -> bool:  # pragma: no cover - test helper
        return self.store.pop(key, None) is not None

    async def clear(self) -> None:  # pragma: no cover - test helper
        self.store.clear()

    async def exists(self, key: str) -> bool:
        return key in self.store

    async def close(self) -> None:  # pragma: no cover - nothing to close
        self.store.clear()


class AiohttpServer:
    def __init__(self, handler):
        self.handler = handler
        self.app = web.Application()
        self.app.router.add_get("/{tail:.*}", handler)
        self.runner = web.AppRunner(self.app)
        self.site: web.TCPSite | None = None
        self.base_url: str | None = None

    async def __aenter__(self):
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await self.site.start()
        assert self.site._server is not None
        port = self.site._server.sockets[0].getsockname()[1]
        self.base_url = f"http://127.0.0.1:{port}/"
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.runner.cleanup()


@pytest.fixture
def config() -> Config:
    cfg = Config(database=FirebirdDatabaseConfig(database="/tmp/test.fdb"))
    cfg.api.base_url = "http://localhost/"
    cfg.api.retry_attempts = 2
    cfg.api.retry_backoff_seconds = 0.01
    cfg.api.timeout_seconds = 1
    cfg.auth_token = "token"
    return cfg


@pytest.fixture
def cache() -> InMemoryCache:
    return InMemoryCache()


@pytest_asyncio.fixture
async def session():
    async with aiohttp.ClientSession() as sess:
        yield sess


@pytest.fixture
def stats() -> ProcessingStats:
    return ProcessingStats()


@pytest.mark.asyncio
async def test_successful_request(config, cache, stats, session):
    async def handler(request):
        return web.json_response({"data": [1, 2, 3]})

    async with AiohttpServer(handler) as server:
        config.api.base_url = server.base_url
        client = FescoApiClient(config, cache, stats)
        data = await client._make_request(session, "orders", cache_key="key", text="ABCD")
        assert data == {"data": [1, 2, 3]}


@pytest.mark.asyncio
async def test_error_4xx_raises_request_error(config, cache, stats, session):
    async def handler(request):
        return web.Response(status=404, text="missing")

    async with AiohttpServer(handler) as server:
        config.api.base_url = server.base_url
        client = FescoApiClient(config, cache, stats)
        with pytest.raises(ApiRequestError):
            await client._make_request(session, "orders", cache_key="key")


@pytest.mark.asyncio
async def test_server_error_triggers_retry(config, cache, stats, session):
    calls = {"count": 0}

    async def handler(request):
        calls["count"] += 1
        if calls["count"] == 1:
            return web.Response(status=502, text="bad gateway")
        return web.json_response({"ok": True})

    async with AiohttpServer(handler) as server:
        config.api.base_url = server.base_url
        config.api.retry_attempts = 2
        client = FescoApiClient(config, cache, stats)
        data = await client._make_request(session, "orders", cache_key="key")
        assert calls["count"] == 2
        assert data == {"ok": True}


@pytest.mark.asyncio
async def test_timeout_raises_request_error(config, cache, stats, session):
    async def handler(request):
        await asyncio.sleep(0.2)
        return web.json_response({"ok": True})

    async with AiohttpServer(handler) as server:
        config.api.base_url = server.base_url
        config.api.timeout_seconds = 0.05
        client = FescoApiClient(config, cache, stats)
        with pytest.raises(ApiRequestError):
            await client._make_request(session, "orders", cache_key="key")


@pytest.mark.asyncio
async def test_cache_hit_skips_network(config, cache, stats, session):
    cache.store["key"] = {"cached": True}

    async def handler(request):  # pragma: no cover - should not be called
        raise AssertionError("network request was executed")

    async with AiohttpServer(handler) as server:
        config.api.base_url = server.base_url
        client = FescoApiClient(config, cache, stats)
        data = await client._make_request(session, "orders", cache_key="key")
        assert data == {"cached": True}


@pytest.mark.asyncio
async def test_negative_cache(config, cache, stats, session):
    responses = {"count": 0}

    async def handler(request):
        responses["count"] += 1
        return web.json_response({"data": []})

    async with AiohttpServer(handler) as server:
        config.api.base_url = server.base_url
        client = FescoApiClient(config, cache, stats)
        result = await client.find_order_by_container(session, "CNT1")
        assert result is None
        # Second call should hit negative cache and not query server again
        result = await client.find_order_by_container(session, "CNT1")
        assert result is None
        assert responses["count"] == 1


@pytest.mark.asyncio
async def test_authentication_error(config, cache, stats, session):
    async def handler(request):
        return web.Response(status=401, text="unauthorised")

    async with AiohttpServer(handler) as server:
        config.api.base_url = server.base_url
        client = FescoApiClient(config, cache, stats)
        with pytest.raises(AuthenticationError):
            await client._make_request(session, "orders", cache_key="key")
