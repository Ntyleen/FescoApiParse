import sys
import pathlib
import pytest
from unittest.mock import AsyncMock
import aiohttp

# Ensure project root is first on sys.path so local packages resolve correctly
ROOT_DIR = pathlib.Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from api.api_client import (
    FescoApiClient,
    AuthenticationError,
    ApiRequestError,
)
from config.settings import Config
from cache import CacheBackend
from models.processing_stats import ProcessingStats


@pytest.fixture
def client():
    from config.settings import FirebirdDatabaseConfig
    config = Config(database=FirebirdDatabaseConfig(database="/tmp/test.fdb"))
    config.api.base_url = "https://example.com/"
    config.auth_token = "token"
    stats = ProcessingStats()
    cache = AsyncMock(spec=CacheBackend)
    session = AsyncMock(spec=aiohttp.ClientSession)
    return FescoApiClient(config, cache, stats), session, cache, stats


@pytest.mark.asyncio
async def test_make_request_cache_hit(client):
    api, session, cache, stats = client
    cache.get.return_value = {"hit": True}
    response = AsyncMock()
    response.status = 200
    response.headers = {"content-type": "application/json"}
    response.json.return_value = {"hit": True}
    session.get.return_value.__aenter__.return_value = response

    result = await api._make_request(session, "https://example.com/test", "key")

    assert result == {"hit": True}
    session.get.assert_called_once()
    cache.set.assert_not_called()
    assert stats.cached_requests == 1


@pytest.mark.asyncio
async def test_make_request_cache_set(client):
    api, session, cache, stats = client
    cache.get.return_value = None
    response = AsyncMock()
    response.status = 200
    response.headers = {"content-type": "application/json"}
    response.json.return_value = {"ok": True}
    session.get.return_value.__aenter__.return_value = response

    result = await api._make_request(session, "https://example.com/test", "key", q=1)

    assert result == {"ok": True}
    session.get.assert_called_once()
    cache.set.assert_called_once_with("key", {"ok": True}, int(api.config.cache.ttl_hours * 3600))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,exc", [
        (401, AuthenticationError),
        (403, AuthenticationError),
        (429, ApiRequestError),
        (500, ApiRequestError),
    ]
)
async def test_handle_response_errors(client, status, exc):
    api, *_ = client
    response = AsyncMock()
    response.status = status
    response.text.return_value = "error"
    with pytest.raises(exc):
        await api._handle_response_errors(response)


@pytest.mark.asyncio
async def test_find_order_by_container_success(client):
    api, session, cache, stats = client
    api._make_request = AsyncMock(return_value={"data": [{"orderId": 123}]})

    order_id = await api.find_order_by_container(session, "TSTU1234567")

    assert order_id == "123"
    assert stats.orders_resolved == 1


@pytest.mark.asyncio
async def test_find_order_by_container_failure(client):
    api, session, *_ = client
    api._make_request = AsyncMock(side_effect=ApiRequestError("boom"))

    result = await api.find_order_by_container(session, "TSTU1234567")

    assert result is None


@pytest.mark.asyncio
async def test_get_order_tracking_success(client):
    api, session, *_ = client
    api._make_request = AsyncMock(return_value={"ok": True})

    data = await api.get_order_tracking(session, "ORD1")

    assert data == {"ok": True}


@pytest.mark.asyncio
async def test_get_order_tracking_error(client):
    api, session, *_ = client
    api._make_request = AsyncMock(side_effect=ApiRequestError("err"))

    with pytest.raises(ApiRequestError):
        await api.get_order_tracking(session, "ORD1")


@pytest.mark.asyncio
async def test_get_container_tracking_success(client):
    api, session, *_ = client
    api._make_request = AsyncMock(return_value={"ok": True})

    data = await api.get_container_tracking(session, "ORD1", "CONT1")

    assert data == {"ok": True}


@pytest.mark.asyncio
async def test_get_container_tracking_error(client):
    api, session, *_ = client
    api._make_request = AsyncMock(side_effect=ApiRequestError("err"))

    with pytest.raises(ApiRequestError):
        await api.get_container_tracking(session, "ORD1", "CONT1")

