import pytest
from unittest.mock import patch, AsyncMock, MagicMock

try:
    import fakeredis.aioredis as fakeredis
except ImportError:  # pragma: no cover - fakeredis optional
    fakeredis = None

from utils.redis_backend import (
    RedisManager,
    RedisConfig,
    CacheNamespace,
    BindingNamespace,
    RedisBackedCache,
    RedisBackedBindingManager,
)


@pytest.mark.asyncio
@pytest.mark.skipif(fakeredis is None, reason="fakeredis not installed")
async def test_manager_client_and_namespaces():
    fake = fakeredis.FakeRedis(decode_responses=True)
    with patch('utils.redis_backend.redis_manager.redis_async.from_url', return_value=fake):
        manager = RedisManager(RedisConfig())
        client = await manager.get_client()
        assert client is fake
        cache_ns = manager.get_cache_namespace()
        binding_ns = manager.get_binding_namespace()
        assert isinstance(cache_ns, CacheNamespace)
        assert isinstance(binding_ns, BindingNamespace)
        # cached instances
        assert manager.get_cache_namespace() is cache_ns
        assert manager.get_binding_namespace() is binding_ns


@pytest.mark.asyncio
@pytest.mark.skipif(fakeredis is None, reason="fakeredis not installed")
async def test_cache_namespace_operations():
    fake = fakeredis.FakeRedis(decode_responses=True)
    with patch('utils.redis_backend.redis_manager.redis_async.from_url', return_value=fake):
        manager = RedisManager(RedisConfig(cache_prefix='test:'))
        ns = manager.get_cache_namespace()
        key = 'item'
        data = {'a': 1}

        assert await ns.get(key) is None
        assert await ns.set(key, data)
        assert await ns.get(key) == data
        assert await ns.delete(key) is True
        await ns.set(key+'1', data)
        await ns.set(key+'2', data)
        deleted = await ns.clear_pattern(key+'*')
        assert deleted == 2


@pytest.mark.asyncio
@pytest.mark.skipif(fakeredis is None, reason="fakeredis not installed")
async def test_binding_namespace_operations():
    fake = fakeredis.FakeRedis(decode_responses=True)
    with patch('utils.redis_backend.redis_manager.redis_async.from_url', return_value=fake):
        manager = RedisManager(RedisConfig(binding_prefix='bind:'))
        ns = manager.get_binding_namespace()

        assert await ns.bind_container_to_order('C1', 'O1') is True
        assert await ns.get_container_order('C1') == 'O1'
        assert await ns.get_order_containers('O1') == ['C1']
        assert await ns.is_order_processed('O1') is False
        assert await ns.mark_order_processed('O1') is True
        assert await ns.is_order_processed('O1') is True


@pytest.mark.asyncio
async def test_cache_adapter_delegates():
    namespace = AsyncMock()
    manager = MagicMock()
    manager.get_cache_namespace.return_value = namespace

    cache = RedisBackedCache(manager)

    await cache.get('k')
    namespace.get.assert_awaited_once_with('k')

    await cache.set('k', {'v': 1}, 10)
    namespace.set.assert_awaited_once_with('k', {'v': 1}, 10)

    await cache.delete('k')
    namespace.delete.assert_awaited_once_with('k')

    await cache.clear()
    namespace.clear_pattern.assert_awaited_once_with('*')

    namespace.get.return_value = {'v': 1}
    assert await cache.exists('k') is True


@pytest.mark.asyncio
async def test_binding_adapter_delegates():
    namespace = AsyncMock()
    manager = MagicMock()
    manager.get_binding_namespace.return_value = namespace

    adapter = RedisBackedBindingManager(manager)

    await adapter.bind_container_to_order('C1', 'O1')
    namespace.bind_container_to_order.assert_awaited_once_with('C1', 'O1')

    await adapter.get_container_order('C1')
    namespace.get_container_order.assert_awaited_once_with('C1')

    await adapter.get_order_containers('O1')
    namespace.get_order_containers.assert_awaited_once_with('O1')

    await adapter.is_order_processed('O1')
    namespace.is_order_processed.assert_awaited_once_with('O1')

    await adapter.mark_order_processed('O1')
    namespace.mark_order_processed.assert_awaited_once_with('O1')

    # should_process_container logic
    namespace.get_container_order.return_value = None
    namespace.is_order_processed.return_value = False
    assert await adapter.should_process_container('C2', 'O2') is True
    namespace.bind_container_to_order.assert_awaited_with('C2', 'O2')

    namespace.bind_container_to_order.reset_mock()
    namespace.get_container_order.return_value = 'O2'
    namespace.is_order_processed.return_value = False
    assert await adapter.should_process_container('C2', 'O2') is True
    namespace.bind_container_to_order.assert_not_awaited()

    namespace.get_container_order.return_value = 'O2'
    namespace.is_order_processed.return_value = True
    assert await adapter.should_process_container('C2', 'O2') is False

    namespace.get_container_order.return_value = 'O3'
    namespace.is_order_processed.return_value = False
    assert await adapter.should_process_container('C2', 'O2') is True
    namespace.bind_container_to_order.assert_awaited_with('C2', 'O2')
