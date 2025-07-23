import os
import sys
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from processing.container_bindings import ContainerBindingManager
from cache.cache_base import CacheBackend


class DummyCache(CacheBackend):
    def __init__(self):
        self.store = {}

    async def get(self, key: str):
        return self.store.get(key)

    async def set(self, key: str, data, ttl_seconds: int = 3600):
        self.store[key] = data

    async def delete(self, key: str) -> bool:
        return self.store.pop(key, None) is not None

    async def clear(self):
        self.store.clear()

    async def exists(self, key: str) -> bool:
        return key in self.store

    async def close(self):
        pass


@pytest.mark.asyncio
async def test_bind_updates_and_queries():
    cache = DummyCache()
    manager = ContainerBindingManager(cache)

    assert await manager.bind_container_to_order("CONT1", "ORD1") is True
    assert await manager.get_container_order("CONT1") == "ORD1"
    assert await manager.get_order_containers("ORD1") == ["CONT1"]

    # Rebind to another order should update both mappings
    assert await manager.bind_container_to_order("CONT1", "ORD2") is True
    assert await manager.get_container_order("CONT1") == "ORD2"
    assert await manager.get_order_containers("ORD1") == []
    assert await manager.get_order_containers("ORD2") == ["CONT1"]


@pytest.mark.asyncio
async def test_mark_and_check_processed():
    cache = DummyCache()
    manager = ContainerBindingManager(cache)

    assert await manager.is_order_processed("ORD1") is False
    await manager.mark_order_processed("ORD1")
    assert await manager.is_order_processed("ORD1") is True
    assert await manager.is_order_processed("ORD2") is False


@pytest.mark.asyncio
async def test_should_process_container_logic():
    cache = DummyCache()
    manager = ContainerBindingManager(cache)

    # New container -> should process and create binding
    assert await manager.should_process_container("CONT1", "ORD1") is True
    assert await manager.get_container_order("CONT1") == "ORD1"

    # Same order not processed yet -> process again
    assert await manager.should_process_container("CONT1", "ORD1") is True

    await manager.mark_order_processed("ORD1")

    # Order processed -> skip
    assert await manager.should_process_container("CONT1", "ORD1") is False

    # Rebinding to new order -> process and update
    assert await manager.should_process_container("CONT1", "ORD2") is True
    assert await manager.get_container_order("CONT1") == "ORD2"
    assert await manager.get_order_containers("ORD1") == []
    assert await manager.get_order_containers("ORD2") == ["CONT1"]


@pytest.mark.asyncio
async def test_mark_container_no_order():
    cache = DummyCache()
    manager = ContainerBindingManager(cache)

    assert await manager.is_container_no_order("CONTX") is False
    await manager.mark_container_no_order("CONTX")
    assert await manager.is_container_no_order("CONTX") is True

    # when marked, should_process_container should skip
    should = await manager.should_process_container("CONTX", "ORD1")
    assert should is False
