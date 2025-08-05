import os
import sys
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

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
async def test_mark_and_check_container_processed():
    cache = DummyCache()
    manager = ContainerBindingManager(cache)

    assert await manager.is_container_processed("CONT1") is False
    await manager.mark_container_processed("CONT1")
    assert await manager.is_container_processed("CONT1") is True
