import sys, pathlib
# Ensure project root is first on sys.path so "cache" resolves to the package
root_dir = pathlib.Path(__file__).resolve().parents[2]
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

import asyncio
import json
import os
import pytest
from cache.file_cache import FileCache

@pytest.mark.asyncio
async def test_file_cache_operations(tmp_path):
    cache = FileCache(tmp_path)
    key = "sample"
    data = {"foo": "bar"}

    # initially key not exists
    assert await cache.get(key) is None
    assert await cache.exists(key) is False

    await cache.set(key, data)
    assert await cache.exists(key) is True
    assert await cache.get(key) == data

    # delete
    assert await cache.delete(key) is True
    assert await cache.get(key) is None
    assert await cache.exists(key) is False

    # set again and clear
    await cache.set(key, data)
    await cache.clear()
    assert await cache.get(key) is None
    assert await cache.exists(key) is False


@pytest.mark.asyncio
async def test_file_cache_ttl_zero_expires_immediately(tmp_path):
    """Items expire instantly when ttl_hours is 0."""
    cache = FileCache(tmp_path, ttl_hours=0)
    key = "instant"
    data = {"foo": "bar"}

    await cache.set(key, data)
    cache_path = cache._get_cache_path(key)
    assert cache_path.exists()

    assert await cache.get(key) is None
    assert not cache_path.exists()
