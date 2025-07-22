import sys, pathlib
# Ensure project root is first on sys.path so "cache" resolves to the package
root_dir = pathlib.Path(__file__).resolve().parents[2]
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

import pytest
from unittest.mock import patch

try:
    import fakeredis.aioredis as fakeredis
except ImportError:  # pragma: no cover - fakeredis is optional
    fakeredis = None

from cache.redis_cache import RedisCache

@pytest.mark.asyncio
@pytest.mark.skipif(fakeredis is None, reason="fakeredis not installed")
async def test_redis_cache_operations():
    fake_client = fakeredis.FakeRedis()
    with patch('cache.redis_cache.redis.from_url', return_value=fake_client):
        cache = RedisCache('redis://localhost:6379', prefix='test:')
        key = 'sample'
        data = {'foo': 'bar'}

        assert await cache.get(key) is None
        assert await cache.exists(key) is False

        await cache.set(key, data)
        assert await cache.exists(key) is True
        assert await cache.get(key) == data

        assert await cache.delete(key) is True
        assert await cache.get(key) is None
        assert await cache.exists(key) is False

        await cache.set(key, data)
        await cache.clear()
        assert await cache.get(key) is None
        assert await cache.exists(key) is False

        await cache.close()
