import sys, pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[2]))

import pytest
from unittest.mock import patch

from cache import create_cache, FileCache, RedisCache


def test_create_cache_file(tmp_path):
    cache = create_cache(cache_type="file", cache_dir=tmp_path)
    assert isinstance(cache, FileCache)


def test_create_cache_invalid_type(tmp_path):
    with pytest.raises(ValueError):
        create_cache(cache_type="unknown", cache_dir=tmp_path)


@pytest.mark.asyncio
async def test_create_cache_redis_with_fakeredis():
    try:
        import fakeredis.aioredis as fakeredis
    except ImportError:
        pytest.skip("fakeredis not installed")

    fake_client = fakeredis.FakeRedis()
    with patch('cache.redis_cache.redis.from_url', return_value=fake_client):
        cache = create_cache(cache_type="redis", redis_url="redis://localhost:6379", prefix="test:")
        assert isinstance(cache, RedisCache)
        await cache.set('k', {'v': 1})
        assert await cache.get('k') == {'v': 1}
        await cache.clear()
        await cache.close()


def test_create_cache_missing_redis(monkeypatch):
    monkeypatch.setattr('cache.REDIS_AVAILABLE', False)
    monkeypatch.setattr('cache.redis_cache.REDIS_AVAILABLE', False)
    with pytest.raises(ImportError):
        create_cache(cache_type="redis")

