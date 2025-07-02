import json
import logging
from typing import Dict, Any

from cache.cache_base import CacheBackend

try:
    import redis.asyncio as redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    redis = None


class RedisCache(CacheBackend):
    """Redis кэш с поддержкой TTL"""
    
    def __init__(self, redis_url: str, prefix: str = "fesco:", ttl_hours: int = 1):
        if not REDIS_AVAILABLE:
            raise ImportError("Redis не установлен. Установите: pip install redis")
        
        self.redis_url = redis_url
        self.prefix = prefix
        self.ttl_seconds = ttl_hours * 3600
        self.client: redis.Redis | None = None
        
        logging.info(f"🔄 RedisCache: {redis_url}, prefix: {prefix}, TTL: {ttl_hours}h")
    
    async def _get_client(self) -> redis.Redis:
        """Ленивая инициализация Redis клиента"""
        if not self.client:
            self.client = redis.from_url(
                self.redis_url, 
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True
            )
            # Проверяем соединение
            await self.client.ping()
            logging.info("🔄 Redis connection established")
        
        return self.client
    
    def _make_key(self, key: str) -> str:
        """Создать полный ключ с префиксом"""
        return f"{self.prefix}{key}"
    
    async def get(self, key: str) -> Dict[str, Any] | None:
        """Получить данные из Redis"""
        try:
            client = await self._get_client()
            full_key = self._make_key(key)
            
            data = await client.get(full_key)
            if data:
                logging.debug(f"🔄 Redis HIT: {key}")
                return json.loads(data)
            
            return None
            
        except Exception as e:
            logging.error(f"Redis get error for {key}: {e}")
            return None
    
    async def set(self, key: str, data: Dict[str, Any], ttl_seconds: int = None) -> None:
        """Сохранить данные в Redis"""
        try:
            client = await self._get_client()
            full_key = self._make_key(key)
            ttl = ttl_seconds or self.ttl_seconds
            
            json_data = json.dumps(data, ensure_ascii=False)
            await client.setex(full_key, ttl, json_data)
            
            logging.debug(f"🔄 Redis SET: {key} (TTL: {ttl}s)")
            
        except Exception as e:
            logging.error(f"Redis set error for {key}: {e}")
    
    async def delete(self, key: str) -> bool:
        """Удалить данные из Redis"""
        try:
            client = await self._get_client()
            full_key = self._make_key(key)
            
            deleted_count = await client.delete(full_key)
            if deleted_count > 0:
                logging.debug(f"🔄 Redis DELETE: {key}")
                return True
            return False
            
        except Exception as e:
            logging.error(f"Redis delete error for {key}: {e}")
            return False
    
    async def clear(self) -> None:
        """Очистить все ключи с префиксом"""
        try:
            client = await self._get_client()
            pattern = f"{self.prefix}*"
            
            keys = await client.keys(pattern)
            if keys:
                deleted_count = await client.delete(*keys)
                logging.info(f"🔄 Redis cleared: {deleted_count} keys")
            else:
                logging.info("🔄 Redis: no keys to clear")
                
        except Exception as e:
            logging.error(f"Redis clear error: {e}")
    
    async def exists(self, key: str) -> bool:
        """Проверить существование ключа"""
        try:
            client = await self._get_client()
            full_key = self._make_key(key)
            
            return bool(await client.exists(full_key))
            
        except Exception as e:
            logging.error(f"Redis exists error for {key}: {e}")
            return False
    
    async def close(self) -> None:
        """Закрыть соединение с Redis"""
        if self.client:
            try:
                await self.client.close()
                logging.info("🔄 Redis connection closed")
            except Exception as e:
                logging.error(f"Redis close error: {e}")
            finally:
                self.client = None