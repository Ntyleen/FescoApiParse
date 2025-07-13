# redis_backend/__init__.py
"""
Unified Redis Backend for FESCO Container Tracking
==================================================

Единый модуль для работы с Redis, предоставляющий специализированные 
интерфейсы для разных типов данных при использовании общего connection pool.

Архитектура:
    RedisManager (базовый слой)
        ├── CacheNamespace     # Для HTTP кэша
        ├── BindingNamespace   # Для привязок контейнеров  
        └── SessionNamespace   # Для будущих расширений

"""

import asyncio
import json
from utils.logging import get_logger
from typing import Dict, Any, Optional, List, Union
from namespaces import RedisNamespace, BindingNamespace, CacheNamespace

from contextlib import asynccontextmanager
import redis

from config.settings import RedisConfig

try:
    import redis.asyncio as redis_async
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    redis_async = None




class RedisManager:
    """
    Главный менеджер Redis подключений
    
    Предоставляет единый connection pool и фабрики для создания
    специализированных namespace'ов.
    """
    
    def __init__(self, config: RedisConfig):
        if not REDIS_AVAILABLE:
            raise ImportError("Redis не установлен. Установите: pip install redis[hiredis]")
        
        self.config = config
        self._client: Optional[redis_async.Redis] = None
        self.logger = get_logger("redis.manager")
        
        # Кэш namespace'ов для переиспользования
        self._namespaces: Dict[str, RedisNamespace] = {}
        
        self.logger.info(f"RedisManager configured: {config.url}")
    
    async def get_client(self) -> redis_async.Redis:
        """Получить Redis клиент (ленивая инициализация)"""
        if not self._client:
            self._client = redis_async.from_url(
                self.config.url,
                max_connections=self.config.max_connections,
                socket_timeout=self.config.socket_timeout,
                socket_connect_timeout=self.config.socket_connect_timeout,
                retry_on_timeout=self.config.retry_on_timeout,
                decode_responses=self.config.decode_responses
            )
            
            # Проверяем подключение
            await self._client.ping()
            self.logger.info("Redis connection established")
        
        return self._client
    
    def get_cache_namespace(self) -> CacheNamespace:
        """Получить namespace для HTTP кэша"""
        if 'cache' not in self._namespaces:
            self._namespaces['cache'] = CacheNamespace(self, self.config.cache_prefix)
        return self._namespaces['cache']
    
    def get_binding_namespace(self) -> BindingNamespace:
        """Получить namespace для привязок контейнеров"""
        if 'binding' not in self._namespaces:
            self._namespaces['binding'] = BindingNamespace(self, self.config.binding_prefix)
        return self._namespaces['binding']
    
    async def get_stats(self) -> Dict[str, Any]:
        """Получить статистику Redis"""
        try:
            client = await self.get_client()
            info = await client.info()
            
            return {
                'connected_clients': info.get('connected_clients', 0),
                'used_memory_human': info.get('used_memory_human', '0B'),
                'total_commands_processed': info.get('total_commands_processed', 0),
                'keyspace': {
                    db: info.get(f'db{db}', {}) for db in range(16) 
                    if f'db{db}' in info
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error getting Redis stats: {e}")
            return {}
    
    @asynccontextmanager
    async def transaction(self):
        """Контекстный менеджер для Redis транзакций"""
        client = await self.get_client()
        pipe = client.pipeline()
        
        try:
            yield pipe
            await pipe.execute()
        except Exception:
            # В Redis нет rollback, но можем логировать
            self.logger.error("Transaction failed")
            raise
    
    async def close(self) -> None:
        """Закрыть все соединения"""
        if self._client:
            await self._client.close()
            self.logger.info("Redis connections closed")
            self._client = None



def create_redis_manager(
    redis_url: str = "redis://localhost:6379",
    **kwargs
) -> Optional[RedisManager]:
    """
    Создать Redis менеджер с упрощенной конфигурацией
    
    Args:
        redis_url: URL Redis сервера
        **kwargs: Дополнительные параметры конфигурации
        
    Returns:
        Настроенный RedisManager
    """
    config = RedisConfig(url=redis_url, **kwargs)
    return RedisManager(config)


__all__ = [
    'RedisManager',
    'RedisConfig', 
    'CacheNamespace',
    'BindingNamespace',
    'RedisBackedCache',
    'RedisBackedBindingManager',
    'create_redis_manager',
    'REDIS_AVAILABLE'
]