from utils.logging import get_logger
from redis_manager import RedisManager, RedisConfig
from abc import ABC
from typing import Dict, Any, Optional, List
import json


class RedisNamespace(ABC):
    """
    Абстрактный namespace для специализированной работы с Redis
    
    Каждый namespace инкапсулирует логику работы с определенным типом данных,
    но использует общий Redis connection pool.
    """
    
    def __init__(self, manager: 'RedisManager', prefix: str, default_ttl: int):
        self.manager = manager
        self.prefix = prefix
        self.default_ttl= RedisConfig.default_ttl * 3600
        self.logger = get_logger(f"redis.{prefix.rstrip(':')}")
    
    def _make_key(self, key: str) -> str:
        """Создать полный ключ с префиксом namespace'а"""
        return f"{self.prefix}{key}"
    
    async def _get_client(self):
        """Получить Redis клиент из менеджера"""
        return await self.manager.get_client()


class CacheNamespace(RedisNamespace):
    """
    Namespace для HTTP кэша
    
    Заменяет функциональность из cache/redis_cache.py,
    но использует общий Redis connection pool.
    """
    
    async def get(self, key: str) -> Optional[Dict[str, Any]]:
        """Получить данные из кэша"""
        try:
            client = await self._get_client()
            full_key = self._make_key(key)
            
            data = await client.get(full_key)
            if data:
                self.logger.debug(f"Cache HIT: {key}")
                return json.loads(data)
            
            self.logger.debug(f"Cache MISS: {key}")
            return None
            
        except Exception as e:
            self.logger.error(f"Cache get error for {key}: {e}")
            return None
    
    async def set(self, key: str, data: Dict[str, Any], ttl_seconds: int = None) -> bool:
        """Сохранить данные в кэш"""
        try:
            client = await self._get_client()
            full_key = self._make_key(key)
            ttl = ttl_seconds or self.manager.config.default_ttl
            
            json_data = json.dumps(data, ensure_ascii=False)
            await client.setex(full_key, ttl, json_data)
            
            self.logger.debug(f"Cache SET: {key} (TTL: {ttl}s)")
            return True
            
        except Exception as e:
            self.logger.error(f"Cache set error for {key}: {e}")
            return False
    
    async def delete(self, key: str) -> bool:
        """Удалить данные из кэша"""
        try:
            client = await self._get_client()
            full_key = self._make_key(key)
            
            deleted = await client.delete(full_key)
            if deleted:
                self.logger.debug(f"Cache DELETE: {key}")
            return bool(deleted)
            
        except Exception as e:
            self.logger.error(f"Cache delete error for {key}: {e}")
            return False
    
    async def clear_pattern(self, pattern: str) -> int:
        """Очистить кэш по паттерну"""
        try:
            client = await self._get_client()
            full_pattern = self._make_key(pattern)
            
            keys = await client.keys(full_pattern)
            if keys:
                deleted = await client.delete(*keys)
                self.logger.info(f"Cache cleared: {deleted} keys matching {pattern}")
                return deleted
            
            return 0
            
        except Exception as e:
            self.logger.error(f"Cache clear error for pattern {pattern}: {e}")
            return 0


class BindingNamespace(RedisNamespace):
    """
    Namespace для привязок контейнеров к заявкам
    
    Заменяет функциональность из ContainerBindingManager,
    но использует общий Redis connection pool.
    """
    
    async def bind_container_to_order(self, container_number: str, order_id: str) -> bool:
        """Привязать контейнер к заявке"""
        try:
            client = await self._get_client()
            
            # Ключи для двусторонней привязки
            container_key = self._make_key(f"container:{container_number}")
            order_key = self._make_key(f"order:{order_id}")
            
            # Используем pipeline для атомарной операции
            pipe = client.pipeline()
            
            # Привязка контейнер → заявка
            pipe.setex(container_key, self.manager.config.default_ttl, order_id)
            
            # Добавляем контейнер в список заявки
            pipe.sadd(order_key, container_number)
            pipe.expire(order_key, self.manager.config.default_ttl)
            
            await pipe.execute()
            
            self.logger.info(f"Bound container {container_number} to order {order_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Binding error for {container_number} → {order_id}: {e}")
            return False
    
    async def get_container_order(self, container_number: str) -> Optional[str]:
        """Получить заявку для контейнера"""
        try:
            client = await self._get_client()
            container_key = self._make_key(f"container:{container_number}")
            
            order_id = await client.get(container_key)
            if order_id:
                self.logger.debug(f"Found binding: {container_number} → {order_id}")
                return order_id
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error getting order for {container_number}: {e}")
            return None
    
    async def get_order_containers(self, order_id: str) -> List[str]:
        """Получить все контейнеры заявки"""
        try:
            client = await self._get_client()
            order_key = self._make_key(f"order:{order_id}")
            
            containers = await client.smembers(order_key)
            container_list = list(containers)
            
            self.logger.debug(f"Order {order_id} has {len(container_list)} containers")
            return container_list
            
        except Exception as e:
            self.logger.error(f"Error getting containers for order {order_id}: {e}")
            return []
    
    async def is_order_processed(self, order_id: str) -> bool:
        """Проверить, обработана ли заявка"""
        try:
            client = await self._get_client()
            processed_key = self._make_key(f"processed:{order_id}")
            
            return bool(await client.exists(processed_key))
            
        except Exception as e:
            self.logger.error(f"Error checking if order {order_id} is processed: {e}")
            return False
    
    async def mark_order_processed(self, order_id: str) -> bool:
        """Отметить заявку как обработанную"""
        try:
            client = await self._get_client()
            processed_key = self._make_key(f"processed:{order_id}")
            
            # Отмечаем на сутки
            await client.setex(processed_key, 86400, "1")
            
            self.logger.debug(f"Marked order {order_id} as processed")
            return True
            
        except Exception as e:
            self.logger.error(f"Error marking order {order_id} as processed: {e}")
            return False
