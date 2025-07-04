from utils.logging import logging
from typing import Dict, Any, Optional, List
from redis_manager import RedisManager


class RedisBackedCache:
    """
    Адаптер для обратной совместимости с cache.CacheBackend
    
    Позволяет использовать Redis namespace как обычный cache backend.
    """
    
    def __init__(self, redis_manager: RedisManager):
        self.cache_namespace = redis_manager.get_cache_namespace()
    
    async def get(self, key: str) -> Optional[Dict[str, Any]]:
        return await self.cache_namespace.get(key)
    
    async def set(self, key: str, data: Dict[str, Any], ttl_seconds: int = 3600) -> None:
        await self.cache_namespace.set(key, data, ttl_seconds)
    
    async def delete(self, key: str) -> bool:
        return await self.cache_namespace.delete(key)
    
    async def clear(self) -> None:
        await self.cache_namespace.clear_pattern("*")
    
    async def exists(self, key: str) -> bool:
        result = await self.cache_namespace.get(key)
        return result is not None
    
    async def close(self) -> None:
        # Менеджер закрывается отдельно
        pass


class RedisBackedBindingManager:
    """
    Адаптер для обратной совместимости с ContainerBindingManager
    """
    
    def __init__(self, redis_manager: RedisManager):
        self.binding_namespace = redis_manager.get_binding_namespace()
        self.logger = logging.get_logger("redis.binding_adapter")
    
    async def bind_container_to_order(self, container_number: str, order_id: str) -> bool:
        return await self.binding_namespace.bind_container_to_order(container_number, order_id)
    
    async def get_container_order(self, container_number: str) -> Optional[str]:
        return await self.binding_namespace.get_container_order(container_number)
    
    async def get_order_containers(self, order_id: str) -> List[str]:
        return await self.binding_namespace.get_order_containers(order_id)
    
    async def is_order_processed(self, order_id: str) -> bool:
        return await self.binding_namespace.is_order_processed(order_id)
    
    async def mark_order_processed(self, order_id: str) -> bool:
        return await self.binding_namespace.mark_order_processed(order_id)
    
    async def should_process_container(self, container_number: str, order_id: str) -> bool:
        """Реализация логики из оригинального ContainerBindingManager"""
        existing_order = await self.get_container_order(container_number)
        
        if not existing_order:
            await self.bind_container_to_order(container_number, order_id)
            return True
        
        elif existing_order == order_id:
            is_processed = await self.is_order_processed(order_id)
            return not is_processed
        
        else:
            # Перепривязка к новой заявке
            await self.bind_container_to_order(container_number, order_id)
            return True
