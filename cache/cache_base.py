from abc import ABC, abstractmethod
from typing import Dict, Any


class CacheBackend(ABC):
    """Абстрактный интерфейс кэша"""
    
    @abstractmethod
    async def get(self, key: str) -> Dict[str, Any] | None:
        """Получить данные из кэша"""
        pass
    
    @abstractmethod  
    async def set(self, key: str, data: Dict[str, Any], ttl_seconds: int = 3600) -> None:
        """Сохранить данные в кэш"""
        pass
    
    @abstractmethod
    async def delete(self, key: str) -> bool:
        """Удалить данные из кэша"""
        pass
    
    @abstractmethod
    async def clear(self) -> None:
        """Очистить весь кэш"""
        pass
    
    @abstractmethod
    async def exists(self, key: str) -> bool:
        """Проверить существование ключа"""
        pass
    
    @abstractmethod
    async def close(self) -> None:
        """Закрыть соединение с кэшем"""
        pass