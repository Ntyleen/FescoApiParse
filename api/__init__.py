# api/__init__.py
"""
FESCO Container Tracking - API Client

HTTP клиент для работы с FESCO API:
- FescoApiClient - основной клиент
- Исключения API
- Вспомогательные функции
"""

# Основной API клиент
from .api_client import FescoApiClient
from .exceptions import FescoApiError, AuthenticationError, ApiRequestError

# Публичный API модуля
__all__ = [
    # Основной клиент
    'FescoApiClient',
    
    # Исключения
    'FescoApiError',
    'AuthenticationError', 
    'ApiRequestError',
    
    # Фабричные функции
    'create_api_client',
]

# Метаданные
__version__ = "0.0.1"
__description__ = "FESCO API client for container tracking"


def create_api_client(config, cache, stats=None):
    """
    Фабричная функция для создания API клиента
    
    Args:
        config: Конфигурация приложения
        cache: Кэш для запросов
        stats: Статистика (опционально)
        
    Returns:
        Настроенный FescoApiClient
        
    Example:
        >>> from config import load_config
        >>> from cache import create_cache
        >>> from api import create_api_client
        >>> 
        >>> config = load_config()
        >>> cache = create_cache()
        >>> client = create_api_client(config, cache)
    """
    from ..models.processing_stats import ProcessingStats
    
    if stats is None:
        stats = ProcessingStats()
    
    return FescoApiClient(config, cache, stats)


def validate_api_config(config) -> bool:
    """
    Проверка корректности конфигурации API
    
    Args:
        config: Конфигурация API
        
    Returns:
        True если конфигурация валидна
    """
    try:
        # Проверяем базовые поля
        if not config.auth_token:
            return False
        
        if not config.api.base_url.startswith(('http://', 'https://')):
            return False
        
        if config.api.timeout_seconds <= 0:
            return False
            
        return True
        
    except AttributeError:
        return False