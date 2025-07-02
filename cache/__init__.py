
# 1. ИМПОРТЫ - что мы берем из модулей пакета
# ================================================

# Базовый интерфейс - ВСЕГДА нужен пользователю
from cache.cache_base import CacheBackend

# Конкретные реализации - пользователь может их использовать напрямую
from .file_cache import FileCache

# Redis может быть недоступен, поэтому импортируем с проверкой
from .redis_cache import RedisCache, REDIS_AVAILABLE

# Дополнительные зависимости для типизации
from pathlib import Path
from typing import Literal, Union


# 2. ПУБЛИЧНЫЙ API - что доступно извне пакета
# =============================================

__all__ = [
    # Классы для прямого использования
    'CacheBackend',      # Интерфейс - для type hints
    'FileCache',         # Файловый кэш
    'RedisCache',        # Redis кэш
    
    # Фабричная функция - основной способ создания
    'create_cache',
    
    # Константы и утилиты
    'REDIS_AVAILABLE',   # Проверка доступности Redis
]

# ❓ Что это значит:
# - При `from cache import *` импортируется ТОЛЬКО то, что в __all__
# - При `from cache import FileCache` тоже работает
# - Но `from cache import _internal_function` НЕ работает


# 3. ФАБРИЧНАЯ ФУНКЦИЯ - главная фишка
# ====================================

def create_cache(
    cache_type: Literal["file", "redis"] = "file",
    cache_dir: Union[Path, str] = "./cache",
    redis_url: str = "redis://localhost:6379",
    prefix: str = "fesco:",
    ttl_hours: int = 1
) -> CacheBackend:
    """
    Фабрика для создания кэша
    
    ❓ ЗАЧЕМ ЭТА ФУНКЦИЯ:
    - Пользователь не выбирает класс вручную
    - Автоматическая проверка зависимостей  
    - Единообразный интерфейс создания
    - Валидация параметров
    
    Args:
        cache_type: Тип кэша ("file" или "redis")
        cache_dir: Директория для файлового кэша
        redis_url: URL для подключения к Redis
        prefix: Префикс ключей для Redis  
        ttl_hours: Время жизни кэша в часах
    
    Returns:
        CacheBackend: Готовый к использованию кэш
    
    Raises:
        ValueError: Неподдерживаемый тип кэша
        ImportError: Redis недоступен, но запрошен redis кэш
        ConnectionError: Не удается подключиться к Redis
    
    Examples:
        >>> # Простое создание файлового кэша
        >>> cache = create_cache()
        
        >>> # Redis кэш с настройками
        >>> cache = create_cache(
        ...     cache_type="redis",
        ...     redis_url="redis://redis.company.com:6379",
        ...     prefix="myapp:",
        ...     ttl_hours=2
        ... )
    """
    
    # Валидация типа кэша
    if cache_type not in ("file", "redis"):
        raise ValueError(
            f"Неподдерживаемый тип кэша: {cache_type}. "
            f"Доступны: 'file', 'redis'"
        )
    
    # Валидация TTL
    if ttl_hours <= 0:
        raise ValueError(f"TTL должен быть положительным, получен: {ttl_hours}")
    
    # === СОЗДАНИЕ ФАЙЛОВОГО КЭША ===
    if cache_type == "file":
        # Нормализация пути
        cache_path = Path(cache_dir) if isinstance(cache_dir, str) else cache_dir
        
        # Создание и возврат
        return FileCache(cache_path, ttl_hours)
    
    # === СОЗДАНИЕ REDIS КЭША ===
    elif cache_type == "redis":
        # Проверка доступности Redis
        if not REDIS_AVAILABLE:
            raise ImportError(
                "Redis не установлен. Установите командой:\n"
                "pip install redis[hiredis]\n\n"
                "Или используйте файловый кэш:\n"
                "create_cache(cache_type='file')"
            )
        
        # Валидация URL
        if not redis_url.startswith(("redis://", "rediss://")):
            raise ValueError(
                f"Неверный формат Redis URL: {redis_url}\n"
                f"Ожидается: redis://host:port или rediss://host:port"
            )
        
        # Создание и возврат
        return RedisCache(redis_url, prefix, ttl_hours)


# 4. УДОБНЫЕ АЛИАСЫ И КОНСТАНТЫ
# =============================

# Проверка доступности Redis (реэкспорт для удобства)
# Пользователь может писать: if cache.REDIS_AVAILABLE: ...
__redis_available = REDIS_AVAILABLE

# Константы по умолчанию (если нужны пользователю)
DEFAULT_CACHE_DIR = Path("./cache")
DEFAULT_TTL_HOURS = 1
DEFAULT_REDIS_URL = "redis://localhost:6379"
DEFAULT_REDIS_PREFIX = "fesco:"


# 5. ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ (опционально)
# =========================================

def clear_all_caches(cache_dir: Path = DEFAULT_CACHE_DIR) -> int:
    """
    Очистка всех файловых кэшей в директории
    
    Args:
        cache_dir: Директория с кэшами
    
    Returns:
        int: Количество удаленных файлов
    """
    if not cache_dir.exists():
        return 0
    
    deleted_count = 0
    for cache_file in cache_dir.glob("*.json"):
        try:
            cache_file.unlink()
            deleted_count += 1
        except OSError:
            pass  # Игнорируем ошибки удаления
    
    return deleted_count


def validate_cache_config(cache_type: str, **kwargs) -> bool:
    """
    Проверка корректности конфигурации кэша
    
    Args:
        cache_type: Тип кэша
        **kwargs: Параметры конфигурации
    
    Returns:
        bool: True если конфигурация валидна
    """
    try:
        # Пытаемся создать кэш с данными параметрами
        create_cache(cache_type, **kwargs)
        return True
    except (ValueError, ImportError, ConnectionError):
        return False


# 6. ИНФОРМАЦИЯ О ПАКЕТЕ (опционально)
# ====================================

__version__ = "0.0.1"
__author__ = "Your Name"
__description__ = "FESCO Container Tracking Cache System"

# Эту информацию можно использовать так:
# import cache
# print(f"Cache version: {cache.__version__}")