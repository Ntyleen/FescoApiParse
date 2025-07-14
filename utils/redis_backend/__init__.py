# redis_backend/__init__.py
"""
Redis Backend for FESCO Container Tracking
==========================================

Специализированная система управления Redis с namespace архитектурой.

Архитектурная философия:
    Представьте Redis как большую библиотеку в вашей компании. Раньше каждый 
    отдел держал свои мини-библиотеки с дублирующими книгами. Теперь мы создали 
    единую библиотеку с организованными секциями:
    
    📚 RedisManager - главный библиотекарь
        ├── 💾 CacheNamespace - секция "HTTP ответы"  
        ├── 🔗 BindingNamespace - секция "Привязки контейнеров"
        └── 🔮 Будущие секции по мере необходимости

Ключевые принципы дизайна:
    ✅ Single Connection Pool - один пул соединений для всех операций
    ✅ Namespace Isolation - каждый тип данных в своем пространстве имен
    ✅ Backward Compatibility - работает с существующим кодом
    ✅ Graceful Degradation - корректно работает если Redis недоступен
    ✅ Easy Testing - каждый namespace можно тестировать независимо

Когда использовать:
    - У вас есть несколько компонентов, использующих Redis
    - Хотите избежать дублирования connection pools
    - Нужно организованное разделение данных по типам
    - Планируете масштабировать использование Redis
"""

from utils.logging import get_logger
from typing import Optional, Dict, Any

# =============================================================================
# ПРОВЕРКА ДОСТУПНОСТИ REDIS
# =============================================================================

try:
    import redis.asyncio as redis_async
    REDIS_AVAILABLE = True
    _redis_import_error = None
except ImportError as e:
    REDIS_AVAILABLE = False
    redis_async = None
    _redis_import_error = str(e)

# =============================================================================
# ИМПОРТЫ КОМПОНЕНТОВ (с graceful degradation)
# =============================================================================

# if REDIS_AVAILABLE:
    # Импортируем основные компоненты только если Redis доступен
    from .redis_manager import RedisManager, RedisConfig
    from .namespaces import CacheNamespace, BindingNamespace
    from .adapters import RedisBackedCache, RedisBackedBindingManager
# else:
#     # Создаем заглушки для случая когда Redis недоступен
#     RedisManager = None
#     RedisConfig = None
#     CacheNamespace = None
#     BindingNamespace = None
#     RedisBackedCache = None
#     RedisBackedBindingManager = None

# =============================================================================
# ПУБЛИЧНЫЙ API
# =============================================================================

__all__ = [
    # === ПРОВЕРКА ДОСТУПНОСТИ ===
    'REDIS_AVAILABLE',
    'check_redis_availability',
    
    # === ОСНОВНЫЕ КОМПОНЕНТЫ ===
    'RedisManager',         # Центральный менеджер
    'RedisConfig',          # Конфигурация
    
    # === NAMESPACE КОМПОНЕНТЫ ===
    'CacheNamespace',       # Для HTTP кэша
    'BindingNamespace',     # Для привязок контейнеров
    
    # === АДАПТЕРЫ СОВМЕСТИМОСТИ ===
    'RedisBackedCache',           # Замена для cache.RedisCache
    'RedisBackedBindingManager',  # Замена для ContainerBindingManager
    
    # === ФАБРИЧНЫЕ ФУНКЦИИ ===
    'create_redis_manager',       # Простое создание менеджера
    'create_compatible_cache',    # Создание совместимого кэша
    'create_compatible_bindings', # Создание совместимого binding manager
    
    # === УТИЛИТЫ ===
    'validate_redis_config',      # Валидация конфигурации
    'get_redis_info',            # Информация о Redis backend
]

# =============================================================================
# ПРОВЕРКА ДОСТУПНОСТИ И ДИАГНОСТИКА
# =============================================================================

def check_redis_availability() -> Dict[str, Any]:
    """
    Проверить доступность Redis и дать рекомендации
    
    
    Returns:
        dict: Подробная информация о доступности Redis
        
    Example:
        >>> status = check_redis_availability()
        >>> if status['available']:
        ...     print("✅ Redis готов к работе")
        ... else:
        ...     print(f"❌ Проблема: {status['issue']}")
        ...     print(f"💡 Решение: {status['recommendation']}")
    """
    
    if REDIS_AVAILABLE:
        return {
            'available': True,
            'redis_version': getattr(redis_async, '__version__', 'unknown'),
            'message': 'Redis client успешно импортирован',
            'components_ready': True,
            'recommendation': 'Можно использовать все функции Redis backend'
        }
    else:
        return {
            'available': False,
            'error': _redis_import_error,
            'issue': 'Redis client не установлен или не может быть импортирован',
            'components_ready': False,
            'recommendation': (
                "Установите Redis client:\n"
                "  pip install redis[hiredis]\n"
                "или используйте файловый кэш как fallback"
            ),
            'fallback_options': [
                'Файловый кэш (cache.FileCache)',
                'In-memory хранение для тестов',
                'Отключение кэширования (не рекомендуется)'
            ]
        }


# =============================================================================
# ФАБРИЧНЫЕ ФУНКЦИИ - Простые способы создания компонентов
# =============================================================================

def create_redis_manager(
    redis_url: str = "redis://localhost:6379",
    max_connections: int = 20,
    **kwargs
) -> Optional['RedisManager']:
    """
    Создать Redis менеджер с разумными настройками по умолчанию
    
    Думайте об этой функции как о "мастере-настройщике". Вы говорите ему 
    основные требования, а он создает полностью настроенную систему.
    
    Args:
        redis_url: URL Redis сервера (например, "redis://localhost:6379")
        max_connections: Максимальное количество соединений в пуле
        **kwargs: Дополнительные параметры конфигурации
        
    Returns:
        RedisManager или None если Redis недоступен
        
    Example:
        >>> # Простейший случай - локальный Redis
        >>> manager = create_redis_manager()
        >>> 
        >>> # Продакшен конфигурация
        >>> manager = create_redis_manager(
        ...     redis_url="redis://prod-redis.company.com:6379",
        ...     max_connections=50,
        ...     socket_timeout=10
        ... )
        >>> 
        >>> if manager:
        ...     cache = manager.get_cache_namespace()
        ...     bindings = manager.get_binding_namespace()
    """
    
    if not REDIS_AVAILABLE:
        logger = get_logger("redis_backend.factory")
        logger.error(
            "❌ Не удается создать RedisManager: Redis недоступен. "
            f"Причина: {_redis_import_error}"
        )
        return None
    
    try:
        # Создаем конфигурацию с переданными параметрами
        config = RedisConfig(
            url=redis_url,
            max_connections=max_connections,
            **kwargs
        )
        
        # Создаем и возвращаем менеджер
        manager = RedisManager(config)
        
        logger = get_logger("redis_backend.factory")
        logger.info(f"✅ RedisManager создан: {redis_url}")
        
        return manager
        
    except Exception as e:
        logger = get_logger("redis_backend.factory")
        logger.error(f"❌ Ошибка создания RedisManager: {e}")
        return None


def create_compatible_cache(redis_manager: 'RedisManager') -> Optional['RedisBackedCache']:
    """
    Создать кэш, совместимый с интерфейсом cache.CacheBackend
    
    Args:
        redis_manager: Настроенный Redis менеджер
        
    Returns:
        Кэш, совместимый с cache.CacheBackend интерфейсом
        
    Example:
        >>> manager = create_redis_manager()
        >>> if manager:
        ...     cache = create_compatible_cache(manager)
        ...     
        ...     # Ваш существующий код продолжает работать!
        ...     await cache.set("key", {"data": "value"})
        ...     result = await cache.get("key")
    """
    
    if not redis_manager:
        return None
    
    if not REDIS_AVAILABLE:
        return None
    
    try:
        return RedisBackedCache(redis_manager)
    except Exception as e:
        logger = get_logger("redis_backend.factory")
        logger.error(f"❌ Ошибка создания совместимого кэша: {e}")
        return None


def create_compatible_bindings(redis_manager: 'RedisManager') -> Optional['RedisBackedBindingManager']:
    """
    Создать менеджер привязок, совместимый с ContainerBindingManager
    
    Args:
        redis_manager: Настроенный Redis менеджер
        
    Returns:
        Менеджер привязок с тем же интерфейсом что и оригинальный
        
    Example:
        >>> manager = create_redis_manager()
        >>> if manager:
        ...     bindings = create_compatible_bindings(manager)
        ...     
        ...     # Тот же API что и раньше!
        ...     await bindings.bind_container_to_order("TDSU123", "ORD456")
        ...     order = await bindings.get_container_order("TDSU123")
    """
    
    if not redis_manager:
        return None
    
    if not REDIS_AVAILABLE:
        return None
    
    try:
        return RedisBackedBindingManager(redis_manager)
    except Exception as e:
        logger = get_logger("redis_backend.factory")
        logger.error(f"❌ Ошибка создания совместимого binding manager: {e}")
        return None


# =============================================================================
# УТИЛИТЫ И ВАЛИДАЦИЯ
# =============================================================================

def validate_redis_config(config_dict: Dict[str, Any]) -> Dict[str, Any]:
    """
    Валидация конфигурации Redis
    
    Args:
        config_dict: Словарь с конфигурацией Redis
        
    Returns:
        Результат валидации с рекомендациями
        
    Example:
        >>> config = {
        ...     'url': 'redis://localhost:6379',
        ...     'max_connections': 20
        ... }
        >>> result = validate_redis_config(config)
        >>> if result['valid']:
        ...     print("✅ Конфигурация корректна")
        ... else:
        ...     for error in result['errors']:
        ...         print(f"❌ {error}")
    """
    
    errors = []
    warnings = []
    
    # Проверка URL
    redis_url = config_dict.get('url', '')
    if not redis_url:
        errors.append("Отсутствует URL Redis сервера")
    elif not redis_url.startswith(('redis://', 'rediss://')):
        errors.append(f"Некорректный формат URL: {redis_url}")
    
    # Проверка max_connections
    max_conn = config_dict.get('max_connections', 10)
    if not isinstance(max_conn, int) or max_conn <= 0:
        errors.append("max_connections должно быть положительным числом")
    elif max_conn > 100:
        warnings.append("max_connections > 100 может быть избыточным")
    
    # Проверка timeout'ов
    socket_timeout = config_dict.get('socket_timeout', 5)
    if socket_timeout <= 0:
        errors.append("socket_timeout должен быть положительным")
    elif socket_timeout > 60:
        warnings.append("socket_timeout > 60 секунд может быть слишком большим")
    
    return {
        'valid': len(errors) == 0,
        'errors': errors,
        'warnings': warnings,
        'config_summary': {
            'url': config_dict.get('url', 'not_set'),
            'max_connections': max_conn,
            'socket_timeout': socket_timeout
        },
        'recommendations': _generate_config_recommendations(config_dict, warnings)
    }


def _generate_config_recommendations(config: Dict[str, Any], warnings: list) -> list:
    """Генерация рекомендаций по конфигурации"""
    
    recommendations = []
    
    if warnings:
        recommendations.append("Рассмотрите предупреждения выше")
    
    # Рекомендации по окружению
    url = config.get('url', '')
    if 'localhost' in url:
        recommendations.append("💡 Для продакшена укажите внешний Redis сервер")
    
    if config.get('max_connections', 10) < 5:
        recommendations.append("💡 Увеличьте max_connections для лучшей производительности")
    
    if not recommendations:
        recommendations.append("✅ Конфигурация выглядит хорошо")
    
    return recommendations


def get_redis_info() -> Dict[str, Any]:
    """
    Получить информацию о Redis backend модуле
    
    Эта функция - ваш "справочник". Она рассказывает что умеет модуль,
    какие у него возможности и ограничения.
    
    Returns:
        Подробная информация о модуле
        
    Example:
        >>> info = get_redis_info()
        >>> print(f"Версия: {info['version']}")
        >>> print(f"Redis доступен: {info['redis_available']}")
        >>> for feature in info['features']:
        ...     print(f"✅ {feature}")
    """
    
    return {
        'version': __version__,
        'redis_available': REDIS_AVAILABLE,
        'redis_client_version': getattr(redis_async, '__version__', 'unknown') if REDIS_AVAILABLE else None,
        'features': [
            'Единый connection pool для всех операций',
            'Namespace изоляция для разных типов данных', 
            'Обратная совместимость с существующим кодом',
            'Graceful degradation при недоступности Redis',
            'Встроенная валидация конфигурации',
            'Подробное логирование операций'
        ],
        'components': {
            'RedisManager': 'Центральный менеджер соединений',
            'CacheNamespace': 'Пространство имен для HTTP кэша',
            'BindingNamespace': 'Пространство имен для привязок контейнеров',
            'Adapters': 'Адаптеры для обратной совместимости'
        },
        'requirements': {
            'python': '>=3.8',
            'redis': 'redis[hiredis]>=4.0.0'
        },
        'status': 'production-ready' if REDIS_AVAILABLE else 'redis-unavailable'
    }


# =============================================================================
# МИГРАЦИОННЫЕ ПОМОЩНИКИ
# =============================================================================

def create_migration_plan() -> Dict[str, Any]:
    """
    Создать план миграции на Redis Backend
    
    Эта функция - ваш "консультант по миграции". Она анализирует текущую 
    ситуацию и предлагает пошаговый план перехода.
    
    Returns:
        Детальный план миграции
    """
    
    return {
        'migration_steps': [
            {
                'step': 1,
                'title': 'Проверка окружения',
                'action': 'Запустите check_redis_availability()',
                'description': 'Убедитесь что Redis доступен'
            },
            {
                'step': 2, 
                'title': 'Создание Redis Manager',
                'action': 'manager = create_redis_manager(your_redis_url)',
                'description': 'Создайте центральный менеджер Redis'
            },
            {
                'step': 3,
                'title': 'Замена Cache компонента',
                'action': 'cache = create_compatible_cache(manager)',
                'description': 'Замените существующий кэш на Redis-backed версию'
            },
            {
                'step': 4,
                'title': 'Замена Binding Manager',
                'action': 'bindings = create_compatible_bindings(manager)',
                'description': 'Замените binding manager на Redis-backed версию'
            },
            {
                'step': 5,
                'title': 'Тестирование',
                'action': 'Протестируйте на небольшом наборе данных',
                'description': 'Убедитесь что все работает как ожидается'
            },
            {
                'step': 6,
                'title': 'Постепенное внедрение',
                'action': 'Переводите компоненты по одному',
                'description': 'Не меняйте все сразу - поэтапная миграция безопаснее'
            }
        ],
        'rollback_plan': [
            'Старые компоненты остаются доступными',
            'Можно переключаться между версиями',
            'Данные в Redis можно экспортировать'
        ],
        'estimated_effort': 'Низкий - высокая совместимость с существующим кодом'
    }


# =============================================================================
# МЕТАДАННЫЕ И ВЕРСИОНИРОВАНИЕ
# =============================================================================

__version__ = "0.0.1"
__author__ = "FESCO Container Tracking Team"
__description__ = "Redis Backend for FESCO Container Tracking"


# Информация о совместимости
__compatibility__ = {
    'replaces': ['cache.RedisCache', 'processing.ContainerBindingManager'],
    'python_versions': ['3.8', '3.9', '3.10', '3.11', '3.12'],
    'redis_versions': ['6.0+', '7.0+'],
    'backwards_compatible': True
}


# =============================================================================
# ИНИЦИАЛИЗАЦИЯ И ПРОВЕРКИ
# =============================================================================

def _perform_startup_checks():
    """Выполнить проверки при импорте модуля"""
    
    logger = get_logger("redis_backend.startup")
    
    if REDIS_AVAILABLE:
        logger.debug("✅ Redis backend загружен успешно")
        logger.debug(f"📦 Redis client версия: {getattr(redis, '__version__', 'unknown')}")
    else:
        logger.info("⚠️ Redis недоступен - некоторые функции отключены")
        logger.info(f"💡 Для полной функциональности установите: pip install redis[hiredis]")


# Выполняем проверки при импорте (только в debug режиме)
if __debug__:
    _perform_startup_checks()


# =============================================================================
# УДОБНЫЕ ФУНКЦИИ ДЛЯ РАЗРАБОТЧИКОВ
# =============================================================================

def show_redis_status():
    """
    Показать текущий статус Redis backend
    
    Удобная функция для быстрой диагностики в REPL или скриптах.
    """
    
    print("🔴 Redis Backend Status")
    print("=" * 40)
    
    status = check_redis_availability()
    
    if status['available']:
        print("✅ Redis: Доступен")
        print(f"📦 Версия client: {status.get('redis_version', 'unknown')}")
        print("🛠️ Доступные компоненты:")
        for component, description in get_redis_info()['components'].items():
            print(f"   • {component}: {description}")
    else:
        print("❌ Redis: Недоступен")  
        print(f"🔍 Причина: {status['issue']}")
        print(f"💡 Решение: {status['recommendation']}")
    
    print("\n📚 Примеры использования:")
    print("   manager = create_redis_manager()")
    print("   cache = create_compatible_cache(manager)")
    print("   bindings = create_compatible_bindings(manager)")
    
    print("=" * 40)