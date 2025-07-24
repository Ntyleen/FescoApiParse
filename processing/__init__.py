# processing/__init__.py
"""
FESCO Container Tracking - Processing Package v2.0
=================================================

Модернизированная архитектура с прямой интеграцией Firebird.

Архитектура v2.0:
    🎼 ContainerTrackingEngine (новый главный компонент)
        ├── 🔥 FirebirdEntityManager - единый источник + запись в entity
        ├── 🌐 FescoApiClient - получение данных трекинга
        ├── 🔗 ContainerBindingManager - привязки контейнер→заявка
        ├── 💾 CacheBackend - кэширование API ответов
        └── 📈 Unified Statistics - общая статистика

Ключевые улучшения v2.0:
    ✅ Single Source of Truth - FirebirdEntityManager для чтения и записи
    ✅ Simplified Architecture - меньше компонентов, больше функциональности
    ✅ Transactional Safety - ACID гарантии для операций с БД
    ✅ Enterprise Ready - готовность к корпоративным интеграциям
    ✅ Backward Compatibility - совместимость с существующим кодом
"""

# =============================================================================
# ИМПОРТЫ - Обновленная структура
# =============================================================================

# Классические компоненты (обратная совместимость)
from .tracker import ContainerTracker
from .events import EventProcessor
from .container_bindings import ContainerBindingManager

# НОВОЕ: Импорт обновленного Engine
from .ContainerTrackingEngine import (
    ContainerTrackingEngine, 
    EngineStats,
    create_container_tracking_engine  # Фабричная функция из Engine
)

# Firebird компоненты
try:
    from utils.db.firebird_manager import (
        FirebirdEntityManager,
        EntityTableConfig,
        EntityColumnMapping,
        EntityStatusID,
        ContainerInfo,
        create_firebird_entity_manager,
        validate_firebird_config,
        FIREBIRD_AVAILABLE
    )
except ImportError:
    FIREBIRD_AVAILABLE = False
    FirebirdEntityManager = None
    EntityTableConfig = None
    EntityColumnMapping = None
    EntityStatusID = None
    ContainerInfo = None
    create_firebird_entity_manager = None
    validate_firebird_config = None

# Cache компоненты (file cache всегда доступен)
from cache import create_cache

# Redis Backend компоненты (опциональные)
try:
    from utils.redis_backend import (
        create_redis_manager,
        create_compatible_cache,
        create_compatible_bindings,
        REDIS_AVAILABLE
    )
except ImportError:
    REDIS_AVAILABLE = False
    create_redis_manager = None
    create_compatible_cache = None
    create_compatible_bindings = None

# =============================================================================
# ПУБЛИЧНЫЙ API
# =============================================================================

__all__ = [
    # === НОВАЯ АРХИТЕКТУРА V2.0 (Primary) ===
    'ContainerTrackingEngine',        # Главный engine
    'create_tracking_engine',         # Главная фабричная функция
    'create_firebird_engine',         # Firebird-specific создание
    'EngineStats',                    # Статистика engine
    
    # === КЛАССИЧЕСКИЕ КОМПОНЕНТЫ (Backward Compatibility) ===
    'ContainerTracker',               # Оригинальный трекер
    'EventProcessor',                 # Обработчик событий
    'ContainerBindingManager',        # Менеджер привязок
    
    # === FIREBIRD КОМПОНЕНТЫ ===
    'FirebirdEntityManager',          # Если доступен
    'EntityTableConfig',
    'EntityColumnMapping',
    'EntityStatusID',
    'ContainerInfo',
    'create_firebird_entity_manager',
    
    # === УТИЛИТЫ И ДИАГНОСТИКА ===
    'validate_processing_environment', # Проверка готовности
    'get_processing_capabilities',     # Доступные возможности
    'create_processing_config',        # Создание конфигурации
    
    # === КОНСТАНТЫ ===
    'FIREBIRD_AVAILABLE',
    'REDIS_AVAILABLE',
]


# =============================================================================
# ГЛАВНАЯ ФАБРИЧНАЯ ФУНКЦИЯ - Упрощенное API
# =============================================================================

async def create_tracking_engine(
    config,
    cache_type: str = "auto",
    firebird_config: dict = None,
    entity_config: EntityTableConfig = None
) -> ContainerTrackingEngine:
    """
    Создать ContainerTrackingEngine с автоматической настройкой
    
    Эта функция - ваш "главный строитель". Она собирает все компоненты
    в единую систему, автоматически выбирая лучшие доступные технологии.
    
    Args:
        config: Главная конфигурация приложения
        cache_type: Тип кэша ("auto", "redis", "file")
        firebird_config: Кастомная конфигурация Firebird (опционально)
        entity_config: Конфигурация entity таблицы (опционально)
        
    Returns:
        Полностью настроенный ContainerTrackingEngine
        
    Raises:
        ImportError: Если Firebird недоступен
        ConnectionError: Если не удается подключиться к БД
        
    Example:
        >>> from processing import create_tracking_engine
        >>> from config import load_config
        >>> 
        >>> config = load_config("production")
        >>> engine = await create_tracking_engine(config, cache_type="redis")
        >>> 
        >>> # Запуск полного workflow
        >>> stats = await engine.run_full_workflow(batch_size=100)
        >>> print(f"Обработано: {stats.containers_successful} контейнеров")
    """
    
    from utils.logging import get_logger
    logger = get_logger("processing.factory")
    
    logger.info("🏗️ Создание ContainerTrackingEngine v2.0")
    
    # === ПРОВЕРКА ДОСТУПНОСТИ FIREBIRD ===
    if not FIREBIRD_AVAILABLE:
        raise ImportError(
            "❌ Firebird недоступен! Для работы нужна БД интеграция.\n"
            "Установите: pip install firebird-driver\n"
            "или используйте классический ContainerTracker"
        )
    
    # === ШАГ 1: Создание Cache Backend ===
    cache = await _create_cache_backend(config, cache_type, logger)
    
    # === ШАГ 2: Подготовка Firebird конфигурации ===
    if firebird_config is None:
        firebird_config = _extract_firebird_config_from_main_config(config)
    
    # === ШАГ 3: Использование фабричной функции из Engine ===
    try:
        engine = await create_container_tracking_engine(
            config=config,
            cache=cache,
            firebird_config=firebird_config,
            entity_config=entity_config
        )
        
        logger.info("✅ ContainerTrackingEngine создан успешно")
        return engine
        
    except Exception as e:
        logger.error(f"❌ Ошибка создания engine: {e}")
        raise


async def create_firebird_engine(
    config,
    firebird_host: str,
    firebird_database: str,
    firebird_user: str = "SYSDBA",
    firebird_password: str = "",
    redis_url: str = None,
    entity_config: EntityTableConfig = None
) -> ContainerTrackingEngine:
    """
    Создать Engine специально для Firebird enterprise окружения
    
    Упрощенная функция для быстрого создания engine с явными параметрами.
    Идеально для production окружений где параметры задаются явно.
    
    Args:
        config: Основная конфигурация приложения
        firebird_host: Хост Firebird сервера
        firebird_database: Путь к .fdb файлу
        firebird_user: Пользователь БД (обычно SYSDBA)
        firebird_password: Пароль БД
        redis_url: URL Redis для кэширования (опционально)
        entity_config: Кастомная конфигурация entity таблицы
        
    Returns:
        Engine настроенный для Firebird
        
    Example:
        >>> engine = await create_firebird_engine(
        ...     config=load_config(),
        ...     firebird_host="192.168.120.19",
        ...     firebird_database="D:/BrokerDB/BROKER_PROD.FDB",
        ...     firebird_password="production_password",
        ...     redis_url="redis://prod-redis:6379"
        ... )
        >>> 
        >>> # Тестируем подключение
        >>> if await engine.firebird_manager.test_connection():
        ...     print("🔥 Firebird готов к работе!")
    """
    
    if not FIREBIRD_AVAILABLE:
        raise ImportError("Firebird недоступен. Установите: pip install firebird-driver")
    
    from utils.logging import get_logger
    logger = get_logger("processing.firebird_factory")
    
    logger.info("🔥 Создание Firebird Engine")
    
    # Подготавливаем конфигурацию Firebird
    firebird_config = {
        'host': firebird_host,
        'database': firebird_database,
        'user': firebird_user,
        'password': firebird_password
    }
    
    # Создаем cache
    if redis_url and REDIS_AVAILABLE:
        try:
            redis_manager = create_redis_manager(redis_url)
            cache = create_compatible_cache(redis_manager)
            logger.info("✅ Redis cache подключен")
        except Exception as e:
            logger.warning(f"⚠️ Redis недоступен, fallback на file cache: {e}")
            cache = create_cache(cache_type="file", cache_dir=config.cache.dir)
    else:
        cache = create_cache(cache_type="file", cache_dir=config.cache.dir)
        logger.info("📁 File cache создан")
    
    # Создаем engine через главную фабричную функцию
    engine = await create_container_tracking_engine(
        config=config,
        cache=cache,
        firebird_config=firebird_config,
        entity_config=entity_config
    )
    
    logger.info("🔥 Firebird Engine готов к работе")
    return engine


# =============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =============================================================================

async def _create_cache_backend(config, cache_type, logger):
    """Создание cache backend с умным fallback"""
    
    # Автоопределение лучшего типа кэша
    if cache_type == "auto":
        if REDIS_AVAILABLE and hasattr(config.cache, 'redis') and config.cache.redis.url:
            cache_type = "redis"
            logger.info("🚀 Автоопределение: выбран Redis cache")
        else:
            cache_type = "file"
            logger.info("🚀 Автоопределение: выбран File cache")
    
    # Попытка создания Redis cache
    if cache_type == "redis":
        if not REDIS_AVAILABLE:
            logger.warning("⚠️ Redis запрошен, но недоступен. Fallback на file cache")
            cache_type = "file"
        else:
            try:
                redis_manager = create_redis_manager(config.cache.redis.url)
                cache = create_compatible_cache(redis_manager)
                logger.info("✅ Redis cache создан")
                return cache
            except Exception as e:
                logger.warning(f"⚠️ Ошибка Redis cache, fallback на file: {e}")
                cache_type = "file"
    
    # Создание file cache (надежный fallback)
    cache = create_cache(
        cache_type="file",
        cache_dir=config.cache.dir,
        ttl_hours=config.cache.ttl_hours
    )
    logger.info("📁 File cache создан")
    return cache

    
def _extract_firebird_config_from_main_config(config) -> dict:
    """Извлечь конфигурацию Firebird из главного config"""
    
    try:
        # Проверяем, есть ли в config атрибут database
        if hasattr(config, 'database'):
            db_config = config.database
            
            # Проверяем, есть ли метод to_firebird_config
            if hasattr(db_config, 'to_firebird_config'):
                return db_config.to_firebird_config()
            
            # Иначе пытаемся извлечь вручную
            return {
                'host': getattr(db_config, 'host', 'localhost'),
                'database': getattr(db_config, 'database', ''),
                'user': getattr(db_config, 'user', 'SYSDBA'),
                'password': getattr(db_config, 'password', '')
            }
        
        # Fallback - минимальная конфигурация
        return {
            'host': 'localhost',
            'database': '',
            'user': 'SYSDBA',
            'password': ''
        }
        
    except Exception as e:
        raise ValueError(f"Не удается извлечь Firebird конфигурацию: {e}")


# =============================================================================
# ДИАГНОСТИКА И ВАЛИДАЦИЯ
# =============================================================================

def validate_processing_environment() -> dict:
    """
    Проверить готовность processing окружения
    
    Эта функция - ваш "доктор системы". Она проверяет все критически
    важные компоненты и дает рекомендации по их улучшению.
    
    Returns:
        Подробный отчет о готовности компонентов
        
    Example:
        >>> from processing import validate_processing_environment
        >>> 
        >>> report = validate_processing_environment()
        >>> if report['ready']:
        ...     print("✅ Система готова к работе")
        ... else:
        ...     for issue in report['missing_components']:
        ...         print(f"❌ Отсутствует: {issue}")
    """
    
    report = {
        'ready': True,
        'components': {},
        'recommendations': [],
        'missing_components': [],
        'warnings': []
    }
    
    # === ПРОВЕРКА FIREBIRD ===
    report['components']['firebird'] = {
        'available': FIREBIRD_AVAILABLE,
        'status': '✅ Готов к enterprise интеграции' if FIREBIRD_AVAILABLE else '❌ Критически важный компонент отсутствует',
        'description': 'Основная БД для чтения контейнеров и записи результатов'
    }
    
    if not FIREBIRD_AVAILABLE:
        report['ready'] = False
        report['missing_components'].append('firebird-driver')
        report['recommendations'].append('КРИТИЧНО: Установите pip install firebird-driver')
    
    # === ПРОВЕРКА REDIS ===
    report['components']['redis'] = {
        'available': REDIS_AVAILABLE,
        'status': '✅ Доступен для высокопроизводительного кэширования' if REDIS_AVAILABLE else '⚠️ Будет использован file cache',
        'description': 'Опциональный компонент для улучшения производительности'
    }
    
    if not REDIS_AVAILABLE:
        report['warnings'].append('Redis недоступен - производительность может быть ниже')
        report['recommendations'].append('Рекомендуется: pip install redis[hiredis]')
    
    # === ПРОВЕРКА КЛАССИЧЕСКИХ КОМПОНЕНТОВ ===
    report['components']['backward_compatibility'] = {
        'available': True,
        'status': '✅ Полная обратная совместимость',
        'description': 'Классические компоненты для миграции'
    }
    
    # === ОБЩАЯ ОЦЕНКА ===
    if report['ready']:
        report['overall_status'] = '🚀 Система полностью готова к enterprise использованию'
    else:
        report['overall_status'] = '⚠️ Требуются критически важные компоненты'
    
    return report


def get_processing_capabilities() -> dict:
    """
    Получить детальную информацию о возможностях processing модуля
    
    Returns:
        Подробное описание всех доступных возможностей
    """
    
    capabilities = {
        'architecture_version': '2.0',
        'primary_approach': 'Firebird-First Enterprise Integration',
        
        'engines': {
            'container_tracking_engine': {
                'available': FIREBIRD_AVAILABLE,
                'recommended': True,
                'features': [
                    'Unified Firebird чтение/запись',
                    'Оптимизация API запросов через группировку заявок',
                    'Транзакционная безопасность (ACID)',
                    'Enterprise статистика и мониторинг',
                    'Batch обработка с умной фильтрацией',
                    'Автоматическая дедупликация событий'
                ] if FIREBIRD_AVAILABLE else ['Недоступен - требуется firebird-driver']
            },
            
            'classic_tracker': {
                'available': True,
                'recommended': False,
                'features': [
                    'Простая параллельная обработка',
                    'Работа только с API данными',
                    'Backward compatibility',
                    'Подходит для простых задач'
                ],
                'limitations': [
                    'Нет интеграции с БД',
                    'Ограниченная оптимизация',
                    'Нет enterprise функций'
                ]
            }
        },
        
        'database_integration': {
            'firebird': {
                'available': FIREBIRD_AVAILABLE,
                'role': 'Primary database for enterprise integrations',
                'capabilities': [
                    'Чтение контейнеров из entity таблицы',
                    'Умная фильтрация по статусам и линиям',
                    'Обновление дат трекинга в реальном времени',
                    'Статистика и аналитика',
                    'Connection pooling и resource management'
                ] if FIREBIRD_AVAILABLE else []
            }
        },
        
        'caching_strategies': {
            'redis': {
                'available': REDIS_AVAILABLE,
                'performance': 'High',
                'features': [
                    'Единый connection pool',
                    'Namespace изоляция',
                    'Распределенное кэширование',
                    'Высокая пропускная способность'
                ] if REDIS_AVAILABLE else []
            },
            'file': {
                'available': True,
                'performance': 'Medium',
                'features': [
                    'Простота настройки',
                    'Отсутствие внешних зависимостей',
                    'Надежность на одной машине'
                ]
            }
        }
    }
    
    return capabilities


def create_processing_config(environment: str = "production") -> dict:
    """
    Создать рекомендуемую конфигурацию для processing модуля
    
    Args:
        environment: Тип окружения ("development", "production", "testing")
        
    Returns:
        Рекомендуемая конфигурация
    """
    
    base_config = {
        'version': '2.0',
        'architecture': 'firebird_first',
        'environment': environment
    }
    
    if environment == "development":
        base_config.update({
            'cache_type': 'file',
            'batch_size': 10,
            'firebird': {
                'host': 'localhost',
                'user': 'SYSDBA',
                'max_connections': 5
            },
            'logging_level': 'DEBUG'
        })
        
    elif environment == "production":
        base_config.update({
            'cache_type': 'redis' if REDIS_AVAILABLE else 'file',
            'batch_size': 100,
            'firebird': {
                'host': '192.168.120.19',
                'user': 'SYSDBA',
                'max_connections': 20
            },
            'logging_level': 'INFO'
        })
        
    elif environment == "testing":
        base_config.update({
            'cache_type': 'file',
            'batch_size': 5,
            'firebird': {
                'host': 'localhost',
                'user': 'SYSDBA',
                'max_connections': 2
            },
            'logging_level': 'WARNING'
        })
    
    return base_config



# =============================================================================
# МЕТАДАННЫЕ И СОВМЕСТИМОСТЬ
# =============================================================================

__version__ = "0.0.1"
__description__ = "Processing components with Firebird-First architecture"

# =============================================================================
# БЫСТРАЯ ПРОВЕРКА ГОТОВНОСТИ ПРИ ИМПОРТЕ
# =============================================================================

if __debug__:  # Только в debug режиме
    _startup_check = validate_processing_environment()
    if not _startup_check['ready']:
        import warnings
        warnings.warn(
            f"⚠️ Processing environment не полностью готов: "
            f"{', '.join(_startup_check['missing_components'])}",
            ImportWarning
        )
