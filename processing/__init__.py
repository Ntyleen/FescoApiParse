# processing/__init__.py
"""
FESCO Container Tracking - Processing Package v2.0
=================================================

Модернизированная архитектура с интеграцией Firebird и Redis Backend.

Архитектура:
    🎼 ContainerTrackingOrchestrator (переименованный workflow_coordinator)
        ├── 🔥 FirebirdEntityManager - источник контейнеров + запись результатов
        ├── 🌐 FescoApiClient - получение данных трекинга
        ├── 🔗 Redis BindingNamespace - привязки контейнер→заявка
        ├── 💾 Redis CacheNamespace - кэширование API ответов
        └── 📈 Unified Statistics - общая статистика

Ключевые улучшения v2.0:
    ✅ Firebird-First Architecture - Firebird как основной источник и цель
    ✅ Redis Backend - единый connection pool для кэша и привязок
    ✅ Simplified Orchestration - упрощенная логика координации
    ✅ Unified Statistics - единая система метрик
    ✅ Backward Compatibility - совместимость с существующим кодом
"""

# =============================================================================
# ИМПОРТЫ - Новая структура с Firebird и Redis приоритетом
# =============================================================================

# Классические компоненты (обратная совместимость)
from .tracker import ContainerTracker
from .events import EventProcessor

# Новая архитектура v2.0
from .ContainerTrackingEngine import ContainerTrackingEngine, EngineStats
from .container_bindings import ContainerBindingManager

# Firebird компоненты
try:
    from utils.db.firebird_manager import (
        FirebirdEntityManager,
        EntityTableConfig,
        create_firebird_entity_manager
    )
    FIREBIRD_AVAILABLE = True
except ImportError:
    FIREBIRD_AVAILABLE = False
    FirebirdEntityManager = None
    EntityTableConfig = None
    create_firebird_entity_manager = None

# Redis Backend компоненты
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
    'ContainerTrackingEngine',  # Главный orchestrator
    'create_orchestrator',            # Фабрика orchestrator'а
    'create_firebird_orchestrator',   # Firebird-specific создание
    
    # === КЛАССИЧЕСКИЕ КОМПОНЕНТЫ (Backward Compatibility) ===
    'ContainerTracker',               # Оригинальный трекер
    'EventProcessor',                 # Обработчик событий
    'ContainerBindingManager',        # Менеджер привязок
    
    # === УТИЛИТЫ И ДИАГНОСТИКА ===
    'validate_processing_environment', # Проверка готовности
    'get_processing_capabilities',     # Доступные возможности
    'create_processing_config',        # Создание конфигурации
    
    # === СТАТИСТИКА ===
    'EngineStats',              # Статистика orchestrator'а
    
    # === МИГРАЦИОННЫЕ ПОМОЩНИКИ ===
    'create_migration_plan',          # План миграции
    'compare_orchestrator_vs_tracker', # Сравнение подходов
]


# =============================================================================
# ГЛАВНАЯ ФАБРИЧНАЯ ФУНКЦИЯ - Создание Orchestrator v2.0
# =============================================================================

async def create_orchestrator(
    config,
    cache_type: str = "auto",
    enable_firebird: bool = True,
    enable_redis: bool = True
) -> 'ContainerTrackingEngine':
    """
    Создать ContainerTrackingEngine с автоматической настройкой
    
    Эта функция - ваш "главный архитектор". Она анализирует доступные 
    компоненты и создает оптимальную конфигурацию orchestrator'а.
    
    Args:
        config: Главная конфигурация приложения
        cache_type: Тип кэша ("auto", "redis", "file")
        enable_firebird: Использовать Firebird как источник данных
        enable_redis: Использовать Redis backend
        
    Returns:
        Полностью настроенный ContainerTrackingEngine
        
    Example:
        >>> from processing import create_orchestrator
        >>> from config import load_config
        >>> 
        >>> config = load_config("production")
        >>> orchestrator = await create_orchestrator(
        ...     config,
        ...     cache_type="redis",
        ...     enable_firebird=True
        ... )
        >>> 
        >>> # Запуск полного workflow
        >>> stats = await orchestrator.run_full_workflow()
    """
    
    from utils.logging import get_logger
    logger = get_logger("processing.factory")
    
    logger.info("🏗️ Создание ContainerTrackingEngine v2.0")
    
    # === ШАГ 1: Создание Cache Backend ===
    cache = await _create_cache_backend(config, cache_type, enable_redis, logger)
    
    # === ШАГ 2: Создание Database Source ===
    db_source = await _create_database_source(config, enable_firebird, logger)
    
    # === ШАГ 3: Создание External Writer ===
    external_writer = await _create_external_writer(config, enable_firebird, logger)
    
    # === ШАГ 4: Создание Orchestrator ===
    orchestrator = ContainerTrackingEngine(
        config=config,
        cache=cache,
        db_source=db_source,
        external_writer=external_writer
    )
    
    logger.info("✅ ContainerTrackingEngine создан успешно")
    return orchestrator


async def create_firebird_orchestrator(
    config,
    firebird_config: dict,
    redis_url: str = "redis://localhost:6379"
) -> 'ContainerTrackingEngine':
    """
    Создать Orchestrator специально для Firebird enterprise окружения
    
    Оптимизированная версия для корпоративных интеграций с Firebird БД.
    
    Args:
        config: Основная конфигурация
        firebird_config: Конфигурация Firebird подключения
        redis_url: URL Redis для кэширования
        
    Returns:
        Orchestrator настроенный для Firebird
        
    Example:
        >>> firebird_config = {
        ...     'host': '192.168.120.19',
        ...     'database': 'D:/BrokerDB/BROKER_PROD.FDB',
        ...     'user': 'SYSDBA',
        ...     'password': 'production_password'
        ... }
        >>> 
        >>> orchestrator = await create_firebird_orchestrator(
        ...     config, firebird_config, "redis://prod-redis:6379"
        ... )
    """
    
    if not FIREBIRD_AVAILABLE:
        raise ImportError(
            "Firebird недоступен. Установите: pip install firebird-driver"
        )
    
    from utils.logging import get_logger
    logger = get_logger("processing.firebird_factory")
    
    logger.info("🔥 Создание Firebird Orchestrator")
    
    # Создаем Firebird Entity Manager
    entity_manager = create_firebird_entity_manager(**firebird_config)
    
    # Проверяем подключение
    if not await entity_manager.test_connection():
        raise ConnectionError("Не удается подключиться к Firebird")
    
    # Создаем Redis backend если доступен
    redis_manager = None
    cache = None
    
    if REDIS_AVAILABLE:
        try:
            redis_manager = create_redis_manager(redis_url)
            cache = create_compatible_cache(redis_manager)
            logger.info("✅ Redis backend подключен")
        except Exception as e:
            logger.warning(f"⚠️ Redis недоступен, используем fallback: {e}")
    
    # Fallback на file cache если Redis недоступен
    if not cache:
        from cache import create_cache
        cache = create_cache(cache_type="file", cache_dir=config.cache.dir)
    
    # Создаем orchestrator
    orchestrator = ContainerTrackingEngine(
        config=config,
        cache=cache,
        db_source=entity_manager,    # Firebird как источник
        external_writer=entity_manager  # Firebird как цель (тот же объект!)
    )
    
    logger.info("🔥 Firebird Orchestrator готов к работе")
    return orchestrator


# =============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ СОЗДАНИЯ КОМПОНЕНТОВ
# =============================================================================

async def _create_cache_backend(config, cache_type, enable_redis, logger):
    """Создание cache backend с автоопределением"""
    
    # Автоопределение лучшего типа кэша
    if cache_type == "auto":
        if enable_redis and REDIS_AVAILABLE:
            cache_type = "redis"
        else:
            cache_type = "file"
            logger.info("🔄 Fallback на file cache")
    
    # Создание Redis cache
    if cache_type == "redis" and REDIS_AVAILABLE:
        try:
            redis_manager = create_redis_manager(config.cache.redis.url)
            cache = create_compatible_cache(redis_manager)
            logger.info("✅ Redis cache подключен")
            return cache
        except Exception as e:
            logger.warning(f"⚠️ Redis ошибка, fallback на file: {e}")
            cache_type = "file"
    
    # Создание file cache (fallback)
    from cache import create_cache
    cache = create_cache(
        cache_type="file",
        cache_dir=config.cache.dir,
        ttl_hours=config.cache.ttl_hours
    )
    logger.info("📁 File cache создан")
    return cache


async def _create_database_source(config, enable_firebird, logger):
    """Создание источника данных"""
    
    if enable_firebird and FIREBIRD_AVAILABLE:
        try:
            # Создаем Firebird Entity Manager как источник
            entity_manager = create_firebird_entity_manager(
                host=config.database.host,
                database=config.database.database,
                user=config.database.user,
                password=config.database.password
            )
            
            # Проверяем подключение
            if await entity_manager.test_connection():
                logger.info("🔥 Firebird источник подключен")
                return entity_manager
            else:
                logger.error("❌ Firebird подключение неудачно")
                
        except Exception as e:
            logger.error(f"❌ Ошибка Firebird источника: {e}")
    
    # TODO: Fallback на generic DatabaseContainerSource
    logger.warning("⚠️ Используется заглушка источника данных")
    return None


async def _create_external_writer(config, enable_firebird, logger):
    """Создание писателя во внешнюю БД"""
    
    if enable_firebird and FIREBIRD_AVAILABLE:
        # Для Firebird источник и цель могут быть одним объектом
        logger.info("🔥 Firebird writer = тот же entity manager")
        return None  # Будет тот же объект что и db_source
    
    # TODO: Создание ExternalDatabaseWriter для других БД
    logger.warning("⚠️ Используется заглушка external writer")
    return None


# =============================================================================
# ДИАГНОСТИКА И ВАЛИДАЦИЯ
# =============================================================================

def validate_processing_environment() -> dict:
    """
    Проверить готовность processing окружения
    
    Returns:
        Подробный отчет о готовности компонентов
    """
    
    report = {
        'ready': True,
        'components': {},
        'recommendations': [],
        'missing_components': []
    }
    
    # Проверка Firebird
    report['components']['firebird'] = {
        'available': FIREBIRD_AVAILABLE,
        'status': '✅ Готов' if FIREBIRD_AVAILABLE else '❌ Не установлен'
    }
    
    if not FIREBIRD_AVAILABLE:
        report['ready'] = False
        report['missing_components'].append('firebird-driver')
        report['recommendations'].append('Установите: pip install firebird-driver')
    
    # Проверка Redis
    report['components']['redis'] = {
        'available': REDIS_AVAILABLE,
        'status': '✅ Готов' if REDIS_AVAILABLE else '⚠️ Fallback на file cache'
    }
    
    if not REDIS_AVAILABLE:
        report['recommendations'].append('Рекомендуется: pip install redis[hiredis]')
    
    # Проверка классических компонентов
    report['components']['classic_tracker'] = {
        'available': True,
        'status': '✅ Обратная совместимость'
    }
    
    return report


def get_processing_capabilities() -> dict:
    """Получить доступные возможности processing модуля"""
    
    return {
        'orchestration_v2': {
            'available': True,
            'features': [
                'Централизованная координация workflow',
                'Оптимизация API запросов',
                'Batch обработка контейнеров',
                'Unified статистика'
            ]
        },
        'firebird_integration': {
            'available': FIREBIRD_AVAILABLE,
            'features': [
                'Чтение контейнеров из entity таблицы',
                'Обновление дат после трекинга',
                'Транзакционная безопасность',
                'Enterprise статистика'
            ] if FIREBIRD_AVAILABLE else []
        },
        'redis_backend': {
            'available': REDIS_AVAILABLE,
            'features': [
                'Единый connection pool',
                'Namespace изоляция',
                'Высокопроизводительное кэширование',
                'Распределенные привязки'
            ] if REDIS_AVAILABLE else []
        },
        'backward_compatibility': {
            'available': True,
            'features': [
                'ContainerTracker (классический)',
                'EventProcessor', 
                'Существующие конфигурации',
                'Постепенная миграция'
            ]
        }
    }


# =============================================================================
# МИГРАЦИОННЫЕ ПОМОЩНИКИ
# =============================================================================

def create_migration_plan() -> dict:
    """Создать план миграции на v2.0 архитектуру"""
    
    return {
        'migration_approach': 'Постепенная миграция с обратной совместимостью',
        'phases': [
            {
                'phase': 1,
                'title': 'Подготовка инфраструктуры',
                'tasks': [
                    'Установить firebird-driver',
                    'Настроить Redis (опционально)',
                    'Обновить конфигурации',
                    'Провести validate_processing_environment()'
                ]
            },
            {
                'phase': 2,
                'title': 'Создание Orchestrator',
                'tasks': [
                    'Использовать create_orchestrator() или create_firebird_orchestrator()',
                    'Протестировать на небольшом наборе контейнеров',
                    'Сравнить производительность с классическим трекером'
                ]
            },
            {
                'phase': 3,
                'title': 'Постепенное переключение',
                'tasks': [
                    'Переводить процессы на orchestrator по одному',
                    'Мониторить метрики и ошибки',
                    'Оптимизировать производительность'
                ]
            }
        ],
        'rollback_strategy': 'Классические компоненты остаются доступными',
        'testing_approach': 'A/B тестирование orchestrator vs tracker'
    }


def compare_orchestrator_vs_tracker(container_count: int = 100) -> dict:
    """Сравнить новый orchestrator с классическим tracker"""
    
    return {
        'comparison_criteria': {
            'performance': {
                'orchestrator': 'Оптимизация через группировку заявок',
                'tracker': 'Простая параллельная обработка'
            },
            'api_efficiency': {
                'orchestrator': 'Минимизация дублирующих запросов',
                'tracker': 'Один запрос = один контейнер'
            },
            'database_integration': {
                'orchestrator': 'Нативная интеграция с Firebird',
                'tracker': 'Только API данные'
            },
            'monitoring': {
                'orchestrator': 'Централизованная статистика workflow',
                'tracker': 'Статистика API запросов'
            }
        },
        'use_cases': {
            'orchestrator_better': [
                'Корпоративные интеграции с БД',
                'Большие объемы данных (>1000 контейнеров)',
                'Требования к оптимизации API',
                'Сложные workflow с несколькими этапами'
            ],
            'tracker_better': [
                'Простые разовые задачи',
                'Малые объемы данных (<100 контейнеров)',
                'Быстрые прототипы',
                'Существующие интеграции'
            ]
        },
        'recommendation': (
            f"Для {container_count} контейнеров рекомендуется: "
            f"{'Orchestrator' if container_count > 100 else 'Tracker'}"
        )
    }


# =============================================================================
# МЕТАДАННЫЕ
# =============================================================================

__version__ = "2.0.0"
__description__ = "Processing components v2.0 with Firebird and Redis integration"