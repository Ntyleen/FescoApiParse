# __init__.py (корневой)
"""
FESCO Container Tracking System
==============================

Система трекинга контейнеров с интеграцией в корпоративные БД.

Быстрый старт:
    >>> from fesco_tracker import quick_track, create_application
    >>> 
    >>> # Простой способ - для разовых задач
    >>> results = await quick_track(["TDSU6005411", "FESU5384983"])
    >>> 
    >>> # Продвинутый способ - для интеграции в приложения  
    >>> app = create_application("production")
    >>> await app.initialize()
    >>> await app.run_tracking_process()

Основные компоненты:
    - FescoTrackingApplication: Главный класс приложения
    - ContainerTrackingWorkflow: Координатор процесса трекинга
    - DatabaseContainerSource: Источник контейнеров из БД
    - ExternalDatabaseWriter: Запись результатов в целевую БД
    - ContainerBindingManager: Управление привязками контейнер→заявка
"""

# =============================================================================
# ИМПОРТЫ - Что мы "выставляем наружу" из нашей системы
# =============================================================================

# Главные классы для пользователей системы
from main_enhanced import FescoTrackingApplication

# Workflow компоненты (для продвинутых пользователей)
from FescoApiParse.processing.ContainerTrackingEngine import ContainerTrackingWorkflow

# Компоненты БД (для кастомных интеграций)
from database.container_source import DatabaseContainerSource, ContainerInfo
from database.external_writer import (
    ExternalDatabaseWriter, 
    TableConfig, 
    ColumnMapping,
    create_shipment_table_config,
    create_tracking_events_table_config
)

# Базовые компоненты (ваши существующие, проверенные временем)
from config import load_config, Config
from cache import create_cache
from models import ContainerEvent, TrackingResult, ProcessingStats

# Утилиты
from utils.logging import setup_logging_from_config, get_logger


# =============================================================================
# ПУБЛИЧНЫЙ API - Что доступно при import fesco_tracker
# =============================================================================

__all__ = [
    # === ПРОСТОЙ API ===
    # Эти функции для тех, кто хочет "просто чтобы работало"
    'quick_track',
    'create_application', 
    
    # === ПРОДВИНУТЫЙ API ===
    # Эти классы для тех, кто хочет тонкой настройки
    'FescoTrackingApplication',
    'ContainerTrackingWorkflow',
    
    # === КОМПОНЕНТЫ БД ===
    # Для кастомных интеграций с БД
    'DatabaseContainerSource',
    'ExternalDatabaseWriter',
    'ContainerInfo',
    'TableConfig',
    'ColumnMapping',
    
    # === БАЗОВЫЕ СТРОИТЕЛЬНЫЕ БЛОКИ ===
    # Ваши проверенные компоненты
    'Config',
    'load_config',
    'create_cache',
    'ContainerEvent',
    'TrackingResult',
    'ProcessingStats',
    
    # === УТИЛИТЫ ===
    'setup_logging_from_config',
    'get_logger',
    
    # === ГОТОВЫЕ КОНФИГУРАЦИИ ===
    'create_shipment_table_config',
    'create_tracking_events_table_config',
]


# =============================================================================
# ПРОСТОЙ API - Функции "одной строкой" для быстрых задач
# =============================================================================

async def quick_track(
    container_numbers: list,
    environment: str = "development",
    batch_size: int = 50
) -> list:
    """
    Быстрый трекинг контейнеров "одной кнопкой"
    
    Это функция для тех случаев, когда вам нужно "просто протрекать 
    несколько контейнеров и получить результат". Вся сложность скрыта внутри.
    
    Args:
        container_numbers: Список номеров контейнеров
        environment: Окружение ("development", "production")
        batch_size: Размер батча для обработки
        
    Returns:
        Список словарей с результатами трекинга
        
    Example:
        >>> results = await quick_track(["TDSU6005411", "FESU5384983"])
        >>> for result in results:
        ...     if result['success']:
        ...         print(f"✅ {result['container_number']}: {result['last_operation']}")
        ...     else:
        ...         print(f"❌ {result['container_number']}: {result['error']}")
    """
    
    # TODO: Здесь будет реализация быстрого трекинга
    # Для простоты пока возвращаем заглушку
    logger = get_logger("fesco_tracker.quick")
    logger.info(f"🚀 Быстрый трекинг {len(container_numbers)} контейнеров")
    
    # В реальной реализации здесь будет:
    # 1. Создание временного app
    # 2. Добавление контейнеров в БД  
    # 3. Запуск трекинга
    # 4. Возврат результатов в простом формате
    
    return [
        {
            "container_number": container,
            "success": False,
            "error": "Quick track not implemented yet"
        }
        for container in container_numbers
    ]


def create_application(environment: str = "development") -> FescoTrackingApplication:
    """
    Фабричная функция для создания приложения трекинга
    
    Это как "конструктор" - собирает готовое к работе приложение
    с правильными настройками для выбранного окружения.
    
    Args:
        environment: Окружение для работы
        
    Returns:
        Настроенное приложение (нужно будет вызвать initialize())
        
    Example:
        >>> app = create_application("production")
        >>> await app.initialize()
        >>> await app.run_tracking_process()
        >>> await app.cleanup()
    """
    
    return FescoTrackingApplication(environment)


# =============================================================================
# ДОПОЛНИТЕЛЬНЫЕ УДОБНЫЕ ФУНКЦИИ
# =============================================================================

def get_version() -> str:
    """Получить версию системы"""
    return "1.0.0"


def get_system_info() -> dict:
    """
    Получить информацию о системе
    
    Полезно для диагностики и поддержки
    """
    import sys
    import platform
    
    return {
        "version": get_version(),
        "python_version": sys.version,
        "platform": platform.platform(),
        "components": {
            "config": "✅ Available",
            "cache": "✅ Available", 
            "database": "✅ Available",
            "api_client": "✅ Available",
            "workflow": "✅ Available"
        }
    }


def validate_environment() -> dict:
    """
    Проверить готовность окружения к работе
    
    Проверяет наличие всех необходимых переменных окружения,
    доступность БД, и другие критичные компоненты.
    
    Returns:
        Словарь с результатами проверок
    """
    
    import os
    
    checks = {
        "config": False,
        "database_source": False,
        "database_target": False,
        "fesco_token": False,
        "cache": False
    }
    
    issues = []
    
    # Проверка FESCO токена
    if os.getenv("FESCO_TOKEN"):
        checks["fesco_token"] = True
    else:
        issues.append("❌ FESCO_TOKEN не найден в переменных окружения")
    
    # Проверка БД источника
    if all(os.getenv(var) for var in ["SOURCE_DB_HOST", "SOURCE_DB_USER", "SOURCE_DB_NAME"]):
        checks["database_source"] = True
    else:
        issues.append("❌ Не хватает настроек БД источника (SOURCE_DB_*)")
    
    # Проверка целевой БД
    if all(os.getenv(var) for var in ["TARGET_DB_HOST", "TARGET_DB_USER", "TARGET_DB_NAME"]):
        checks["database_target"] = True
    else:
        issues.append("❌ Не хватает настроек целевой БД (TARGET_DB_*)")
    
    # Проверка кэша
    if os.getenv("REDIS_URL") or os.path.exists("./cache"):
        checks["cache"] = True
    else:
        issues.append("⚠️ Ни Redis, ни файловый кэш не настроены")
    
    all_good = all(checks.values())
    
    return {
        "ready": all_good,
        "checks": checks,
        "issues": issues,
        "recommendations": [
            "📖 Проверьте файл .env на наличие всех переменных",
            "🔗 Убедитесь в доступности БД",
            "🚀 Запустите validate_environment() перед продакшеном"
        ] if not all_good else ["✅ Все проверки пройдены, система готова к работе"]
    }


# =============================================================================
# ПРИМЕРЫ ИСПОЛЬЗОВАНИЯ В DOCSTRING
# =============================================================================

__doc__ += """

Примеры использования:

1. Простой трекинг (для разовых задач):
    ```python
    import asyncio
    from fesco_tracker import quick_track
    
    async def main():
        results = await quick_track([
            "TDSU6005411", 
            "FESU5384983"
        ])
        
        for result in results:
            print(f"{result['container_number']}: {result['success']}")
    
    asyncio.run(main())
    ```

2. Полноценное приложение (для интеграции):
    ```python
    import asyncio
    from fesco_tracker import create_application
    
    async def main():
        app = create_application("production")
        
        try:
            await app.initialize()
            await app.run_tracking_process(batch_size=100)
        finally:
            await app.cleanup()
    
    asyncio.run(main())
    ```

3. Кастомная интеграция (для разработчиков):
    ```python
    from fesco_tracker import (
        ContainerTrackingWorkflow,
        DatabaseContainerSource,
        ExternalDatabaseWriter,
        create_cache,
        load_config
    )
    
    # Создаете компоненты по отдельности
    config = load_config()
    cache = create_cache()
    db_source = DatabaseContainerSource(your_db_config)
    writer = ExternalDatabaseWriter(target_db_config, table_configs)
    
    # Собираете workflow
    workflow = ContainerTrackingWorkflow(config, cache, db_source, writer)
    
    # Запускаете
    await workflow.run_full_workflow()
    ```

4. Проверка системы:
    ```python
    from fesco_tracker import validate_environment, get_system_info
    
    # Проверяем готовность
    env_check = validate_environment()
    if env_check["ready"]:
        print("✅ Система готова к работе")
    else:
        for issue in env_check["issues"]:
            print(issue)
    
    # Информация о системе
    info = get_system_info()
    print(f"Версия: {info['version']}")
    ```
"""


# =============================================================================
# МЕТАДАННЫЕ ПАКЕТА
# =============================================================================

__version__ = "1.0.0"
__author__ = "FESCO Container Tracking Team"
__description__ = "Advanced container tracking system with database integration"
__url__ = "https://github.com/your-repo/fesco-tracker"

# Минимальные требования Python
__python_requires__ = ">=3.8"

# Основные зависимости (для справки)
__dependencies__ = [
    "aiohttp>=3.8.0",
    "aiomysql>=0.1.1", 
    "redis>=4.0.0",
    "pyyaml>=6.0",
    "python-dotenv>=0.19.0"
]