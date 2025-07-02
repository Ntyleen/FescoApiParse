# __init__.py
"""
FESCO Container Tracking System

Система трекинга контейнеров FESCO с поддержкой:
- YAML конфигурации  
- Централизованного логирования
- Кэширования (File/Redis)
- Параллельной обработки
- Дедупликации событий

Quick Start:
    >>> from fesco_tracker import quick_track_containers
    >>> 
    >>> containers = ["TDSU6005411", "FESU5384983"] 
    >>> results = await quick_track_containers(containers)
    >>> 
    >>> for result in results:
    ...     print(f"{result.container_number}: {result.success}")
"""

# Основные компоненты системы
from config import load_config, Config
from models import ContainerEvent, TrackingResult, ProcessingStats
from cache import create_cache
from processing import create_tracker
from utils import setup_logging_from_config, get_logger

# Публичный API всей системы
__all__ = [
    # Основная функция
    'quick_track_containers',
    'track_containers_advanced',
    
    # Конфигурация
    'load_config',
    'Config',
    
    # Модели данных
    'ContainerEvent',
    'TrackingResult', 
    'ProcessingStats',
    
    # Компоненты
    'create_cache',
    'create_tracker',
    
    # Логирование
    'setup_logging_from_config',
    'get_logger',
]

# Метаданные проекта
__version__ = "0.0.1"
__author__ = "FESCO Container Tracking Team"
__description__ = "Advanced container tracking system with YAML configuration"
__url__ = "https://github.com/your-repo/fesco-tracker"


async def quick_track_containers(
    container_numbers: list,
    environment: str = "development",
    log_level: str = "INFO"
) -> list:
    """
    Быстрый трекинг контейнеров с минимальной настройкой
    
    Args:
        container_numbers: Список номеров контейнеров
        environment: Окружение (development/production)
        log_level: Уровень логирования
        
    Returns:
        Список TrackingResult
        
    Example:
        >>> import asyncio
        >>> from fesco_tracker import quick_track_containers
        >>> 
        >>> async def main():
        ...     containers = ["TDSU6005411", "FESU5384983"]
        ...     results = await quick_track_containers(containers)
        ...     
        ...     for result in results:
        ...         if result.success:
        ...             print(f"✅ {result.container_number}: {result.last_event.operation}")
        ...         else:
        ...             print(f"❌ {result.container_number}: {result.error_message}")
        >>> 
        >>> asyncio.run(main())
    """
    # Загрузка конфигурации
    config = load_config(environment=environment)
    
    # Настройка логирования
    if log_level != config.logging.level:
        config.logging.level = log_level
    setup_logging_from_config(config.logging)
    
    logger = get_logger("fesco_tracker.quick")
    logger.info(f"🚀 Быстрый трекинг {len(container_numbers)} контейнеров")
    
    try:
        # Создание компонентов
        cache = create_cache(
            cache_type=config.cache.type,
            cache_dir=config.cache.dir,
            redis_url=config.cache.redis.url,
            prefix=config.cache.redis.prefix,
            ttl_hours=config.cache.ttl_hours
        )
        
        tracker = create_tracker(config, cache)
        
        # Трекинг
        results = []
        async for result in tracker.track_containers(container_numbers):
            results.append(result)
        
        # Статистика
        successful = sum(1 for r in results if r.success)
        logger.info(f"✅ Завершено: {successful}/{len(results)} успешных")
        
        return results
        
    except Exception as e:
        logger.error(f"❌ Ошибка быстрого трекинга: {e}")
        raise


async def track_containers_advanced(
    container_numbers: list,
    config_files: list = None,
    environment: str = None,
    cache_type: str = None,
    batch_size: int = None
) -> list:
    """
    Продвинутый трекинг контейнеров с полной настройкой
    
    Args:
        container_numbers: Список номеров контейнеров
        config_files: Дополнительные файлы конфигурации
        environment: Окружение
        cache_type: Тип кэша ("file" или "redis")
        batch_size: Размер батча для больших списков
        
    Returns:
        Список TrackingResult
        
    Example:
        >>> results = await track_containers_advanced(
        ...     containers=["TDSU6005411", "FESU5384983"],
        ...     environment="production",
        ...     cache_type="redis",
        ...     batch_size=100
        ... )
    """
    # Загрузка конфигурации
    config = load_config(environment=environment, config_files=config_files)
    
    # Переопределение настроек
    if cache_type:
        config.cache.type = cache_type
    if batch_size:
        config.processing.batch_size = batch_size
    
    # Настройка логирования
    setup_logging_from_config(config.logging)
    
    logger = get_logger("fesco_tracker.advanced")
    logger.info(f"🎯 Продвинутый трекинг {len(container_numbers)} контейнеров")
    logger.info(f"🔧 Окружение: {environment or 'auto'}")
    logger.info(f"💾 Кэш: {config.cache.type}")
    
    try:
        # Создание компонентов
        cache = create_cache(
            cache_type=config.cache.type,
            cache_dir=config.cache.dir,
            redis_url=config.cache.redis.url,
            prefix=config.cache.redis.prefix,
            ttl_hours=config.cache.ttl_hours
        )
        
        tracker = create_tracker(config, cache)
        
        # Выбор режима обработки
        if len(container_numbers) > config.processing.batch_size:
            logger.info(f"📦 Батчевая обработка: размер батча {config.processing.batch_size}")
            
            all_results = []
            async for batch_results in tracker.track_containers_batch(
                container_numbers, 
                config.processing.batch_size
            ):
                all_results.extend(batch_results)
            
            return all_results
        
        else:
            logger.info("🎯 Обычная обработка")
            
            results = []
            async for result in tracker.track_containers(container_numbers):
                results.append(result)
            
            return results
        
    except Exception as e:
        logger.error(f"❌ Ошибка продвинутого трекинга: {e}")
        raise


def get_version() -> str:
    """Получить версию пакета"""
    return __version__


def get_info() -> dict:
    """
    Получить информацию о пакете
    
    Returns:
        Словарь с метаданными пакета
    """
    return {
        "name": "fesco-tracker",
        "version": __version__,
        "description": __description__,
        "author": __author__,
        "url": __url__
    }