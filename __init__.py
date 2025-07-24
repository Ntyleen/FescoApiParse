# __init__.py
"""
FESCO Container Tracking System
===============================

Система трекинга контейнеров с поддержкой:
- Множественных источников данных (Firebird, файлы)
- Кэширования (File/Redis)
- Параллельной обработки
- YAML конфигурации

Быстрый старт:
    >>> import asyncio
    >>> from fesco_tracker import quick_track_containers
    >>> 
    >>> containers = ["TDSU6005411", "FESU5384983"]
    >>> results = asyncio.run(quick_track_containers(containers))
    >>> 
    >>> for result in results:
    ...     print(f"{result.container_number}: {'✅' if result.success else '❌'}")
"""

__version__ = "0.1.0"
__author__ = "FESCO Container Tracking Team"
__description__ = "Container tracking system with YAML configuration"

# =============================================================================
# ПРОСТЫЕ ФУНКЦИИ ДЛЯ БЫСТРОГО СТАРТА
# =============================================================================

async def quick_track_containers(
    container_numbers: list,
    environment: str = "production",
    output_file: str = None # type: ignore
) -> list:
    """
    Быстрый трекинг контейнеров без настройки
    
    Args:
        container_numbers: Список номеров контейнеров
        environment: Окружение (development/production)
        output_file: Файл для сохранения результатов (опционально)
        
    Returns:
        Список результатов трекинга
        
    Example:
        >>> results = await quick_track_containers(["TDSU6005411"])
        >>> print(results[0].success)
    """
    from config import load_config
    from cache import create_cache
    from processing import ContainerTracker
    
    # Загружаем конфигурацию
    config = load_config(environment=environment)
    
    # Создаем кэш
    cache = create_cache(
        cache_type="file",  # Для простоты используем файловый кэш
        cache_dir=config.cache.dir
    )
    
    # Создаем трекер
    tracker = ContainerTracker(config, cache)
    
    # Собираем результаты
    results = []
    async for result in tracker.track_containers(container_numbers):
        results.append(result)
    
    # Сохраняем если нужно
    if output_file:
        import json
        from pathlib import Path
        from dataclasses import asdict
        
        Path(output_file).parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(
                [asdict(r) for r in results],
                f,
                ensure_ascii=False,
                indent=2
            )
    
    return results


def check_system_status() -> dict:
    """
    Проверить статус всех компонентов системы
    
    Returns:
        Словарь со статусом компонентов
        
    Example:
        >>> status = check_system_status()
        >>> print(f"Firebird: {status['firebird']['available']}")
        >>> print(f"Redis: {status['redis']['available']}")
    """
    status = {
        'version': __version__,
        'components': {}
    }
    
    # Проверка Firebird
    try:
        from utils.db.firebird_manager import FIREBIRD_AVAILABLE
        status['components']['firebird'] = {
            'available': FIREBIRD_AVAILABLE,
            'message': 'Firebird драйвер доступен' if FIREBIRD_AVAILABLE else 'Установите firebird-driver'
        }
    except ImportError:
        status['components']['firebird'] = {
            'available': False,
            'message': 'Модуль firebird_manager не найден'
        }
    
    # Проверка Redis
    try:
        from utils.redis_backend import REDIS_AVAILABLE
        status['components']['redis'] = {
            'available': REDIS_AVAILABLE,
            'message': 'Redis клиент доступен' if REDIS_AVAILABLE else 'Установите redis[hiredis]'
        }
    except ImportError:
        status['components']['redis'] = {
            'available': False,
            'message': 'Модуль redis_backend не найден'
        }
    
    # Общий статус
    all_available = all(
        comp.get('available', False) 
        for comp in status['components'].values()
    )
    
    status['ready'] = all_available
    status['message'] = 'Все компоненты доступны' if all_available else 'Некоторые компоненты недоступны'
    
    return status


def create_test_config(
    containers: list = None, # type: ignore
    use_redis: bool = False,
    batch_size: int = 10
) -> dict:
    """
    Создать тестовую конфигурацию для экспериментов
    
    Args:
        containers: Список тестовых контейнеров
        use_redis: Использовать Redis вместо файлового кэша
        batch_size: Размер батча для обработки
        
    Returns:
        Словарь с тестовой конфигурацией
    """
    if containers is None:
        containers = [
            "TDSU6005411",
            "FESU5384983",
            "TEMU1234567"
        ]
    
    return {
        'environment': 'test',
        'containers': containers,
        'cache': {
            'type': 'redis' if use_redis else 'file',
            'ttl_hours': 0.5  # 30 минут для тестов
        },
        'processing': {
            'batch_size': batch_size,
            'enable_retries': False  # Отключаем для тестов
        },
        'logging': {
            'level': 'DEBUG'
        }
    }


# =============================================================================
# ЭКСПОРТ ОСНОВНЫХ КОМПОНЕНТОВ
# =============================================================================

# Для обратной совместимости экспортируем основные классы
try:
    from config import Config, load_config
    from models import ContainerEvent, TrackingResult, ProcessingStats
    from cache import create_cache
    from processing import ContainerTracker
    
    __all__ = [
        # Быстрые функции
        'quick_track_containers',
        'check_system_status',
        'create_test_config',
        
        # Конфигурация
        'Config',
        'load_config',
        
        # Модели
        'ContainerEvent',
        'TrackingResult',
        'ProcessingStats',
        
        # Компоненты
        'create_cache',
        'ContainerTracker',
        
        # Метаданные
        '__version__',
    ]
    
except ImportError as e:
    # Если какие-то модули не импортируются, система все равно работает
    import warnings
    warnings.warn(f"Некоторые компоненты недоступны: {e}")
    
    __all__ = [
        'quick_track_containers',
        'check_system_status', 
        'create_test_config',
        '__version__',
    ]


# =============================================================================
# ПОЛЕЗНЫЕ УТИЛИТЫ ДЛЯ РАЗРАБОТКИ
# =============================================================================

def print_banner():
    """Вывести красивый баннер"""
    banner = """
    ╔═══════════════════════════════════════╗
    ║   FESCO Container Tracking System     ║
    ║         Version {}              ║
    ╚═══════════════════════════════════════╝
    """.format(__version__.center(10))
    print(banner)


if __name__ == "__main__":
    # При запуске модуля показываем информацию
    print_banner()
    
    status = check_system_status()
    print("\n📊 Статус компонентов:")
    for name, info in status['components'].items():
        emoji = "✅" if info['available'] else "❌"
        print(f"  {emoji} {name}: {info['message']}")
    
    print(f"\n🎯 Система {'готова к работе' if status['ready'] else 'требует настройки'}")
    print("\n💡 Для запуска используйте: python main.py --help")
