# processing/__init__.py
"""
FESCO Container Tracking - Processing Logic

Бизнес-логика обработки контейнеров:
- ContainerTracker - основной трекер
- EventProcessor - обработка событий
- Вспомогательные функции
"""

# Основные классы обработки
from .tracker import ContainerTracker
from .events import EventProcessor

# Публичный API модуля
__all__ = [
    # Основные классы
    'ContainerTracker',
    'EventProcessor',
    
    # Фабричные функции
    'create_tracker',
    'create_event_processor',
    
    # Вспомогательные функции
    'validate_container_number',
    'batch_containers',
]

# Метаданные
__version__ = "0.0.1"
__description__ = "Processing logic for FESCO Container Tracking"


def create_tracker(config, cache):
    """
    Фабричная функция для создания трекера контейнеров
    
    Args:
        config: Конфигурация приложения
        cache: Кэш для данных
        
    Returns:
        Настроенный ContainerTracker
        
    Example:
        >>> from config import load_config
        >>> from cache import create_cache
        >>> from processing import create_tracker
        >>> 
        >>> config = load_config()
        >>> cache = create_cache()
        >>> tracker = create_tracker(config, cache)
    """
    return ContainerTracker(config, cache)


def create_event_processor():
    """
    Создать обработчик событий
    
    Returns:
        EventProcessor
    """
    return EventProcessor()


def validate_container_number(container_number: str) -> bool:
    """
    Проверка корректности номера контейнера
    
    Args:
        container_number: Номер контейнера для проверки
        
    Returns:
        True если номер валидный
        
    Example:
        >>> validate_container_number("TDSU6005411")
        True
        >>> validate_container_number("invalid")
        False
    """
    if not isinstance(container_number, str):
        return False
    
    # Убираем пробелы
    container_number = container_number.strip().upper()
    
    # Базовая проверка длины (обычно 11 символов)
    if len(container_number) < 10 or len(container_number) > 15:
        return False
    
    # Проверяем, что содержит буквы и цифры
    if not any(c.isalpha() for c in container_number):
        return False
    
    if not any(c.isdigit() for c in container_number):
        return False
    
    # Проверяем отсутствие недопустимых символов
    allowed_chars = set('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789')
    if not all(c in allowed_chars for c in container_number):
        return False
    
    return True


def batch_containers(container_numbers: list, batch_size: int = 50) -> list:
    """
    Разбить список контейнеров на батчи
    
    Args:
        container_numbers: Список номеров контейнеров
        batch_size: Размер батча
        
    Returns:
        Список батчей
        
    Example:
        >>> containers = ["TDSU6005411", "FESU5384983", "TEMU1234567"]
        >>> batches = batch_containers(containers, batch_size=2)
        >>> len(batches)
        2
    """
    if batch_size <= 0:
        raise ValueError("Размер батча должен быть положительным")
    
    batches = []
    for i in range(0, len(container_numbers), batch_size):
        batch = container_numbers[i:i + batch_size]
        batches.append(batch)
    
    return batches


def filter_valid_containers(container_numbers: list) -> tuple:
    """
    Фильтрация валидных номеров контейнеров
    
    Args:
        container_numbers: Список номеров для проверки
        
    Returns:
        Tuple (валидные_контейнеры, невалидные_контейнеры)
        
    Example:
        >>> containers = ["TDSU6005411", "invalid", "FESU5384983"]
        >>> valid, invalid = filter_valid_containers(containers)
        >>> len(valid), len(invalid)
        (2, 1)
    """
    valid = []
    invalid = []
    
    for container in container_numbers:
        if validate_container_number(container):
            valid.append(container.strip().upper())
        else:
            invalid.append(container)
    
    return valid, invalid