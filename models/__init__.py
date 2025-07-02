# models/__init__.py
"""
FESCO Container Tracking - Data Models

Содержит все модели данных для трекинга контейнеров:
- ContainerEvent - события трекинга
- TrackingResult - результаты трекинга  
- ProcessingStats - статистика обработки
"""

# Основные модели событий и результатов
from .container_event import ContainerEvent, TrackingResult

# Модели статистики
from .processing_stats import ProcessingStats

# Публичный API модуля
__all__ = [
    # Основные модели
    'ContainerEvent',
    'TrackingResult', 
    'ProcessingStats',
]

# Метаданные модуля
__version__ = "0.0.1"
__description__ = "Data models for FESCO Container Tracking"

# Удобные функции для создания объектов

def create_empty_result(container_number: str) -> TrackingResult:
    """
    Создать пустой результат трекинга для контейнера
    
    Args:
        container_number: Номер контейнера
        
    Returns:
        Пустой TrackingResult
    """
    return TrackingResult(container_number=container_number)


def create_error_result(container_number: str, error_message: str) -> TrackingResult:
    """
    Создать результат с ошибкой
    
    Args:
        container_number: Номер контейнера
        error_message: Сообщение об ошибке
        
    Returns:
        TrackingResult с ошибкой
    """
    result = TrackingResult(container_number=container_number)
    result.error_message = error_message
    return result


def create_successful_result(
    container_number: str,
    order_id: str,
    last_event: ContainerEvent,
    events_source: str = "merged"
) -> TrackingResult:
    """
    Создать успешный результат трекинга
    
    Args:
        container_number: Номер контейнера
        order_id: Номер заявки
        last_event: Последнее событие
        events_source: Источник событий
        
    Returns:
        Успешный TrackingResult
    """
    result = TrackingResult(container_number=container_number)
    result.order_id = order_id
    result.last_event = last_event
    result.events_source = events_source
    return result