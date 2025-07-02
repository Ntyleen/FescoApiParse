# utils/__init__.py
"""
FESCO Container Tracking - Utilities

Вспомогательные утилиты для проекта:
- Система логирования
- Декораторы
- Общие функции
"""

# Основные функции логирования
from .logging import (
    setup_logging,
    setup_logging_from_config,
    get_logger,
    configure_fesco_logging,
    FescoLoggerAdapter,
    create_container_logger,
    create_api_logger,
    log_execution_time
)

# Публичный API модуля
__all__ = [
    # Основные функции логирования
    'setup_logging',
    'setup_logging_from_config', 
    'get_logger',
    
    # Специализированные функции логирования
    'configure_fesco_logging',
    'create_container_logger',
    'create_api_logger',
    
    # Классы и декораторы
    'FescoLoggerAdapter',
    'log_execution_time',
]

# Метаданные
__version__ = "0.0.1"
__description__ = "Utilities for FESCO Container Tracking"


# Удобные функции для быстрой настройки

def quick_setup_logging(level: str = "INFO") -> None:
    """
    Быстрая настройка логирования для простых случаев
    
    Args:
        level: Уровень логирования
    """
    setup_logging(level=level)


def setup_debug_logging() -> None:
    """Настройка отладочного логирования"""
    setup_logging(
        level="DEBUG",
        format_string="%(asctime)s [%(levelname)s] %(name)s:%(filename)s:%(lineno)d - %(message)s",
        date_format="%H:%M:%S"
    )