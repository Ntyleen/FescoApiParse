"""
FESCO Container Tracking Configuration Module

Предоставляет загрузку конфигурации из YAML файлов с поддержкой
различных окружений и переопределения через переменные окружения.
"""

from .settings import (
    Config,
    ApiConfig,
    CacheConfig,
    RedisConfig,
    LoggingConfig,
    OutputConfig,
    ProcessingConfig,
    SchedulerConfig,
    GoogleSheetsConfig,
    ConfigError,
    load_config,
)

__all__ = [
    # Основные классы конфигурации
    'Config',
    'ApiConfig', 
    'CacheConfig',
    'RedisConfig',
    'LoggingConfig',
    'OutputConfig',
    'ProcessingConfig',
    'SchedulerConfig',
    'GoogleSheetsConfig',
    
    # Исключения
    'ConfigError',
    
    # Основная функция загрузки
    'load_config'
]

__version__ = "0.0.1"
__description__ = "YAML-based configuration system for FESCO Container Tracking"
