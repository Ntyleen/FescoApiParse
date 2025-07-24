import logging
import sys
from typing import Optional, Dict, Any
from pathlib import Path

# Импорт конфигурации (избегаем циклических импортов)
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from config.settings import LoggingConfig


def setup_logging_from_config(logging_config: 'LoggingConfig') -> None:
    """
    Настройка логирования на основе YAML конфигурации
    
    Args:
        logging_config: Объект конфигурации логирования из YAML
    """
    log_file = None
    if hasattr(logging_config, "get_file_path"):
        path = logging_config.get_file_path()
        log_file = str(path) if path else None
    else:
        log_file = logging_config.file

    
    setup_logging(
        level=logging_config.level,
        log_file=log_file,
        format_string=logging_config.format,
        date_format=logging_config.date_format,
        external_levels=logging_config.external_levels
    )


def setup_logging(
    level: str = "INFO",
    log_file: Optional[str] = None,
    format_string: Optional[str] = None,
    date_format: Optional[str] = None,
    external_levels: Optional[Dict[str, str]] = None
) -> None:
    """
    Настройка системы логирования
    
    Args:
        level: Уровень логирования (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Путь к файлу для записи логов (опционально)
        format_string: Кастомный формат логов
        date_format: Формат времени
        external_levels: Уровни логирования для внешних библиотек
    """
    
    # Конвертация уровня из строки
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    
    # Формат по умолчанию
    if format_string is None:
        format_string = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    
    # Формат времени по умолчанию
    if date_format is None:
        date_format = "%Y-%m-%d %H:%M:%S"
    
    # Создание форматтера
    formatter = logging.Formatter(format_string, datefmt=date_format)
    
    # Получение root logger и очистка существующих handlers
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)
    root_logger.handlers.clear()
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # File handler (если указан файл)
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Ротация логов (опционально)
        if _should_use_rotating_handler(log_file):
            file_handler = _create_rotating_handler(log_file, formatter)
        else:
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setFormatter(formatter)
        
        file_handler.setLevel(numeric_level)
        root_logger.addHandler(file_handler)
        
        logging.info(f"📝 Логи сохраняются в: {log_file}")
    
    # Настройка уровней для внешних библиотек
    if external_levels:
        for logger_name, ext_level in external_levels.items():
            try:
                ext_numeric_level = getattr(logging, ext_level.upper())
                logging.getLogger(logger_name).setLevel(ext_numeric_level)
            except AttributeError:
                logging.warning(f"⚠️ Неверный уровень логирования для {logger_name}: {ext_level}")
    
    # Настройка по умолчанию для внешних библиотек
    else:
        logging.getLogger("aiohttp").setLevel(logging.WARNING)
        logging.getLogger("asyncio").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)
    
    logging.info(f"📝 Логирование настроено: уровень {level}")


def _should_use_rotating_handler(log_file: str) -> bool:
    """Определяет, нужно ли использовать ротацию логов"""
    # Используем ротацию для продакшен логов
    return "prod" in log_file.lower() or "/var/log/" in log_file


def _create_rotating_handler(log_file: str, formatter: logging.Formatter):
    """Создает handler с ротацией логов"""
    try:
        from logging.handlers import RotatingFileHandler
        
        # 10MB файлы, 5 бэкапов
        handler = RotatingFileHandler(
            log_file, 
            maxBytes=10*1024*1024, 
            backupCount=5,
            encoding='utf-8'
        )
        handler.setFormatter(formatter)
        logging.info(f"📝 Включена ротация логов: {log_file}")
        return handler
        
    except ImportError:
        # Fallback на обычный FileHandler
        handler = logging.FileHandler(log_file, encoding='utf-8')
        handler.setFormatter(formatter)
        return handler


def get_logger(name: str) -> logging.Logger:
    """
    Получение именованного логгера
    
    Args:
        name: Имя логгера (обычно __name__)
    
    Returns:
        Настроенный логгер
    """
    return logging.getLogger(name)


def configure_fesco_logging(config: 'LoggingConfig') -> logging.Logger:
    """
    Настройка логирования специально для FESCO проекта
    
    Args:
        config: Конфигурация логирования
        
    Returns:
        Основной логгер для FESCO
    """
    
    # Настройка общего логирования
    setup_logging_from_config(config)
    
    # Создание специального логгера для FESCO
    fesco_logger = logging.getLogger("fesco_tracker")
    
    # Дополнительные настройки для FESCO логгера
    if config.level.upper() == "DEBUG":
        # В debug режиме добавляем информацию о файле и строке
        debug_format = "%(asctime)s [%(levelname)s] %(name)s:%(filename)s:%(lineno)d - %(message)s"
        debug_formatter = logging.Formatter(debug_format, datefmt=config.date_format)
        
        # Применяем debug формат ко всем handlers
        for handler in fesco_logger.handlers or logging.getLogger().handlers:
            handler.setFormatter(debug_formatter)
    
    return fesco_logger


class FescoLoggerAdapter(logging.LoggerAdapter):
    """
    Адаптер логгера с дополнительным контекстом для FESCO
    Автоматически добавляет контекстную информацию к сообщениям
    """
    
    def __init__(self, logger: logging.Logger, extra: Optional[Dict[str, Any]] = None):
        super().__init__(logger, extra or {})
    
    def process(self, msg, kwargs):
        """Добавляет контекстную информацию к сообщениям"""
        
        emoji_map = {
            logging.DEBUG: "🔍",
            logging.INFO: "ℹ️",
            logging.WARNING: "⚠️",
            logging.ERROR: "❌",
            logging.CRITICAL: "🚨"
        }
        
        # Получаем уровень из kwargs или определяем по умолчанию
        level = kwargs.get('level', logging.INFO)
        emoji = emoji_map.get(level, "📝")
        
        # Добавляем контекст из extra
        context_parts = []
        if self.extra:
            for key, value in self.extra.items():
                if key not in ['funcName', 'filename', 'lineno']:  # Исключаем стандартные поля
                    context_parts.append(f"{key}={value}")
        
        context_str = f"[{', '.join(context_parts)}] " if context_parts else ""
        
        # Форматируем сообщение
        formatted_msg = f"{emoji} {context_str}{msg}"
        
        return formatted_msg, kwargs


def create_container_logger(container_number: str) -> FescoLoggerAdapter:
    """
    Создание логгера для конкретного контейнера
    
    Args:
        container_number: Номер контейнера
        
    Returns:
        Адаптер логгера с контекстом контейнера
    """
    
    base_logger = logging.getLogger("fesco_tracker.container")
    return FescoLoggerAdapter(base_logger, {"container": container_number})


def create_api_logger(endpoint: str) -> FescoLoggerAdapter:
    """
    Создание логгера для API запросов
    
    Args:
        endpoint: Конечная точка API
        
    Returns:
        Адаптер логгера с контекстом API
    """
    
    base_logger = logging.getLogger("fesco_tracker.api")
    return FescoLoggerAdapter(base_logger, {"endpoint": endpoint})


# Декоратор для логирования времени выполнения функций
def log_execution_time(logger: Optional[logging.Logger] = None):
    """
    Декоратор для логирования времени выполнения функций
    
    Args:
        logger: Логгер для вывода (по умолчанию root)
    """
    import time
    from functools import wraps
    
    def decorator(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            start_time = time.perf_counter()
            func_logger = logger or logging.getLogger(func.__module__)
            
            func_logger.debug(f"🚀 Запуск {func.__name__}")
            
            try:
                result = await func(*args, **kwargs)
                execution_time = time.perf_counter() - start_time
                func_logger.info(f"✅ {func.__name__} завершен за {execution_time:.2f}s")
                return result
                
            except Exception as e:
                execution_time = time.perf_counter() - start_time
                func_logger.error(f"❌ {func.__name__} завершен с ошибкой за {execution_time:.2f}s: {e}")
                raise
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            start_time = time.perf_counter()
            func_logger = logger or logging.getLogger(func.__module__)
            
            func_logger.debug(f"🚀 Запуск {func.__name__}")
            
            try:
                result = func(*args, **kwargs)
                execution_time = time.perf_counter() - start_time
                func_logger.info(f"✅ {func.__name__} завершен за {execution_time:.2f}s")
                return result
                
            except Exception as e:
                execution_time = time.perf_counter() - start_time
                func_logger.error(f"❌ {func.__name__} завершен с ошибкой за {execution_time:.2f}s: {e}")
                raise
        
        # Возвращаем соответствующий wrapper в зависимости от типа функции
        import asyncio
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator