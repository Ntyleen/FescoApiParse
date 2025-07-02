# config/settings.py
"""
FESCO Container Tracking Configuration
Мигрированная версия с поддержкой YAML конфигурации
"""

import os
import yaml
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime
import re

from dotenv import load_dotenv

# Загружаем .env файл для секретов
load_dotenv(dotenv_path='E:/Repositories/FescoApiParse/deploy/.env/local.env')


class ConfigError(Exception):
    """Ошибка конфигурации"""
    pass


@dataclass
class ApiConfig:
    """Конфигурация FESCO API"""
    base_url: str = "https://api.fesco.com/api/v1/lk/"
    token_type: str = "Bearer"
    auth_token: str = ""
    timeout_seconds: int = 15
    max_parallel: int = 10
    max_retries: int = 3
    user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    
    def __post_init__(self):
        if not self.base_url.startswith(("http://", "https://")):
            raise ConfigError(f"Неверный формат base_url: {self.base_url}")
        if self.timeout_seconds <= 0:
            raise ConfigError("timeout_seconds должен быть положительным")
        if self.max_parallel <= 0:
            raise ConfigError("max_parallel должен быть положительным")


@dataclass
class RedisConfig:
    """Конфигурация Redis"""
    url: str = "redis://localhost:6379"
    prefix: str = "fesco_cache:"
    socket_timeout: int = 5
    retry_on_timeout: bool = True
    
    def __post_init__(self):
        if not self.url.startswith(("redis://", "rediss://")):
            raise ConfigError(f"Неверный формат Redis URL: {self.url}")


@dataclass
class CacheConfig:
    """Конфигурация кэширования"""
    type: str = "file"  # "file" или "redis"
    ttl_hours: float = 1.0
    dir: str = "./FescoApiParse/cache/"
    redis: RedisConfig = field(default_factory=RedisConfig)
    
    def __post_init__(self):
        if self.type not in ("file", "redis"):
            raise ConfigError(f"Неподдерживаемый тип кэша: {self.type}")
        if self.ttl_hours <= 0:
            raise ConfigError("ttl_hours должен быть положительным")
        
        # Создаем директорию кэша
        Path(self.dir).mkdir(parents=True, exist_ok=True)


@dataclass
class LoggingConfig:
    """Конфигурация логирования"""
    level: str = "INFO"
    format: str = "%(asctime)s [%(levelname)s] %(message)s"
    date_format: str = "%H:%M:%S"
    file: Optional[str] = None
    external_levels: Dict[str, str] = field(default_factory=lambda: {
        "aiohttp": "WARNING",
        "asyncio": "WARNING",
        "urllib3": "WARNING",
        "redis": "WARNING"
    })
    
    def __post_init__(self):
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if self.level.upper() not in valid_levels:
            raise ConfigError(f"Неверный уровень логирования: {self.level}")


@dataclass
class OutputConfig:
    """Конфигурация вывода результатов"""
    dir: str = "./FescoApiParse/output/"
    filename: str = "fesco_tracking_results.json"
    pretty_print: bool = True
    sort_by_container: bool = True
    include_metadata: bool = True
    
    def __post_init__(self):
        # Создаем директорию вывода
        Path(self.dir).mkdir(parents=True, exist_ok=True)
    
    def get_full_path(self) -> Path:
        """Получить полный путь к файлу результатов с подстановкой переменных"""
        filename = self.filename.format(
            date=datetime.now().strftime("%Y%m%d"),
            time=datetime.now().strftime("%H%M%S"),
            timestamp=int(datetime.now().timestamp())
        )
        return Path(self.dir) / filename


@dataclass
class ProcessingConfig:
    """Конфигурация обработки контейнеров"""
    batch_size: int = 50
    pause_between_batches: float = 0.5
    deduplicate_events: bool = True
    prefer_container_events: bool = True
    enable_retries: bool = True
    
    def __post_init__(self):
        if self.batch_size <= 0:
            raise ConfigError("batch_size должен быть положительным")
        if self.pause_between_batches < 0:
            raise ConfigError("pause_between_batches не может быть отрицательным")


@dataclass
class Config:
    """
    Главная конфигурация FESCO Container Tracking
    
    Поддерживает загрузку из YAML файлов с переопределением через окружения
    """
    api: ApiConfig = field(default_factory=ApiConfig)
    cache: CacheConfig = field(default_factory=CacheConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    output: OutputConfig = field(default_factory=OutputConfig)
    processing: ProcessingConfig = field(default_factory=ProcessingConfig)
    
    # Секреты из переменных окружения
    auth_token: str = ""
    
    @classmethod
    def from_yaml(
        cls, 
        config_files: Optional[List[str]] = None,
        environment: Optional[str] = None
    ) -> 'Config':
        """
        Загрузка конфигурации из YAML файлов
        
        Args:
            config_files: Дополнительные файлы конфигурации
            environment: Окружение (development, production, etc.)
        
        Returns:
            Загруженная конфигурация
        """
        
        # Определение окружения
        if environment is None:
            environment = os.getenv("ENVIRONMENT", "local")
        
        # Путь к директории конфигурации
        config_dir = Path("./FescoApiParse/deploy/config/")
        
        # Базовые файлы конфигурации (в порядке приоритета)
        base_files = [
            config_dir / "app_global.yaml",
            config_dir / f"{environment}.yaml",
            config_dir / "local.yaml"  # Локальные переопределения
        ]
        
        # Добавляем пользовательские файлы
        if config_files:
            base_files.extend([Path(f) for f in config_files])
        
        # Загрузка и объединение конфигураций
        merged_config = {}
        loaded_files = []
        
        for config_file in base_files:
            if config_file.exists():
                try:
                    with open(config_file, 'r', encoding='utf-8') as f:
                        # YAML может содержать несколько документов (разделенных ---)
                        documents = yaml.safe_load_all(f)
                        for doc in documents:
                            if doc:  # Пропускаем пустые документы
                                merged_config = cls._deep_merge(merged_config, doc)
                    
                    loaded_files.append(str(config_file.name))
                    
                except yaml.YAMLError as e:
                    raise ConfigError(f"Ошибка парсинга YAML файла {config_file}: {e}")
                except Exception as e:
                    raise ConfigError(f"Ошибка загрузки файла {config_file}: {e}")
        
        if not loaded_files:
            logging.warning("⚠️ Не найдено файлов конфигурации, используются значения по умолчанию")
            merged_config = {}
        else:
            logging.info(f"📁 Загружены конфигурации: {', '.join(loaded_files)}")
        
        # Подстановка переменных окружения
        merged_config = cls._substitute_env_vars(merged_config)
        
        # Создание объекта конфигурации
        try:
            config = cls._create_from_dict(merged_config)
            print(f"{config.auth_token}")
            # Загрузка секретов из переменных окружения
            config.auth_token = os.getenv("FESCO_TOKEN", "")
            if not config.auth_token:
                raise ConfigError("❌ Не найден FESCO_TOKEN в переменных окружения")
            
            # Применение дополнительных переопределений из .env
           # config._apply_env_overrides()
            
            logging.info(f"✅ Конфигурация загружена для окружения: {environment}")
            logging.info(f"✅ Токен загружен: {config.auth_token[:10]}...")
            
            return config
            
        except Exception as e:
            raise ConfigError(f"Ошибка создания конфигурации: {e}")
    
    # @classmethod
    # def from_env(cls) -> 'Config':
        # """
        # Обратная совместимость: загрузка только из переменных окружения
        # 
        # @deprecated: Используйте load_config() для загрузки из YAML
        # """
        # logging.warning("⚠️ Config.from_env() устарел, используйте load_config()")
        # 
      #  Создаем базовую конфигурацию
        # config = cls()
        # 
      #  Загружаем токен
        # config.auth_token = os.getenv("FESCO_TOKEN", "")
        # if not config.auth_token:
            # raise ConfigError("❌ Не найден FESCO_TOKEN в переменных окружения")
        # 
       # Применяем переопределения из переменных окружения
        # config._apply_env_overrides()
        # 
        # logging.info(f"✅ Конфигурация загружена из переменных окружения")
        # return config
    # 
    # def _apply_env_overrides(self) -> None:
        # """Применение переопределений из переменных окружения"""
        # 
       # API переопределения
        # if timeout := os.getenv("FESCO_TIMEOUT_SECONDS"):
            # self.api.timeout_seconds = int(timeout)
        # 
        # if parallel := os.getenv("FESCO_MAX_PARALLEL"):
            # self.api.max_parallel = int(parallel)
        # 
        # if base_url := os.getenv("FESCO_BASE_URL"):
            # self.api.base_url = base_url
        # 
        # if token_type := os.getenv("FESCO_TOKEN_TYPE"):
            # self.api.token_type = token_type
        # 
       # Cache переопределения
        # if cache_type := os.getenv("FESCO_CACHE_TYPE"):
            # self.cache.type = cache_type
        # 
        # if cache_ttl := os.getenv("FESCO_CACHE_TTL_HOURS"):
            # self.cache.ttl_hours = float(cache_ttl)
        # 
        # if cache_dir := os.getenv("FESCO_CACHE_DIR"):
            # self.cache.dir = cache_dir
        # 
        # if redis_url := os.getenv("REDIS_URL"):
            # self.cache.redis.url = redis_url
        # 
      #  Logging переопределения
        # if log_level := os.getenv("FESCO_LOG_LEVEL"):
            # self.logging.level = log_level
        # 
        # if log_file := os.getenv("FESCO_LOG_FILE"):
            # self.logging.file = log_file
    
    @staticmethod
    def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """Глубокое объединение словарей"""
        result = base.copy()
        
        for key, value in override.items():
            if (key in result and 
                isinstance(result[key], dict) and 
                isinstance(value, dict)):
                result[key] = Config._deep_merge(result[key], value)
            else:
                result[key] = value
        
        return result
    
    @staticmethod
    def _substitute_env_vars(config: Dict[str, Any]) -> Dict[str, Any]:
        """Рекурсивная подстановка переменных окружения в формате ${VAR_NAME}"""
        
        def substitute_value(value):
            if isinstance(value, str):
                # Поиск и замена переменных окружения
                pattern = re.compile(r'\$\{([^}]+)\}')
                
                def replace_var(match):
                    var_name = match.group(1)
                    env_value = os.getenv(var_name)
                    if env_value is None:
                        logging.warning(f"⚠️ Переменная окружения {var_name} не найдена")
                        return match.group(0)  # Возвращаем исходное значение
                    return env_value
                
                return pattern.sub(replace_var, value)
            
            elif isinstance(value, dict):
                return {k: substitute_value(v) for k, v in value.items()}
            
            elif isinstance(value, list):
                return [substitute_value(item) for item in value]
            
            else:
                return value
        
        return substitute_value(config)
    
    @classmethod
    def _create_from_dict(cls, config_dict: Dict[str, Any]) -> 'Config':
        """Создание объекта конфигурации из словаря"""
        
        # API конфигурация
        api_dict = config_dict.get("api", {})
        api_config = ApiConfig(**api_dict)
        
        # Cache конфигурация
        cache_dict = config_dict.get("cache", {})
        redis_dict = cache_dict.pop("redis", {})
        redis_config = RedisConfig(**redis_dict)
        cache_config = CacheConfig(**cache_dict)
        cache_config.redis = redis_config
        
        # Остальные конфигурации
        logging_config = LoggingConfig(**config_dict.get("logging", {}))
        output_config = OutputConfig(**config_dict.get("output", {}))
        processing_config = ProcessingConfig(**config_dict.get("processing", {}))
        
        return cls(
            api=api_config,
            cache=cache_config,
            logging=logging_config,
            output=output_config,
            processing=processing_config
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Преобразование конфигурации в словарь (для отладки)"""
        import dataclasses
        
        def convert_dataclass(obj):
            if dataclasses.is_dataclass(obj):
                result = {}
                for field in dataclasses.fields(obj):
                    value = getattr(obj, field.name)
                    result[field.name] = convert_dataclass(value)
                return result
            return obj
        
        result = convert_dataclass(self)
        # Скрываем секретные данные
        result["auth_token"] = "***"
        return result


# Удобная функция для загрузки конфигурации
def load_config(
    environment: Optional[str] = None,
    config_files: Optional[List[str]] = None
) -> Config:
    """
    Загрузка конфигурации FESCO Container Tracking
    
    Args:
        environment: Окружение (development, production, auto-detect если None)
        config_files: Дополнительные файлы конфигурации
    
    Returns:
        Загруженная и валидированная конфигурация
    
    Examples:
        >>> # Простая загрузка
        >>> config = load_config()
        
        >>> # Для продакшена
        >>> config = load_config(environment="production")
        
        >>> # С дополнительными файлами
        >>> config = load_config(config_files=["./custom.yaml"])
    """
    return Config.from_yaml(config_files, environment)