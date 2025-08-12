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
from typing import Dict, Any, Optional, List, Union, TypeVar, Generic
from datetime import datetime
import re

from dotenv import load_dotenv

ConfigValue = Union[str, int, float, bool, None, Dict[str, Any], List[Any]]

# Загружаем .env файл для секретов
load_dotenv(dotenv_path='./deploy/.env/dev.env')


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
    """Конфигурация Redis подключения"""
    url: str = "redis://localhost:6379"
    max_connections: int = 20
    socket_timeout: int = 5
    socket_connect_timeout: int = 5
    retry_on_timeout: bool = True
    decode_responses: bool = True
    
    # Настройки для разных namespace'ов
    default_ttl_hours: float = 1.0
    cache_prefix: str = "fesco_cache:"
    binding_prefix: str = "fesco_bindings:"
    
    def __post_init__(self):
        if not self.url.startswith(("redis://", "rediss://")):
            raise ConfigError(f"Неверный формат Redis URL: {self.url}")
        if self.default_ttl_hours <= 0:
            raise ConfigError("default_ttl_hours должен быть положительным")


@dataclass
class FirebirdDatabaseConfig:
    """Конфигурация Firebird БД для работы с entity таблицей"""
    
    # === ПОДКЛЮЧЕНИЕ К FIREBIRD ===
    host: str = "localhost"
    port: int = 3050  # Стандартный порт Firebird
    user: str = "SYSDBA"  # Стандартный пользователь
    password: str = ""
    database: str = ""  # Путь к .fdb файлу
    dsn: Optional[str] = None  # Альтернативный способ подключения
    charset: str = "UTF8"
    
    # === НАСТРОЙКИ ENTITY ТАБЛИЦЫ ===
    table_name: str = "ENTITY"
    primary_key: str = "ID"
    container_column: str = "NAME"
    status_column: str = "SP_ENTITY_STATUS"
    line_column: str = "LEGAL_PERSON_LINE_ID"
    railway_carrier_column: str = "LEGAL_PERSON_RAILWAY_CARRIER_ID"
    
    # === КОЛОНКИ ДАТ ===
    date_eta: str = "DATE_ETA"
    date_etd: str = "DATE_ETD"
    date_in: str = "DATE_IN"
    date_railway_loading: str = "DATE_RAILWAY_LOADING"
    date_railway_delivery: str = "DATE_RAILWAY_DELIVERY"
    remaining_distance: str = "TRACING_DAYS"
    
    # === НАСТРОЙКИ ОБРАБОТКИ ===
    excluded_status_ids: List[int] = field(default_factory=lambda: [8, 9, 24])  # закрыто, доставлено, отменено
    target_line_ids: List[int] = field(default_factory=lambda: [451, 751])  # Если пусто - все линии
    target_railway_carrier_ids: List[int] = field(default_factory=list)
    batch_size: int = 100
    max_connections: int = 10
    
    # === КАСТОМНЫЕ МАППИНГИ ===
    custom_date_mappings: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    
    def __post_init__(self):
        self.port = int(self.port)  # Приводим порт к int

        # Приведение target_line_ids к List[int]
        if isinstance(self.target_line_ids, list):
            self.target_line_ids = [int(x) for x in self.target_line_ids]
        elif self.target_line_ids is None:
            self.target_line_ids = []
        else:
            # Одинарное значение (int/str и др.)
            self.target_line_ids = [int(self.target_line_ids)]

        # Приведение target_railway_carrier_ids к List[int]
        if isinstance(self.target_railway_carrier_ids, list):
            self.target_railway_carrier_ids = [int(x) for x in self.target_railway_carrier_ids]
        elif self.target_railway_carrier_ids is None:
            self.target_railway_carrier_ids = []
        else:
            self.target_railway_carrier_ids = [int(self.target_railway_carrier_ids)]

        """Валидация Firebird конфигурации"""
        if not self.host:
            raise ConfigError("Firebird host не может быть пустым")
        if not self.user:
            raise ConfigError("Firebird user не может быть пустым")
        if not self.database:
            raise ConfigError("Firebird database path не может быть пустым")
        if self.port <= 0 or self.port > 65535:
            raise ConfigError("Firebird port должен быть от 1 до 65535")
    
    def to_firebird_config(self) -> Dict[str, Any]:
        """Преобразовать в формат для FirebirdConnectionManager"""
        config = {
            'host': self.host,
            'port': self.port,
            'user': self.user,
            'password': self.password,
            'database': self.database,
            'charset': self.charset
        }
        
        if self.dsn:
            config['dsn'] = self.dsn
            
        return config
    


@dataclass
class CacheConfig:
    """Конфигурация кэширования"""
    type: str = "file"  # "file" или "redis"
    ttl_hours: float = 1.0
    dir: str = "./app_cache/"
    redis: RedisConfig = field(default_factory=RedisConfig)
    cache_prefix: str = "cache_fesco:"
    binding_prefix: str = "bindings_fesco:"

    def __post_init__(self):
        if self.type not in ("file", "redis", "Redis"):
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

    def get_file_path(self) -> Optional[Path]:
        if not self.file:
            return None

        filename = self.file.format(
            date=datetime.now().strftime("%d.%m.%Y"),
            time=datetime.now().strftime("%H.%M.%S"),
            timestamp=int(datetime.now().timestamp()),
        )
        path = Path(filename)
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

@dataclass
class OutputConfig:
    """Конфигурация вывода результатов"""
    dir: str = "./app_output/"
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
class SchedulerConfig:
    """Configuration for background scheduler"""
    enabled: bool = False
    cron: str = "0 * * * *"

    def __post_init__(self):
        if not isinstance(self.enabled, bool):
            raise ConfigError("enabled должен быть bool")


class EnvironmentSubstitutor:
    """
    Отдельный класс для подстановки переменных окружения.
    """
    
    def __init__(self):
        # Компилируем regex один раз для производительности
        self._env_pattern = re.compile(r'\$\{([^}]+)\}')
    
    def _replace_env_variables(self, text: str) -> str:
        """Заменяет переменные окружения в строке"""
        def replace_var(match):
            var_name = match.group(1)
            env_value = os.getenv(var_name)
            if env_value is None:
                logging.warning(f"⚠️ Переменная окружения {var_name} не найдена")
                return match.group(0)  # Возвращаем исходное значение
            return env_value
        
        return self._env_pattern.sub(replace_var, text)
    
    def substitute_in_value(self, value: ConfigValue) -> ConfigValue:
        """
        Подстановка переменных в произвольном значении конфигурации
        
        Сохраняет структуру и типы данных
        """
        if isinstance(value, str):
            return self._replace_env_variables(value)
        elif isinstance(value, dict):
            return self.substitute_in_dict(value)
        elif isinstance(value, list):
            return self.substitute_in_list(value)
        else:
            # Примитивные типы (int, bool, None) возвращаем как есть
            return value
    
    def substitute_in_dict(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Подстановка переменных в словаре
        
        Четкий контракт: Dict -> Dict
        """
        return {
            key: self.substitute_in_value(value) 
            for key, value in data.items()
        }
    
    def substitute_in_list(self, data: List[Any]) -> List[Any]:
        """
        Подстановка переменных в списке
        
        Четкий контракт: List -> List
        """
        return [
            self.substitute_in_value(item) 
            for item in data
        ]

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
    scheduler: SchedulerConfig = field(default_factory=SchedulerConfig)
    database: FirebirdDatabaseConfig = field(default_factory=FirebirdDatabaseConfig)

    
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
            environment = os.getenv("ENVIRONMENT", "production")
        
        # Путь к директории конфигурации
        config_dir = Path("./deploy/config/")
        
        # Базовые файлы конфигурации (в порядке приоритета)
        base_files = [
            config_dir / "app_global.yaml",
            config_dir / f"{environment}.yaml",
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

            # Загрузка секретов из переменных окружения
            config.auth_token = os.getenv("FESCO_TOKEN", config.auth_token)
            if not config.auth_token:
                raise ConfigError("❌ Не найден FESCO_TOKEN в переменных окружения")
            
            # Применение дополнительных переопределений из .env
            config.database.password = os.getenv("DB_PASSWORD", config.database.password)
            config.database.user = os.getenv("DB_USER", config.database.user)
            config.database.port = int(os.getenv('DB_PORT', config.database.port))
            
            logging.info(f"✅ Конфигурация загружена для окружения: {environment}")
            logging.info(f"✅ Токен загружен: {config.auth_token[:10]}...")
            logging.info(f"✅ БД источник: {config.database.host}/{config.database.database}")
            
            return config
            
        except Exception as e:
            raise ConfigError(f"Ошибка создания конфигурации: {e}")
    
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
        """
        Подстановка переменных окружения в конфигурации
        
        Теперь это простая функция-оркестратор с понятной логикой:
        1. Создаем специализированный инструмент
        2. Используем его метод с четким контрактом Dict -> Dict
        3. Возвращаем результат
        
        Никаких неожиданностей, никаких нарушений контракта!
        """
        substitutor = EnvironmentSubstitutor()
        return substitutor.substitute_in_dict(config)
    
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

        # БД конфигурации
        database_dict = config_dict.get("BrokerDatabase", {})
        database_config = FirebirdDatabaseConfig(**database_dict)
        
        # Остальные конфигурации
        logging_config = LoggingConfig(**config_dict.get("logging", {}))
        output_config = OutputConfig(**config_dict.get("output", {}))
        processing_config = ProcessingConfig(**config_dict.get("processing", {}))
        scheduler_config = SchedulerConfig(**config_dict.get("scheduler", {}))

        return cls(
            api=api_config,
            cache=cache_config,
            logging=logging_config,
            output=output_config,
            processing=processing_config,
            scheduler=scheduler_config,
            database=database_config,
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Преобразование конфигурации в словарь (для отладки)"""
        import dataclasses
        from typing import Any, Dict, Union
        
        def convert_dataclass(obj: Any) -> Any:
            """Рекурсивное преобразование dataclass в словарь"""
            if dataclasses.is_dataclass(obj):
                result = {}
                for field in dataclasses.fields(obj):
                    value = getattr(obj, field.name)
                    result[field.name] = convert_dataclass(value)
                return result
            elif isinstance(obj, (list, tuple)):
                # Обрабатываем списки и кортежи
                return [convert_dataclass(item) for item in obj]
            elif isinstance(obj, dict):
                # Обрабатываем обычные словари
                return {k: convert_dataclass(v) for k, v in obj.items()}
            else:
                # Для простых типов (str, int, bool, None) возвращаем как есть
                return obj
        
        # Гарантированно получаем словарь, так как self - это dataclass
        result: Dict[str, Any] = convert_dataclass(self)
        
        # Теперь безопасно работаем со словарем
        # Скрываем секретные данные
        if "auth_token" in result:
            result["auth_token"] = "***"
        
        if "database" in result and isinstance(result["database"], dict):
            result["database"]["password"] = "***"
        
        if "external_database" in result and isinstance(result["external_database"], dict):
            result["external_database"]["password"] = "***"
            
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