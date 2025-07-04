# database/__init__.py
"""
FESCO Container Tracking - Database Package
===========================================

Модуль для работы с базами данных. Организован по принципу "разделения ответственности":

Data Flow Architecture:
    ┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
    │  Source DB      │───▶│   Processing     │───▶│   Target DB     │
    │ (Ваша основная) │    │   (FESCO API)    │    │ (Сторонняя)     │
    └─────────────────┘    └──────────────────┘    └─────────────────┘
            │                        │                        │
      DatabaseContainerSource  ContainerEvent/           ExternalDatabaseWriter
            │                  TrackingResult                  │
    ┌─────────────────┐                              ┌─────────────────┐
    │   Источники     │                              │    Назначения   │
    │   данных        │                              │    данных       │
    └─────────────────┘                              └─────────────────┘

Компоненты по назначению:

    Sources (Источники данных):
        ├── DatabaseContainerSource   # Загрузка контейнеров из вашей БД
        ├── ContainerInfo            # Модель данных контейнера
        └── Batch operations         # Пакетная загрузка

    Writers (Назначения данных):
        ├── ExternalDatabaseWriter   # Запись в стороннюю БД  
        ├── TableConfig             # Конфигурация таблиц
        ├── ColumnMapping           # Маппинг полей
        └── Predefined configs      # Готовые конфигурации

    Utilities (Утилиты):
        ├── Connection management   # Управление подключениями
        ├── Configuration helpers  # Помощники конфигурации
        └── Validation tools       # Инструменты валидации
"""

# =============================================================================
# ИМПОРТЫ - Источники данных (где берем контейнеры)
# =============================================================================

# Основной источник контейнеров из БД
from .container_source import DatabaseContainerSource, ContainerInfo

# =============================================================================
# ИМПОРТЫ - Назначения данных (куда пишем результаты)
# =============================================================================

# Писатель в стороннюю БД
from .external_writer import (
    ExternalDatabaseWriter,
    TableConfig,
    ColumnMapping,
    create_shipment_table_config,
    create_tracking_events_table_config
)

# =============================================================================
# ИМПОРТЫ - Утилиты и вспомогательные компоненты  
# =============================================================================

# Пока в заглушках - будем добавлять по мере необходимости
# from .connection import DatabaseConnection
# from .migrations import MigrationManager


# =============================================================================
# ПУБЛИЧНЫЙ API - Что доступно при импорте database
# =============================================================================

__all__ = [
    # === ИСТОЧНИКИ ДАННЫХ ===
    'DatabaseContainerSource',      # Главный источник контейнеров
    'ContainerInfo',                # Модель данных контейнера
    
    # === НАЗНАЧЕНИЯ ДАННЫХ ===
    'ExternalDatabaseWriter',       # Писатель результатов
    'TableConfig',                  # Конфигурация таблицы
    'ColumnMapping',                # Маппинг колонок
    
    # === ГОТОВЫЕ КОНФИГУРАЦИИ ===
    'create_shipment_table_config',      # Конфиг таблицы отгрузок
    'create_tracking_events_table_config', # Конфиг таблицы событий
    
    # === ФАБРИЧНЫЕ ФУНКЦИИ ===
    'create_container_source',      # Создать источник контейнеров
    'create_external_writer',       # Создать писатель результатов
    'create_database_config',       # Создать конфигурацию БД
    
    # === УТИЛИТЫ ===
    'validate_database_config',     # Валидация конфигурации
    'test_database_connections',    # Тестирование соединений
    'get_database_info',           # Информация о БД компонентах
    
    # === ПРЕДОПРЕДЕЛЕННЫЕ КОНФИГУРАЦИИ ===
    'COMMON_SOURCE_CONFIGS',       # Типовые конфигурации источников
    'COMMON_TARGET_CONFIGS',       # Типовые конфигурации целей
]


# =============================================================================
# ФАБРИЧНЫЕ ФУНКЦИИ - Удобное создание компонентов
# =============================================================================

def create_container_source(
    host: str,
    user: str,
    password: str,
    database: str,
    table: str = "containers",
    container_column: str = "container_number",
    **kwargs
) -> DatabaseContainerSource:
    """
    Создать источник контейнеров с упрощенной конфигурацией
    
    Это функция "для ленивых" - передаете основные параметры,
    получаете готовый к работе источник данных.
    
    Args:
        host: Хост БД
        user: Пользователь БД  
        password: Пароль
        database: Имя базы данных
        table: Имя таблицы с контейнерами
        container_column: Имя колонки с номерами контейнеров
        **kwargs: Дополнительные параметры (port, status_column, etc.)
        
    Returns:
        DatabaseContainerSource: Готовый источник данных
        
    Example:
        >>> from database import create_container_source
        >>> 
        >>> source = create_container_source(
        ...     host="localhost",
        ...     user="myapp_user", 
        ...     password="secret",
        ...     database="shipping_db",
        ...     table="shipment_containers",
        ...     container_column="container_no"
        ... )
        >>> 
        >>> await source.connect()
        >>> containers = await source.get_containers_list(limit=100)
    """
    
    config = {
        'host': host,
        'port': kwargs.get('port', 3306),
        'user': user,
        'password': password,
        'database': database,
        'table': table,
        'column': container_column,
        'status_column': kwargs.get('status_column', 'status'),
        'priority_column': kwargs.get('priority_column', 'priority')
    }
    
    return DatabaseContainerSource(config)


def create_external_writer(
    host: str,
    user: str,
    password: str,
    database: str,
    table_configs: list = None,
    **kwargs
) -> ExternalDatabaseWriter:
    """
    Создать писатель в стороннюю БД с упрощенной конфигурацией
    
    Args:
        host: Хост целевой БД
        user: Пользователь
        password: Пароль  
        database: Имя базы данных
        table_configs: Список конфигураций таблиц (если None - используем стандартные)
        **kwargs: Дополнительные параметры
        
    Returns:
        ExternalDatabaseWriter: Готовый писатель
        
    Example:
        >>> from database import create_external_writer
        >>> 
        >>> writer = create_external_writer(
        ...     host="external-system.company.com",
        ...     user="integration_user",
        ...     password="integration_pass", 
        ...     database="shipment_management"
        ... )
        >>> 
        >>> await writer.connect()
    """
    
    db_config = {
        'host': host,
        'port': kwargs.get('port', 3306),
        'user': user,
        'password': password,
        'database': database
    }
    
    # Используем стандартные конфигурации если не указаны
    if table_configs is None:
        table_configs = [create_shipment_table_config()]
    
    return ExternalDatabaseWriter(db_config, table_configs)


def create_database_config(
    source_db: dict,
    target_db: dict,
    table_mappings: dict = None
) -> dict:
    """
    Создать полную конфигурацию БД для системы
    
    Это функция "архитектора" - создает полную конфигурацию
    для интеграции между источником и целью.
    
    Args:
        source_db: Конфигурация БД источника
        target_db: Конфигурация целевой БД
        table_mappings: Маппинг таблиц (опционально)
        
    Returns:
        dict: Полная конфигурация системы БД
        
    Example:
        >>> config = create_database_config(
        ...     source_db={
        ...         'host': 'main-db.company.com',
        ...         'database': 'shipping_system',
        ...         'table': 'active_containers'
        ...     },
        ...     target_db={
        ...         'host': 'external-api.partner.com',
        ...         'database': 'logistics_data'
        ...     }
        ... )
    """
    
    config = {
        'source': source_db,
        'target': target_db,
        'created_at': None,  # Можно добавить timestamp
        'version': '1.0'
    }
    
    if table_mappings:
        config['table_mappings'] = table_mappings
    
    return config


# =============================================================================
# УТИЛИТЫ - Валидация и диагностика
# =============================================================================

async def validate_database_config(config: dict) -> dict:
    """
    Валидация конфигурации БД
    
    Проверяет корректность настроек и доступность БД.
    
    Args:
        config: Конфигурация для проверки
        
    Returns:
        dict: Результаты валидации
        
    Example:
        >>> config = {'host': 'localhost', 'user': 'test', ...}
        >>> result = await validate_database_config(config)
        >>> 
        >>> if result['valid']:
        ...     print("✅ Конфигурация корректна")
        >>> else:
        ...     for error in result['errors']:
        ...         print(f"❌ {error}")
    """
    
    errors = []
    warnings = []
    
    # Проверка обязательных полей
    required_fields = ['host', 'user', 'password', 'database']
    for field in required_fields:
        if not config.get(field):
            errors.append(f"Отсутствует обязательное поле: {field}")
    
    # Проверка формата хоста
    host = config.get('host', '')
    if host and not (host.startswith(('localhost', '127.0.0.1')) or '.' in host):
        warnings.append("Хост может быть некорректным")
    
    # Проверка порта
    port = config.get('port', 3306)
    if not isinstance(port, int) or port <= 0 or port > 65535:
        errors.append("Некорректный порт БД")
    
    # TODO: Здесь можно добавить реальную проверку подключения
    # try:
    #     test_connection = await create_test_connection(config)
    #     await test_connection.close()
    # except Exception as e:
    #     errors.append(f"Не удается подключиться к БД: {e}")
    
    return {
        'valid': len(errors) == 0,
        'errors': errors,
        'warnings': warnings,
        'config_summary': {
            'host': config.get('host', 'not_set'),
            'database': config.get('database', 'not_set'),
            'has_credentials': bool(config.get('user') and config.get('password'))
        }
    }


async def test_database_connections(source_config: dict, target_config: dict) -> dict:
    """
    Тестирование подключений к источнику и цели
    
    Args:
        source_config: Конфигурация БД источника
        target_config: Конфигурация целевой БД
        
    Returns:
        dict: Результаты тестирования
    """
    
    results = {
        'source': {'connected': False, 'error': None, 'latency_ms': 0},
        'target': {'connected': False, 'error': None, 'latency_ms': 0},
        'overall_status': 'unknown'
    }
    
    # TODO: Реальное тестирование подключений
    # Пока возвращаем заглушку
    
    results['overall_status'] = 'testing_not_implemented'
    
    return results


def get_database_info() -> dict:
    """
    Получить информацию о доступных компонентах БД
    
    Полезно для диагностики и документации.
    
    Returns:
        dict: Информация о компонентах
    """
    
    return {
        'version': '1.0.0',
        'components': {
            'sources': {
                'DatabaseContainerSource': {
                    'description': 'Источник контейнеров из реляционной БД',
                    'supported_databases': ['MySQL', 'PostgreSQL', 'MariaDB'],
                    'features': ['batch_loading', 'filtering', 'priority_sorting']
                }
            },
            'writers': {
                'ExternalDatabaseWriter': {
                    'description': 'Запись результатов в стороннюю БД',
                    'features': ['field_mapping', 'conditional_writes', 'upsert_operations'],
                    'supported_transforms': ['upper', 'lower', 'date', 'trim', 'not_null']
                }
            }
        },
        'predefined_configs': {
            'shipment_table': 'Конфигурация для таблицы отгрузок',
            'tracking_events': 'Конфигурация для таблицы событий'
        },
        'requirements': {
            'python_packages': ['aiomysql>=0.1.1'],
            'database_permissions': ['SELECT (source)', 'INSERT, UPDATE (target)']
        }
    }


# =============================================================================
# ПРЕДОПРЕДЕЛЕННЫЕ КОНФИГУРАЦИИ - Готовые шаблоны
# =============================================================================

COMMON_SOURCE_CONFIGS = {
    'mysql_localhost': {
        'host': 'localhost',
        'port': 3306,
        'table': 'containers',
        'column': 'container_number',
        'status_column': 'status',
        'description': 'Стандартная конфигурация для локального MySQL'
    },
    
    'mysql_production': {
        'host': '${PROD_DB_HOST}',
        'port': 3306,
        'user': '${PROD_DB_USER}',
        'password': '${PROD_DB_PASSWORD}',
        'table': 'shipment_containers',
        'column': 'container_no',
        'status_column': 'processing_status',
        'description': 'Продакшен конфигурация с переменными окружения'
    },
    
    'postgresql_standard': {
        'host': 'localhost',
        'port': 5432,
        'table': 'logistics_containers',
        'column': 'container_number',
        'description': 'Стандартная конфигурация для PostgreSQL'
    }
}


COMMON_TARGET_CONFIGS = {
    'shipment_management': {
        'tables': ['shipments', 'tracking_events'],
        'primary_use': 'Запись в систему управления отгрузками',
        'table_configs': 'use create_shipment_table_config()'
    },
    
    'logistics_platform': {
        'tables': ['container_tracking', 'logistics_events'],
        'primary_use': 'Интеграция с логистической платформой',
        'table_configs': 'use create_tracking_events_table_config()'
    },
    
    'reporting_warehouse': {
        'tables': ['fact_container_events', 'dim_containers'],
        'primary_use': 'Запись в хранилище данных для отчетности',
        'table_configs': 'custom configuration required'
    }
}


# =============================================================================
# КАСТОМНЫЕ КОНФИГУРАЦИИ - Примеры для разных сценариев
# =============================================================================

def create_custom_table_config(
    table_name: str,
    primary_key: str,
    container_column: str,
    field_mappings: dict
) -> TableConfig:
    """
    Создать кастомную конфигурацию таблицы
    
    Для случаев, когда стандартные конфигурации не подходят.
    
    Args:
        table_name: Имя таблицы
        primary_key: Первичный ключ
        container_column: Колонка с номером контейнера
        field_mappings: Словарь маппингов полей
        
    Returns:
        TableConfig: Кастомная конфигурация
        
    Example:
        >>> custom_config = create_custom_table_config(
        ...     table_name='my_custom_table',
        ...     primary_key='id',
        ...     container_column='container_ref',
        ...     field_mappings={
        ...         'status_code': {'fesco_field': 'operation', 'transform': 'upper'},
        ...         'last_position': {'fesco_field': 'location', 'transform': 'trim'}
        ...     }
        ... )
    """
    
    # Преобразуем простые маппинги в объекты ColumnMapping
    columns = {}
    for column_name, mapping_info in field_mappings.items():
        columns[column_name] = ColumnMapping(
            fesco_field=mapping_info['fesco_field'],
            target_column=column_name,
            transform_func=mapping_info.get('transform'),
            condition_value=mapping_info.get('condition')
        )
    
    return TableConfig(
        table_name=table_name,
        primary_key=primary_key,
        container_column=container_column,
        columns=columns
    )


# =============================================================================
# МЕТАДАННЫЕ ПАКЕТА
# =============================================================================

__version__ = "1.0.0"
__description__ = "Database integration components for FESCO Container Tracking"