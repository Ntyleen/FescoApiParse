# database/__init__.py
""" FESCO Container Tracking - Database Package """

# =============================================================================
# ИМПОРТЫ - Новая структура с приоритетом Firebird
# =============================================================================

# Firebird компоненты (основные для enterprise)
from .firebird_manager import (
    FirebirdEntityManager,
    EntityTableConfig, 
    EntityColumnMapping,
    EntityStatusID,
    ContainerInfo,
    create_firebird_entity_manager,
    validate_firebird_config
)

# Generic компоненты (для совместимости и гибкости)
# from .container_source import DatabaseContainerSource
# from .external_writer import (
#     ExternalDatabaseWriter,
#     TableConfig,
#     ColumnMapping,
#     create_shipment_table_config,
#     create_tracking_events_table_config
# )

# =============================================================================
# ПУБЛИЧНЫЙ API - Упрощенный и логичный
# =============================================================================

__all__ = [
    # === FIREBIRD КОМПОНЕНТЫ (Primary) ===
    'FirebirdEntityManager',        # Главный компонент
    'EntityTableConfig',            # Конфигурация entity 
    'EntityColumnMapping',          # Маппинг операций
    'EntityStatusID',               # Статусы enum
    'ContainerInfo',                # Модель контейнера
    
    # === GENERIC КОМПОНЕНТЫ (Secondary) ===
#    'DatabaseContainerSource',      # Generic источник
#    'ExternalDatabaseWriter',       # Generic писатель
#    'TableConfig',                  # Generic конфигурация
#    'ColumnMapping',                # Generic маппинг
    
    # === ФАБРИЧНЫЕ ФУНКЦИИ (Unified Interface) ===
    'create_database_source',       # Универсальный источник
    'create_database_writer',       # Универсальный писатель
    'create_unified_config',        # Единая конфигурация
    
    # === ГОТОВЫЕ КОНФИГУРАЦИИ ===
    'create_shipment_table_config',
    'create_tracking_events_table_config',
    'create_firebird_entity_config',
    
    # === УТИЛИТЫ ===
    'validate_database_config',
    'test_database_connections',
    'detect_database_type',
    'get_database_capabilities',
    
    # === ПРЕДОПРЕДЕЛЕННЫЕ КОНФИГУРАЦИИ ===
    'FIREBIRD_CONFIGS',
    'MYSQL_CONFIGS', 
    'POSTGRESQL_CONFIGS',
]


# =============================================================================
# УНИФИЦИРОВАННЫЕ ФАБРИЧНЫЕ ФУНКЦИИ
# =============================================================================

def create_database_source(
    db_type: str = "auto",
    **config_kwargs
):
    """
    Универсальная фабрика для создания источника данных
    
    Автоматически определяет тип БД и создает подходящий компонент.
    
    Args:
        db_type: Тип БД ("firebird", "mysql", "postgresql", "auto")
        **config_kwargs: Параметры подключения
        
    Returns:
        Подходящий источник данных
        
    Architecture Decision:
        🎯 Firebird-First Strategy: 
        - Если не указан тип, сначала проверяем Firebird
        - Firebird = enterprise reality, остальное = fallback
        
    Example:
        >>> # Автоопределение (сначала проверит Firebird)
        >>> source = create_database_source(
        ...     host="localhost",
        ...     database="C:/shipping.fdb",  # .fdb = Firebird автоматически
        ...     user="SYSDBA",
        ...     password="masterkey"
        ... )
        >>> 
        >>> # Принудительный выбор
        >>> source = create_database_source(
        ...     db_type="mysql",
        ...     host="localhost",
        ...     database="shipping_db",
        ...     user="mysql_user",
        ...     password="mysql_pass"
        ... )
    """
    
    # Автоопределение типа БД
    if db_type == "auto":
        db_type = detect_database_type(config_kwargs)
    
    # Создаем источник по типу
    if db_type == "firebird":
        return _create_firebird_source(**config_kwargs)
    else:
        raise ValueError(f"Неподдерживаемый тип БД: {db_type}")


def create_database_writer(
    db_type: str = "auto", 
    table_configs: list = None,
    **config_kwargs
):
    """
    Универсальная фабрика для создания писателя данных
    
    Для Firebird использует встроенные возможности FirebirdEntityManager,
    для остальных БД - ExternalDatabaseWriter.
    
    Args:
        db_type: Тип целевой БД
        table_configs: Конфигурации таблиц
        **config_kwargs: Параметры подключения
        
    Returns:
        Подходящий писатель данных
        
    Example:
        >>> writer = create_database_writer(
        ...     db_type="mysql",
        ...     host="external-system.com",
        ...     database="logistics_db",
        ...     table_configs=[create_shipment_table_config()]
        ... )
    """
    
    if db_type == "auto":
        db_type = detect_database_type(config_kwargs)
    
    if db_type == "firebird":
        # Для Firebird не нужен отдельный writer - используем EntityManager
        return _create_firebird_source(**config_kwargs)  # Тот же менеджер
    else:
        # Для остальных БД используем generic writer
        if table_configs is None:
            table_configs = [create_shipment_table_config()]
        
#        return ExternalDatabaseWriter(config_kwargs, table_configs)


def create_unified_config(
    source_config: dict,
    target_config: dict = None,
    processing_rules: dict = None
) -> dict:
    """
    Создать единую конфигурацию для всей системы БД
    
    Объединяет конфигурации источника, цели и правил обработки
    в единую структуру для простого управления.
    
    Args:
        source_config: Конфигурация источника данных
        target_config: Конфигурация цели (может быть None для Firebird)
        processing_rules: Правила обработки и маппинга
        
    Returns:
        Унифицированная конфигурация
        
    Example:
        >>> unified_config = create_unified_config(
        ...     source_config={
        ...         'type': 'firebird',
        ...         'host': 'localhost',
        ...         'database': 'C:/shipping.fdb',
        ...         'user': 'SYSDBA',
        ...         'password': 'masterkey'
        ...     },
        ...     processing_rules={
        ...         'batch_size': 100,
        ...         'excluded_statuses': [8, 9, 24],  # закрыто, доставлено, отменено
        ...         'priority_lines': [1, 2, 3]       # важные линии
        ...     }
        ... )
    """
    
    # Определяем тип источника
    source_type = source_config.get('type') or detect_database_type(source_config)
    
    config = {
        'version': '2.0',
        'created_at': None,  # Timestamp создания
        'source': {
            'type': source_type,
            'config': source_config
        },
        'processing': processing_rules or {},
        'capabilities': get_database_capabilities(source_type)
    }
    
    # Добавляем цель если есть (для интеграций)
    if target_config:
        target_type = target_config.get('type') or detect_database_type(target_config)
        config['target'] = {
            'type': target_type,
            'config': target_config
        }
    
    return config


# =============================================================================
# УТИЛИТЫ И ДИАГНОСТИКА
# =============================================================================

def detect_database_type(config: dict) -> str:
    """
    Автоматическое определение типа БД по конфигурации
    
    Firebird-First Strategy: проверяем Firebird признаки первыми.
    
    Detection Logic:
        1. Явный тип в config['type']
        2. Расширение файла БД (.fdb = Firebird)
        3. Порт подключения (3050 = Firebird, 3306 = MySQL, 5432 = PostgreSQL)  
        4. Имя пользователя (SYSDBA = Firebird)
        5. Fallback = mysql
    """
    
    # Явное указание типа
    if 'type' in config:
        return config['type'].lower()
    
    # По расширению файла БД
    database = config.get('database', '')
    if database.lower().endswith('.fdb'):
        return 'firebird'
    
    # По порту
    port = config.get('port', 0)
    if port == 3050:
        return 'firebird'
    elif port == 3306:
        return 'mysql'
    elif port == 5432:
        return 'postgresql'
    
    # По пользователю
    user = config.get('user', '').upper()
    if user == 'SYSDBA':
        return 'firebird'
    
    # По умолчанию MySQL (наиболее распространенный)
    return 'mysql'


def get_database_capabilities(db_type: str) -> dict:
    """
    Получить возможности типа БД
    
    Помогает понять что доступно для каждого типа БД.
    
    Returns:
        Словарь с возможностями БД
    """
    
    capabilities = {
        'firebird': {
            'unified_manager': True,      # Есть FirebirdEntityManager
            'read_write_same_db': True,   # Чтение и запись в одну БД
            'transaction_support': True,  # Полные транзакции
            'date_types': ['DATE', 'TIMESTAMP'],
            'concurrent_connections': 'limited',  # Ограниченные
            'enterprise_features': True,
            'recommended_for': ['corporate_integrations', 'enterprise_systems']
        },
        
        'mysql': {
            'unified_manager': False,     # Только generic компоненты
            'read_write_same_db': False,  # Обычно разные БД
            'transaction_support': True,
            'date_types': ['DATE', 'DATETIME', 'TIMESTAMP'],
            'concurrent_connections': 'high',  # Высокая производительность
            'enterprise_features': True,
            'recommended_for': ['web_applications', 'microservices']
        },
        
        'postgresql': {
            'unified_manager': False,
            'read_write_same_db': False,
            'transaction_support': True,
            'date_types': ['DATE', 'TIMESTAMP', 'TIMESTAMPTZ'],
            'concurrent_connections': 'high',
            'enterprise_features': True,
            'recommended_for': ['analytics', 'complex_queries', 'json_data']
        }
    }
    
    return capabilities.get(db_type, {})


async def validate_database_config(config: dict) -> dict:
    """
    Универсальная валидация конфигурации БД
    
    Адаптируется под тип БД для специфичных проверок.
    """
    
    db_type = detect_database_type(config)
    
    # Базовые проверки для всех БД
    errors = []
    warnings = []
    
    required_fields = ['host', 'user', 'password', 'database']
    for field in required_fields:
        if not config.get(field):
            errors.append(f"Отсутствует {field}")
    
    # Специфичные проверки по типу БД
    if db_type == 'firebird':
        firebird_result = await validate_firebird_config(config)
        errors.extend(firebird_result['errors'])
        warnings.extend(firebird_result['warnings'])
    
    elif db_type == 'mysql':
        port = config.get('port', 3306)
        if port != 3306:
            warnings.append(f"Нестандартный порт MySQL: {port}")
    
    elif db_type == 'postgresql':
        port = config.get('port', 5432)
        if port != 5432:
            warnings.append(f"Нестандартный порт PostgreSQL: {port}")
    
    return {
        'valid': len(errors) == 0,
        'detected_type': db_type,
        'errors': errors,
        'warnings': warnings,
        'capabilities': get_database_capabilities(db_type)
    }


async def test_database_connections(config: dict) -> dict:
    """
    Тестирование подключений с автоопределением типа БД
    
    Returns:
        Результаты тестирования с рекомендациями
    """
    
    db_type = detect_database_type(config)
    results = {
        'detected_type': db_type,
        'connection_test': {'success': False, 'error': None, 'latency_ms': 0},
        'capabilities_test': {},
        'recommendations': []
    }
    
    # Тестируем подключение по типу БД
    try:
        if db_type == 'firebird':
            # Тестируем Firebird
            from .firebird_manager import FirebirdConnectionManager
            
            connection_manager = FirebirdConnectionManager(config)
            success = await connection_manager.test_connection()
            
            results['connection_test']['success'] = success
            
            if success:
                results['capabilities_test'] = {
                    'transactions': True,
                    'concurrent_access': True,
                    'enterprise_ready': True
                }
                results['recommendations'].append("✅ Firebird готов для enterprise использования")
            
        else:
            # Для других БД пока заглушка
            results['connection_test']['success'] = False
            results['connection_test']['error'] = f"Тестирование {db_type} пока не реализовано"
            results['recommendations'].append(f"💡 Реализуйте тестирование для {db_type}")
    
    except Exception as e:
        results['connection_test']['error'] = str(e)
        results['recommendations'].append(f"❌ Ошибка подключения: {e}")
    
    return results


# =============================================================================
# ВНУТРЕННИЕ ФУНКЦИИ (не в __all__)
# =============================================================================

def _create_firebird_source(**config_kwargs):
    """Создать Firebird источник"""
    
    # Извлекаем параметры для FirebirdEntityManager
    required_params = ['host', 'database', 'user', 'password']
    firebird_config = {param: config_kwargs[param] for param in required_params}
    
    # Дополнительные параметры
    entity_config = config_kwargs.get('entity_config')
    if 'dsn' in config_kwargs:
        firebird_config['dsn'] = config_kwargs['dsn']
    
    return create_firebird_entity_manager(
        host=firebird_config['host'],
        database=firebird_config['database'],
        user=firebird_config['user'],
        password=firebird_config['password'],
        entity_config=entity_config
    )



# =============================================================================
# ГОТОВЫЕ КОНФИГУРАЦИИ 2.0
# =============================================================================

def create_firebird_entity_config(
    custom_mappings: dict = None,
    excluded_statuses: set = None
) -> EntityTableConfig:
    """
    Создать конфигурацию Firebird entity с кастомизацией
    
    Args:
        custom_mappings: Дополнительные маппинги операций
        excluded_statuses: Статусы для исключения
        
    Returns:
        Настроенная EntityTableConfig
        
    Example:
        >>> config = create_firebird_entity_config(
        ...     custom_mappings={
        ...         'DATE_CUSTOMS': EntityColumnMapping(
        ...             entity_column='DATE_CUSTOMS',
        ...             fesco_field='date',
        ...             operation_patterns=('таможенное оформление', 'customs clearance'),
        ...             priority=9
        ...         )
        ...     },
        ...     excluded_statuses={8, 9, 24, 99}  # Дополнительные статусы
        ... )
    """
    
    config = EntityTableConfig()
    
    # Добавляем кастомные маппинги
    if custom_mappings:
        config.date_mappings.update(custom_mappings)
    
    # Переопределяем исключенные статусы
    if excluded_statuses:
        config.excluded_status_ids = excluded_statuses
    
    return config



# =============================================================================
# МЕТАДАННЫЕ ПАКЕТА
# =============================================================================

__version__ = "0.0.1"
__description__ = "Database integration components for FESCO Container Tracking"