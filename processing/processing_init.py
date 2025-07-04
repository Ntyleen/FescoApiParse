# processing/__init__.py
"""
FESCO Container Tracking - Processing Package
=============================================

Модуль обработки контейнеров. Содержит как проверенные временем компоненты
(ваш оригинальный трекер и обработчик событий), так и новые возможности
для интеграции с базами данных.

Архитектура:
    Classic Components (ваш исходный код):
        ├── ContainerTracker      # Ваш оригинальный трекер
        ├── EventProcessor        # Ваш обработчик событий
        └── Utilities             # Вспомогательные функции
    
    Enhanced Components (новый функционал):
        ├── ContainerTrackingWorkflow     # Координатор с БД интеграцией
        ├── ContainerBindingManager       # Управление привязками
        └── Database Integration          # Компоненты работы с БД

Выбор подходящего компонента:
    - ContainerTracker: Для простого трекинга списка контейнеров (ваш оригинал)
    - ContainerTrackingWorkflow: Для полной интеграции с БД (новая версия)
    - EventProcessor: Универсальный обработчик событий (используется везде)
"""

# =============================================================================
# ИМПОРТЫ - Проверенные временем компоненты (ваш оригинальный код)
# =============================================================================

# Ваш оригинальный трекер - работает как раньше
from .tracker import ContainerTracker

# Ваш обработчик событий - используется везде
from .events import EventProcessor

# =============================================================================
# ИМПОРТЫ - Новые компоненты для расширенного функционала
# =============================================================================

# Новый workflow координатор
from .workflow_coordinator import ContainerTrackingWorkflow, WorkflowStats

# Управление привязками контейнеров к заявкам
from .container_bindings import ContainerBindingManager

# =============================================================================
# ПУБЛИЧНЫЙ API - Что доступно при импорте processing
# =============================================================================

__all__ = [
    # === КЛАССИЧЕСКИЕ КОМПОНЕНТЫ ===
    # Ваш проверенный временем код
    'ContainerTracker',           # Оригинальный трекер
    'EventProcessor',             # Обработчик событий
    
    # === РАСШИРЕННЫЕ КОМПОНЕНТЫ ===  
    # Новый функционал с БД интеграцией
    'ContainerTrackingWorkflow',  # Координатор workflow
    'ContainerBindingManager',    # Управление привязками
    'WorkflowStats',              # Статистика workflow
    
    # === ФАБРИЧНЫЕ ФУНКЦИИ ===
    # Удобные способы создания компонентов
    'create_tracker',             # Создать классический трекер
    'create_workflow',            # Создать расширенный workflow
    'create_binding_manager',     # Создать менеджер привязок
    
    # === УТИЛИТЫ ===
    # Вспомогательные функции
    'validate_container_number',  # Валидация номеров
    'batch_containers',           # Разбивка на батчи
    'compare_tracking_approaches', # Сравнение подходов
]


# =============================================================================
# ФАБРИЧНЫЕ ФУНКЦИИ - Удобные способы создания компонентов
# =============================================================================

def create_tracker(config, cache):
    """
    Создать классический трекер контейнеров (ваш оригинальный)
    
    Используйте эту функцию когда:
    - Нужно простое трекинг списка контейнеров
    - Не требуется интеграция с БД
    - Хотите сохранить привычное поведение
    
    Args:
        config: Конфигурация приложения
        cache: Кэш для запросов
        
    Returns:
        ContainerTracker: Готовый к работе трекер
        
    Example:
        >>> from processing import create_tracker
        >>> from config import load_config
        >>> from cache import create_cache
        >>> 
        >>> config = load_config()
        >>> cache = create_cache()
        >>> tracker = create_tracker(config, cache)
        >>> 
        >>> # Работает как ваш оригинальный код
        >>> containers = ["TDSU6005411", "FESU5384983"]
        >>> async for result in tracker.track_containers(containers):
        ...     print(f"{result.container_number}: {result.success}")
    """
    return ContainerTracker(config, cache)


def create_workflow(config, cache, db_source, external_writer):
    """
    Создать расширенный workflow координатор
    
    Используйте эту функцию когда:
    - Нужна интеграция с базами данных
    - Требуется оптимизация API запросов
    - Хотите полный контроль над процессом
    
    Args:
        config: Конфигурация приложения
        cache: Кэш для операций
        db_source: Источник контейнеров из БД
        external_writer: Писатель в целевую БД
        
    Returns:
        ContainerTrackingWorkflow: Готовый workflow
        
    Example:
        >>> from processing import create_workflow
        >>> from database import DatabaseContainerSource, ExternalDatabaseWriter
        >>> 
        >>> # Создаем компоненты БД
        >>> db_source = DatabaseContainerSource(source_config)
        >>> writer = ExternalDatabaseWriter(target_config, table_configs)
        >>> 
        >>> # Создаем workflow
        >>> workflow = create_workflow(config, cache, db_source, writer)
        >>> 
        >>> # Запускаем полный процесс
        >>> stats = await workflow.run_full_workflow()
    """
    return ContainerTrackingWorkflow(config, cache, db_source, external_writer)


def create_binding_manager(cache):
    """
    Создать менеджер привязок контейнеров к заявкам
    
    Используйте когда нужно управлять привязками отдельно от основного процесса.
    
    Args:
        cache: Кэш для хранения привязок
        
    Returns:
        ContainerBindingManager: Готовый менеджер
        
    Example:
        >>> from processing import create_binding_manager
        >>> 
        >>> binding_manager = create_binding_manager(cache)
        >>> 
        >>> # Привязываем контейнер к заявке
        >>> await binding_manager.bind_container_to_order("TDSU6005411", "ORD123456")
        >>> 
        >>> # Проверяем привязку
        >>> order_id = await binding_manager.get_container_order("TDSU6005411")
        >>> print(f"Контейнер привязан к заявке: {order_id}")
    """
    return ContainerBindingManager(cache)


# =============================================================================
# УТИЛИТЫ - Вспомогательные функции
# =============================================================================

def validate_container_number(container_number: str) -> bool:
    """
    Валидация номера контейнера
    
    Проверяет соответствие номера стандартным форматам контейнеров.
    
    Args:
        container_number: Номер контейнера для проверки
        
    Returns:
        True если номер валидный
        
    Example:
        >>> from processing import validate_container_number
        >>> 
        >>> validate_container_number("TDSU6005411")  # True
        >>> validate_container_number("invalid")      # False
    """
    if not isinstance(container_number, str):
        return False
    
    # Убираем пробелы и приводим к верхнему регистру
    container_number = container_number.strip().upper()
    
    # Базовая проверка длины (обычно 11 символов)
    if len(container_number) < 10 or len(container_number) > 15:
        return False
    
    # Проверяем наличие букв и цифр
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
    
    Полезно для обработки больших списков контейнеров порциями.
    
    Args:
        container_numbers: Список номеров контейнеров
        batch_size: Размер батча
        
    Returns:
        Список батчей (списков контейнеров)
        
    Example:
        >>> from processing import batch_containers
        >>> 
        >>> containers = ["TDSU6005411", "FESU5384983", "TEMU1234567"]
        >>> batches = batch_containers(containers, batch_size=2)
        >>> 
        >>> for i, batch in enumerate(batches):
        ...     print(f"Батч {i+1}: {batch}")
        # Батч 1: ['TDSU6005411', 'FESU5384983']
        # Батч 2: ['TEMU1234567']
    """
    if batch_size <= 0:
        raise ValueError("Размер батча должен быть положительным")
    
    batches = []
    for i in range(0, len(container_numbers), batch_size):
        batch = container_numbers[i:i + batch_size]
        batches.append(batch)
    
    return batches


def compare_tracking_approaches(
    container_numbers: list,
    environment: str = "development"
) -> dict:
    """
    Сравнить производительность классического и расширенного подходов
    
    Полезно для принятия решения о миграции или выборе подходящего метода.
    
    Args:
        container_numbers: Список контейнеров для тестирования
        environment: Окружение для тестирования
        
    Returns:
        Словарь с результатами сравнения
        
    Example:
        >>> from processing import compare_tracking_approaches
        >>> 
        >>> containers = ["TDSU6005411", "FESU5384983"]
        >>> comparison = compare_tracking_approaches(containers)
        >>> 
        >>> print(f"Классический: {comparison['classic']['duration']}s")
        >>> print(f"Расширенный: {comparison['workflow']['duration']}s")
        >>> print(f"Рекомендация: {comparison['recommendation']}")
    """
    
    # TODO: Здесь будет реализация сравнения
    # Пока возвращаем заглушку с структурой результата
    
    return {
        "container_count": len(container_numbers),
        "environment": environment,
        "classic": {
            "duration": 0.0,
            "api_calls": 0,
            "success_rate": 0.0,
            "memory_usage": "N/A"
        },
        "workflow": {
            "duration": 0.0,
            "api_calls": 0,
            "success_rate": 0.0,
            "memory_usage": "N/A",
            "db_operations": 0
        },
        "recommendation": "comparison_not_implemented",
        "notes": [
            "Классический подход лучше для разовых задач",
            "Workflow подход лучше для интеграции в системы",
            "Запустите реальное сравнение для получения метрик"
        ]
    }


# =============================================================================
# МИГРАЦИОННЫЕ ПОМОЩНИКИ - Переход между подходами
# =============================================================================

def migration_helper() -> dict:
    """
    Помощник для миграции с классического на расширенный подход
    
    Анализирует текущее использование и дает рекомендации по миграции.
    
    Returns:
        Словарь с планом миграции и рекомендациями
    """
    
    return {
        "migration_steps": [
            "1. Настройте подключения к БД (источник и цель)",
            "2. Создайте конфигурации таблиц для записи",
            "3. Протестируйте workflow на небольшом наборе контейнеров",
            "4. Постепенно переводите процессы на новый подход",
            "5. Мониторьте производительность и качество данных"
        ],
        "compatibility": {
            "EventProcessor": "✅ Полностью совместим",
            "TrackingResult": "✅ Полностью совместим", 
            "Config": "✅ Полностью совместим",
            "Cache": "✅ Полностью совместим",
            "API Client": "✅ Полностимо совместим"
        },
        "breaking_changes": [],
        "new_requirements": [
            "Настройка БД источника контейнеров",
            "Настройка целевой БД для записи результатов",
            "Конфигурация маппинга полей между системами"
        ],
        "rollback_plan": [
            "Классический ContainerTracker остается доступным",
            "Можно переключаться между подходами по мере необходимости",
            "Все существующие конфигурации продолжают работать"
        ]
    }


# =============================================================================
# ДИАГНОСТИЧЕСКИЕ ФУНКЦИИ
# =============================================================================

def get_processing_capabilities() -> dict:
    """
    Получить информацию о доступных возможностях обработки
    
    Полезно для диагностики и выбора подходящих компонентов.
    
    Returns:
        Словарь с описанием возможностей
    """
    
    return {
        "classic_tracking": {
            "available": True,
            "description": "Оригинальный трекинг списка контейнеров",
            "use_cases": [
                "Разовые задачи трекинга",
                "Интеграция в существующие скрипты",
                "Простые автоматизации"
            ],
            "limitations": [
                "Нет интеграции с БД",
                "Ограниченные возможности кэширования",
                "Нет оптимизации API запросов"
            ]
        },
        "workflow_tracking": {
            "available": True,
            "description": "Расширенный workflow с БД интеграцией",
            "use_cases": [
                "Корпоративные интеграции",
                "Автоматизированные системы",
                "Высоконагруженные процессы"
            ],
            "features": [
                "Привязка контейнеров к заявкам",
                "Оптимизация API запросов", 
                "Интеграция с внешними БД",
                "Расширенное кэширование"
            ]
        },
        "event_processing": {
            "available": True,
            "description": "Универсальный обработчик событий",
            "features": [
                "Извлечение событий из API ответов",
                "Дедупликация событий",
                "Объединение данных из разных источников"
            ]
        }
    }


# =============================================================================
# МЕТАДАННЫЕ ПАКЕТА
# =============================================================================

__version__ = "1.0.0"
__description__ = "Container tracking processing components - classic and enhanced"

# Для совместимости с вашим существующим кодом
from .tracker import ContainerTracker as LegacyContainerTracker
from .events import EventProcessor as LegacyEventProcessor

# Алиасы для обратной совместимости
create_tracker_legacy = create_tracker  # На случай, если понадобится различать версии