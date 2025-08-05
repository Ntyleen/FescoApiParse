# database/firebird_entity_manager.py
"""
Единый модуль для работы с Firebird entity таблицей
Объединяет чтение контейнеров и обновление результатов трекинга

Архитектурное решение:
    FirebirdEntityManager - единая точка работы с entity таблицей
    ├── Чтение контейнеров для обработки
    ├── Обновление дат после трекинга  
    ├── Управление статусами и фильтрацией
    └── Статистика и мониторинг
"""

import asyncio
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Any, Set, AsyncGenerator, Tuple
from dataclasses import dataclass, field
from datetime import datetime, date
from enum import IntEnum
from contextlib import contextmanager, asynccontextmanager
import re
from models.container_event import TrackingResult
from utils.logging import get_logger

try:
    import firebird.driver as fdb  # pip install firebird-driver
    FIREBIRD_AVAILABLE = True
except ImportError:
    FIREBIRD_AVAILABLE = False
    fdb = None


# =============================================================================
# КОНФИГУРАЦИОННЫЕ МОДЕЛИ - твоя идея с dataclasses!
# =============================================================================

class EntityStatusID(IntEnum):
    """
    Важные статусы entity как Enum - типобезопасно и читаемо
    Основано на твоем JSON файле tables
    """
    NEW = 1 # "00. Новый"
    LOCATION_RECEIVED = 2 # "01. Место получено"
    SEA = 3 # "03. Море"
    ARRIVED_AT_STATION = 4 # "07. Прибыл на станцию"
    TERMINAL_OPERATION = 5 # "04. ТО"
    WAITING_DEPARTURE = 6 # "02. Жду выход"
    RAILWAY = 7 # "06. ЖД"
    TRANSPORTATION_CLOSED = 8 # "09. Перевозка закрыта"
    DELIVERED = 9 # "08. Доставлен"
    DOCUMENTS_RECEIVED = 12 # "08..Документы получены"
    ATTENTION = 13 # "08...Внимание"
    WAITING_SHIPMENT = 15 # "05. Ожидает отгрузку"
    RAID = 16 # "03. Рейд"
    DIRECT_CAR = 17 # "03. Прямое авто"
    RAIL = 23 #  "03. RAIL"
    CANCELLED = 24  # "99. Отмена"
    UNLOADING = 25 # "03..Выгрузка"
    CLIENT_DEBT = 26 # "08..Долг Клиента"
    OUTPUT = 27 # "04..Выпуск"
    LCL_RU = 28 # "06. LCL по РФ"
    NO_AVP = 29 # "08..нет АВП"
    RAILWAY_OUTPUT = 30 # "07..Выпуск ЖД"
    PP_DIRECT_RAILWAY = 31 # "05.. П\/П прямое ЖД"
    LOCAL_ISSUANCE = 32 # "04..Выпуск МВ"
    AWAITS_AVAILABILITY = 33 # "00.1 Ожидает готовности"
    LOOKING_PLACE = 34 # "00.2 Ищем место"
    WAITING_PLACE = 35 # "00.3 Ждем место"
    TRANSHIPMET = 36 # "03.Трансшипмент"
    GIVEN_FOR_CLOSE = 37 # "08..Передана на закрытие"
    SEA_THROUGH = 41 # "03. Море \/ СКВОЗНОЙ СЕРВИС"
    RAID_THROUGH = 42 # "03. Рейд\/ СКВОЗНОЙ СЕРВИС"
    UNLOADING_THROUGH = 43 # "03..Выгрузка\/ СКВОЗНОЙ СЕРВИС"
    TERMINAL_OPERATION_THROUGH = 44 # "04. ТО\/ СКВОЗНОЙ СЕРВИС"
    RELEASE_THROUGH = 45 # "04..Выпуск\/ СКВОЗНОЙ СЕРВИС"
    WAITING_SHIPMENT_THROUGH = 46 # "05. Ожидает отгрузку\/ СКВОЗНОЙ СЕРВИС"
    WAITING_EMPTY = 52 # "08..Ожидаем сдачу порожнего"
    AUTO_CN = 55 # "03. Авто Китай"
    AUTO_BORDER_CROSSING = 56 # "03..Переход границы (авто)"
    TERMINAL_OPERATION_AUTO = 57 # "ТО (прямое авто)"
    FTL_RELEASE = 58 # "04...Выпуск FTL"

    
    @classmethod
    def get_excluded_statuses(cls) -> Set[int]:
        """Статусы, которые исключаем из обработки"""
        return {
            int(cls.TRANSPORTATION_CLOSED),  # 8 - "09. Перевозка закрыта"
            int(cls.DELIVERED),              # 9 - "08. Доставлен" 
           int(cls.CANCELLED)               # 24 - "99. Отмена"
        }


@dataclass(frozen=True)
class EntityColumnMapping:
    """
    Маппинг колонки entity на операцию FESCO
    Использует твою концепцию ColumnMapping, но адаптированную под entity
    """
    entity_column: str                  # Колонка в entity (например, DATE_ETA)
    fesco_field: str                    # Поле из FESCO API
    operation_patterns: Tuple[str, ...] # Паттерны операций для этой колонки
    transform_func: str = "firebird_date"
    priority: int = 0                   # Приоритет при совпадении нескольких паттернов
    description: str = ""               # Описание для документации
    column_datatype: str = "DATE"       # Тип данных колонки: "DATE", "TIMESTAMP", "INTEGER"
    

    def __post_init__(self):
        if not self.operation_patterns:
            raise ValueError("operation_patterns должен cодержать хотя бы один паттерн")

    def get_transform_datatype(self) -> str:
        """Определяет стратегию трансформации на основе типа колонки"""
        if self.column_datatype == "TIMESTAMP":
            return "firebird_timestamp"
        elif self.column_datatype == "DATE":
            return "firebird_date_only"
        elif self.column_datatype == "INTEGER":
            return "firebird_integer"
        else:
            return "firebird_date"  # fallback

    def matches_operation(self, operation: str) -> float:
        """
        Проверяет соответствие операции маппингу и возвращает оценку
        
        Returns:
            float: Оценка от 0.0 до 1.0 (нормализованная)
        """
        if not operation:
            return 0.0
            
        operation_lower = operation.lower().strip()
        max_score = 0.0
        
        for pattern in self.operation_patterns:
            pattern_lower = pattern.lower().strip()
            
            # Точное совпадение
            if pattern_lower == operation_lower:
                max_score = max(max_score, 1.0)
            # Операция содержит паттерн
            elif pattern_lower in operation_lower:
                coverage = len(pattern_lower) / len(operation_lower)
                position_bonus = 0.2 if operation_lower.startswith(pattern_lower) else 0.1
                score = 0.8 * coverage + position_bonus
                max_score = max(max_score, score)
            # Паттерн содержит операцию  
            elif operation_lower in pattern_lower:
                score = 0.6 * len(operation_lower) / len(pattern_lower)
                max_score = max(max_score, score)
        
        # Нормализуем с учетом приоритета (но не превышаем 1.0)
        priority_bonus = min(0.1, self.priority / 100)
        return min(1.0, max_score + priority_bonus)


@dataclass
class EntityTableConfig:
    """
    Конфигурация entity таблицы
    """
    # Основные колонки таблицы
    table_name: str = "ENTITY"  # Firebird обычно в верхнем регистре
    primary_key: str = "ID"
    container_column: str = "NAME"
    status_column: str = "SP_ENTITY_STATUS_ID"
    line_column: str = "LEGAL_PERSON_LINE_ID"
    railway_carrier_column: str = "LEGAL_PERSON_RAILWAY_CARRIER_ID"

    # Колонки дат
    date_eta: str = "DATE_ETA"
    date_etd: str = "DATE_ETD"
    date_in: str = "DATE_IN"
    date_railway_loading: str = "DATE_RAILWAY_LOADING"
    date_railway_delivery: str = "DATE_RAILWAY_DELIVERY"
    remaining_distance: str = "TRACING_DAYS"
    
    # Маппинги операций FESCO на колонки дат
    date_mappings: Dict[str, EntityColumnMapping] = field(default_factory=dict)
    
    # Статусы для исключения из обработки
    excluded_status_ids: Set[int] = field(default_factory=lambda: EntityStatusID.get_excluded_statuses())
    
    def __post_init__(self):
        """Создаем маппинги по умолчанию если не указаны"""
        if not self.date_mappings:
            self.date_mappings = self._create_default_mappings()
        self._validate_config()

        self.excluded_status_ids = {int(v) for v in self.excluded_status_ids}

    def _validate_config(self):
        """Валидация конфигурации"""
        if not self.table_name or not self.table_name.strip():
            raise ValueError("table_name не может быть пустым")
        
        required_columns = [
            self.primary_key, self.container_column, 
            self.status_column, self.line_column,
            self.railway_carrier_column,
        ]
        
        for column in required_columns:
            if not column or not column.strip():
                raise ValueError(f"Обязательная колонка не может быть пустой: {column}")
    
    def _create_default_mappings(self) -> Dict[str, EntityColumnMapping]:
        """
        Создает маппинги операций FESCO на колонки
        """
        return {
            self.date_eta: EntityColumnMapping(
                entity_column=self.date_eta,
                fesco_field="date",
                operation_patterns=["Выгружается груженным"], # type: ignore
                priority=10,
                description="Estimated Time of Arrival",
                column_datatype="DATE"
            ),
            
            self.date_etd: EntityColumnMapping(
                entity_column=self.date_etd,
                fesco_field="date",
                operation_patterns=["Грузится на фидер", "Loading Feeder Full"], # type: ignore
                priority=10,
                description="Estimated Time of Departure",
                column_datatype="DATE"
            ),
            
            self.date_in: EntityColumnMapping(
                entity_column=self.date_in,
                fesco_field="date",
                operation_patterns=["Прием с моря", "Регистрация ДО1", "Discharged from vessel", "DO1 registration"], # type: ignore
                priority=8,
                description="Выгрузка на терминал",
                column_datatype="TIMESTAMP"
            ),
            
            self.date_railway_loading: EntityColumnMapping(
                entity_column=self.date_railway_loading,
                fesco_field="date",
                operation_patterns=["Отправление вагона со станции", "Wagon has left the station"], # type: ignore
                priority=8,
                description="Отгрузка на платформу",
                column_datatype="DATE"
            ),
            
            self.date_railway_delivery: EntityColumnMapping(
                entity_column=self.date_railway_delivery,
                fesco_field="date",
                operation_patterns=["Документы для отправки по ЖД приняты", "Documents for sending by railway accepted"], # type: ignore
                priority=8,
                description="Сдача на ж/д",
                column_datatype="DATE"
            ),

            self.remaining_distance: EntityColumnMapping(
                entity_column=self.remaining_distance,
                fesco_field="remainingDistance",
                operation_patterns=("",),
                priority=10,
                description="Слежение",
                column_datatype="INTEGER"
            )
        }


@dataclass
class ContainerInfo:
    """
    Информация о контейнере из entity
    """
    id: int
    container_number: str
    status_id: Optional[int] = None
    status_name: str = ""
    line_id: Optional[int] = None
    railway_carrier_id: Optional[int] = None
    priority: int = 0
    remaining_distance: Optional[int] = None
    # created_at: Optional[str] = None
    # updated_at: Optional[str] = None
    
    # Текущие значения дат (для отслеживания изменений)
    current_dates: Dict[str, Optional[str]] = field(default_factory=dict)
    
    # Флаги обработки
    processing_flags: Dict[str, bool] = field(default_factory=dict)


# =============================================================================
# ГЛАВНЫЙ КЛАСС - ЕДИНЫЙ МЕНЕДЖЕР ENTITY
# =============================================================================

class FirebirdConnectionManager:
    
    def __init__(self, firebird_config: dict, entity_config: EntityTableConfig = None):
        if not FIREBIRD_AVAILABLE:
            raise ImportError(
                "❌ Firebird драйвер не установлен!\n"
                "Установите: pip install fdb\n"
                "или: pip install firebird-driver"
            )
        
        self.config = firebird_config
        self.logger = get_logger("firebird.connection")
        self._validate_config()
        
        # Thread-safe счетчик соединений
        self._connection_lock = threading.Lock()
        self._active_connections = 0
        self._max_connections = 10
    
    def _validate_config(self):
        """Валидация конфигурации подключения"""
        required_fields = ['host', 'database', 'user', 'password']
        for field in required_fields:
            if not self.config.get(field):
                raise ValueError(f"Отсутствует обязательное поле: {field}")
    
    @contextmanager
    def get_connection(self):
        """
        Context manager для получения подключения
        
        Гарантирует правильное закрытие и учет активных соединений
        """
        connection = None
        
        # Проверяем лимит соединений
        with self._connection_lock:
            if self._active_connections >= self._max_connections:
                raise RuntimeError(f"Превышен лимит соединений: {self._max_connections}")
            self._active_connections += 1
        
        try:
            connection = self._create_connection()
            yield connection
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка работы с подключением: {e}")
            raise
            
        finally:
            # Всегда освобождаем ресурсы
            if connection:
                try:
                    connection.close()
                except Exception as close_error:
                    self.logger.error(f"Ошибка закрытия соединения: {close_error}")
            
            with self._connection_lock:
                self._active_connections -= 1
    
    def _create_connection(self):
        """Создание нового подключения к Firebird"""
        try:
            if 'dsn' in self.config:
                dsn = self.config['dsn']
            else:
                host = self.config['host']
                database = self.config['database']
                dsn = f"{host}:{database}"
            
            connection = fdb.connect(
                database=self.config['database'],
                user=self.config['user'],
                password=self.config['password'],
                charset='UTF8'
            )
            
            return connection
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка создания Firebird подключения: {e}")
            raise
    
    async def test_connection(self) -> bool:
        """Тестирование подключения"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1 FROM RDB$DATABASE")
                result = cursor.fetchone()
                return result is not None
                
        except Exception as e:
            self.logger.error(f"❌ Ошибка тестирования подключения: {e}")
            return False

class FirebirdDateTransformer:
    """
    Трансформация дат из FESCO в форматы Firebird
    
    Отвечает ТОЛЬКО за преобразование типов данных
    """
    
    def __init__(self):
        self.logger = get_logger("firebird.transformer")
        
        # Предкомпилированные regex для производительности
        self._number_pattern = re.compile(r'\d+')
        
        # Поддерживаемые форматы дат FESCO
        self._timestamp_formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
            "%d.%m.%Y %H:%M:%S",
            "%d.%m.%Y %H:%M",
        ]
        
        self._date_formats = [
            "%Y-%m-%d",
            "%d.%m.%Y",
            "%Y/%m/%d",
            "%d/%m/%Y"
        ]
    
    def transform_value(self, value: Any, target_type: str) -> Optional[Any]:
        """
        Универсальный метод трансформации значений
        
        Args:
            value: Исходное значение от FESCO
            target_type: Целевой тип ("DATE", "TIMESTAMP", "INTEGER")
            
        Returns:
            Преобразованное значение или None при ошибке
        """
        if value is None:
            return None
        
        try:
            if target_type == "TIMESTAMP":
                return self._transform_to_timestamp(value)
            elif target_type == "DATE":
                return self._transform_to_date(value)
            elif target_type == "INTEGER":
                return self._transform_to_integer(value)
            else:
                self.logger.warning(f"Неизвестный тип: {target_type}")
                return str(value)
                
        except Exception as e:
            self.logger.warning(f"Ошибка трансформации '{value}' в {target_type}: {e}")
            return None
    
    def _transform_to_timestamp(self, date_str: str) -> Optional[datetime]:
        """Трансформация в TIMESTAMP (дата + время)"""
        if not date_str or not str(date_str).strip():
            return None
        
        cleaned = str(date_str).strip()
        
        # Пробуем форматы с временем
        for fmt in self._timestamp_formats:
            try:
                return datetime.strptime(cleaned, fmt)
            except ValueError:
                continue
        
        # Если время не указано, пробуем форматы даты + добавляем время 00:00:00
        for fmt in self._date_formats:
            try:
                date_only = datetime.strptime(cleaned, fmt)
                return date_only  # datetime автоматически добавит 00:00:00
            except ValueError:
                continue
        
        return None
    
    def _transform_to_date(self, date_str: str) -> Optional[date]:
        """Трансформация в DATE (только дата)"""
        timestamp = self._transform_to_timestamp(date_str)
        return timestamp.date() if timestamp else None
    
    def _transform_to_integer(self, value_str: str) -> Optional[int]:
        """Трансформация в INTEGER (извлечение числа)"""
        if not value_str:
            return None
        
        # Извлекаем первое число из строки
        numbers = self._number_pattern.findall(str(value_str))
        if numbers:
            try:
                return int(numbers[0])
            except ValueError:
                pass
        
        return None


class FirebirdOperationMatcher:
    """
    Сопоставление операций FESCO с колонками entity
    
    Отвечает ТОЛЬКО за логику маппинга операций
    """
    
    def __init__(self, entity_config: EntityTableConfig):
        self.entity_config = entity_config
        self.logger = get_logger("firebird.matcher")
        # By default we operate on line mappings. Can be switched to
        # railway-specific mappings via ``set_railway_mode``.
        self._railway_mode = False

    def set_railway_mode(self, enabled: bool) -> None:
        """Switch between line and railway mapping sets.

        Args:
            enabled: ``True`` to use railway mappings, ``False`` for line mappings
        """
        self._railway_mode = enabled
    
    def find_best_mapping(self, operation: str) -> Optional[EntityColumnMapping]:
        """
        Найти лучший маппинг для операции FESCO
        
        Returns:
            Лучший маппинг или None если не найден
        """
        if not operation or not operation.strip():
            return None
        
        operation_clean = operation.strip()
        self.logger.debug(f"🎯 Анализируем операцию: '{operation_clean}'")
        
        # Собираем все маппинги с оценками
        scored_mappings = []

        mappings = self.entity_config.date_mappings

        if self._railway_mode:
            allowed = {
                self.entity_config.date_railway_loading,
                self.entity_config.date_railway_delivery,
                self.entity_config.remaining_distance,
            }
            mappings = {k: v for k, v in mappings.items() if k in allowed}

        for column_name, mapping in mappings.items():
            score = mapping.matches_operation(operation_clean)
            
            if score > 0:
                scored_mappings.append((mapping, score))
                self.logger.debug(f"  📊 {column_name}: score={score:.3f}")
        
        if not scored_mappings:
            self.logger.debug("  🤷 Подходящих маппингов не найдено")
            return None
        
        # Сортируем по оценке (лучшие первыми)
        scored_mappings.sort(key=lambda x: x[1], reverse=True)
        
        best_mapping, best_score = scored_mappings[0]
        
        # Проверяем минимальный порог качества
        if best_score < 0.3:  # Настраиваемый порог
            self.logger.debug(f"🚫 Лучшая оценка {best_score:.3f} ниже порога 0.3")
            return None
        
        self.logger.debug(f"🎯 Выбран маппинг: {best_mapping.entity_column} (score: {best_score:.3f})")
        return best_mapping
    
    def suggest_new_mappings(self, unmapped_operations: List[str]) -> Dict[str, List[str]]:
        """
        Предложить новые маппинги для неопознанных операций
        
        Returns:
            Словарь предложений по улучшению маппингов
        """
        suggestions = {}
        
        for operation in unmapped_operations:
            # Ищем частичные совпадения с существующими маппингами
            for column_name, mapping in self.entity_config.date_mappings.items():
                similarity = self._calculate_similarity(operation, mapping.operation_patterns)
                
                if 0.1 < similarity < 0.3:  # Похоже, но не проходит порог
                    if column_name not in suggestions:
                        suggestions[column_name] = []
                    
                    suggestions[column_name].append(
                        f"Рассмотреть добавление паттерна для '{operation}' "
                        f"(схожесть: {similarity:.2f})"
                    )
        
        return suggestions
    
    def _calculate_similarity(self, operation: str, patterns: Tuple[str, ...]) -> float:
        """Вычислить схожесть операции с паттернами"""
        if not patterns:
            return 0.0
        
        operation_words = set(operation.lower().split())
        max_similarity = 0.0
        
        for pattern in patterns:
            pattern_words = set(pattern.lower().split())
            
            if operation_words and pattern_words:
                intersection = operation_words.intersection(pattern_words)
                union = operation_words.union(pattern_words)
                
                if union:
                    similarity = len(intersection) / len(union)
                    max_similarity = max(max_similarity, similarity)
        
        return max_similarity


class FirebirdStatisticsCollector:
    """
    Сбор и анализ статистики работы
    
    Отвечает ТОЛЬКО за метрики и аналитику
    """
    
    def __init__(self):
        self.stats = {
            # Статистика чтения
            'containers_loaded': 0,
            'containers_filtered': 0,
            'batches_processed': 0,
            
            # Статистика записи (из твоего ExternalDatabaseWriter)
            'records_updated': 0,
            'records_failed': 0,
            'date_columns_updated': {},
            'operations_processed': {},
            
            # Общая статистика
            'connections_created': 0,
            'transactions_committed': 0,
            'transactions_rollbacked': 0,
            'operation_times': [],
        }
        self.logger = get_logger("firebird.stats")
    
    def record_container_loaded(self, count: int = 1):
        """Зафиксировать загрузку контейнеров"""
        self.stats['containers_loaded'] += count
    
    def record_container_filtered(self, count: int = 1):
        """Зафиксировать фильтрацию контейнеров"""
        self.stats['containers_filtered'] += count
    
    def record_batch_processed(self):
        """Зафиксировать обработку батча"""
        self.stats['batches_processed'] += 1
    
    def record_update_success(self, column_name: str, operation: str):
        """Зафиксировать успешное обновление"""
        self.stats['records_updated'] += 1
        
        # Статистика по колонкам
        if column_name not in self.stats['date_columns_updated']:
            self.stats['date_columns_updated'][column_name] = 0
        self.stats['date_columns_updated'][column_name] += 1
        
        # Статистика по операциям (ограничиваем длину)
        operation_key = operation[:50] if operation else "EMPTY_OPERATION"
        if operation_key not in self.stats['operations_processed']:
            self.stats['operations_processed'][operation_key] = 0
        self.stats['operations_processed'][operation_key] += 1
    
    def record_update_failure(self):
        """Зафиксировать неудачное обновление"""
        self.stats['records_failed'] += 1
    
    def record_operation_time(self, time_ms: float):
        """Зафиксировать время выполнения операции"""
        # Храним только последние 1000 операций для расчета среднего
        self.stats['operation_times'].append(time_ms)
        if len(self.stats['operation_times']) > 1000:
            self.stats['operation_times'] = self.stats['operation_times'][-1000:]
    
    def get_summary(self) -> Dict[str, Any]:
        """Получить сводную статистику"""
        operation_times = self.stats['operation_times']
        
        summary = {
            'totals': {
                'containers_loaded': self.stats['containers_loaded'],
                'containers_filtered': self.stats['containers_filtered'],
                'batches_processed': self.stats['batches_processed'],
                'records_updated': self.stats['records_updated'],
                'records_failed': self.stats['records_failed'],
            },
            'success_rate': self._calculate_success_rate(),
            'performance': {
                'avg_operation_time_ms': sum(operation_times) / len(operation_times) if operation_times else 0,
                'min_operation_time_ms': min(operation_times) if operation_times else 0,
                'max_operation_time_ms': max(operation_times) if operation_times else 0,
            },
            'top_columns': self._get_top_columns(),
            'top_operations': self._get_top_operations()
        }
        
        return summary
    
    def _calculate_success_rate(self) -> float:
        """Вычислить процент успешных операций"""
        total = self.stats['records_updated'] + self.stats['records_failed']
        return (self.stats['records_updated'] / total * 100) if total > 0 else 0.0
    
    def _get_top_columns(self, limit: int = 5) -> List[Tuple[str, int]]:
        """Получить топ обновляемых колонок"""
        items = list(self.stats['date_columns_updated'].items())
        items.sort(key=lambda x: x[1], reverse=True)
        return items[:limit]
    
    def _get_top_operations(self, limit: int = 5) -> List[Tuple[str, int]]:
        """Получить топ операций"""
        items = list(self.stats['operations_processed'].items())
        items.sort(key=lambda x: x[1], reverse=True)
        return items[:limit]


# =============================================================================
# ГЛАВНЫЙ КЛАСС - УПРОЩЕННЫЙ И НАДЕЖНЫЙ
# =============================================================================

class FirebirdEntityManager:
    """
    Главный менеджер для работы с Firebird entity таблицей
    """
    
    def __init__(self, firebird_config: dict, entity_config: Optional[EntityTableConfig] = None):
        self.entity_config = entity_config or EntityTableConfig()
        self.logger = get_logger("firebird.entity_manager")
        
        # Компоненты (композиция вместо монолита)
        self.connection_manager = FirebirdConnectionManager(firebird_config)
        self.transformer = FirebirdDateTransformer()
        self.operation_matcher = FirebirdOperationMatcher(self.entity_config)
        self.stats = FirebirdStatisticsCollector()
        
        # Thread pool для async операций (правильное управление ресурсами)
        self._thread_pool = None
        self._max_workers = min(8, (os.cpu_count() or 1) + 4)
        
        self.logger.info(f"🔥 FirebirdEntityManager инициализирован для {self.entity_config.table_name}")
        self.logger.debug(f"🔧 Исключенные статусы: {self.entity_config.excluded_status_ids}")

    @asynccontextmanager
    async def _get_thread_pool(self):
        """Context manager для thread pool"""
        if self._thread_pool is None:
            self._thread_pool = ThreadPoolExecutor(
                max_workers=self._max_workers,
                thread_name_prefix="firebird_entity_"
            )
        
        try:
            yield self._thread_pool
        finally:
            # Thread pool закрывается в close()
            pass

    def _init_worker_thread(self):
        """Инициализация worker thread (избегаем проблем с connection sharing)"""
        # Firebird connections не thread-safe, поэтому каждый thread создает свой
        pass
    
    # =========================================================================
    # ЧТЕНИЕ КОНТЕЙНЕРОВ
    # =========================================================================
    
    async def get_containers_for_processing(
        self,
        batch_size: int = 100,
        target_line_ids: Optional[Set[int]] = None,
        min_priority: int = 0
    ) -> AsyncGenerator[List[ContainerInfo], None]:
        """
        Получить контейнеры для обработки из entity таблицы
        
        Args:
            batch_size: Размер батча для обработки
            target_line_ids: ID линий для фильтрации (если None - все линии)  
            min_priority: Минимальный приоритет для фильтрации
            
        Yields:
            Списки ContainerInfo объектов
        """
        
        def _load_containers():
            """Синхронная загрузка в отдельном потоке"""
            try:
                with self.connection_manager.get_connection() as connection:
                    cursor = connection.cursor()
                    
                    query, params = self._build_selection_query(target_line_ids, min_priority)
                    
                    self.logger.debug(f"🔥 SQL: {query}")
                    self.logger.debug(f"🔥 Params: {params}")
                    
                    cursor.execute(query, params)
                    rows = cursor.fetchall()
                    
                    self.logger.info(f"🔥 Загружено {len(rows)} записей из entity")
                    self.stats.record_container_loaded(len(rows))
                    
                    return rows
                    
            except Exception as e:
                self.logger.error(f"❌ Ошибка загрузки контейнеров: {e}")
                raise
        
        async with self._get_thread_pool() as thread_pool:
            loop = asyncio.get_event_loop()
            rows = await loop.run_in_executor(thread_pool, _load_containers)
            
            if not rows:
                self.logger.warning("📭 Нет контейнеров для обработки")
                return
            
            # Обрабатываем результаты и возвращаем батчами
            valid_containers = []
            
            for row in rows:
                container_info = self._create_container_info_from_row(row)
                
                if self._should_process_container(container_info):
                    valid_containers.append(container_info)
                else:
                    self.stats.record_container_filtered()
            
            self.logger.info(f"✅ К обработке: {len(valid_containers)} из {len(rows)} контейнеров")
            
            # Возвращаем батчами
            for i in range(0, len(valid_containers), batch_size):
                batch = valid_containers[i:i + batch_size]
                self.stats.record_batch_processed()
                
                self.logger.debug(f"📦 Батч {self.stats.stats['batches_processed']}: {len(batch)} контейнеров")
                yield batch

    async def get_containers_for_contractors(
        self,
        batch_size: int = 100,
        carrier_ids: Optional[Set[int]] = None,
        processed_ids: Optional[Set[int]] = None,
    ) -> AsyncGenerator[List[ContainerInfo], None]:
        """Получить контейнеры для указанных железнодорожных перевозчиков"""

        def _load_containers():
            try:
                with self.connection_manager.get_connection() as connection:
                    cursor = connection.cursor()

                    query, params = self._build_contractor_selection_query(carrier_ids, processed_ids)

                    self.logger.debug(f"🔥 SQL: {query}")
                    self.logger.debug(f"🔥 Params: {params}")

                    cursor.execute(query, params)
                    rows = cursor.fetchall()

                    self.logger.info(f"🔥 Загружено {len(rows)} записей из entity (contractor mode)")
                    self.stats.record_container_loaded(len(rows))

                    return rows

            except Exception as e:
                self.logger.error(f"❌ Ошибка загрузки контейнеров для перевозчиков: {e}")
                raise

        async with self._get_thread_pool() as thread_pool:
            loop = asyncio.get_event_loop()
            rows = await loop.run_in_executor(thread_pool, _load_containers)

            if not rows:
                self.logger.warning("📭 Нет контейнеров для обработки (contractors)")
                return

            valid_containers = []

            for row in rows:
                container_info = self._create_container_info_from_row(row)

                # Последний столбец - ID перевозчика
                if len(row) > 10:
                    container_info.railway_carrier_id = row[10]

                if self._should_process_container(container_info):
                    valid_containers.append(container_info)
                else:
                    self.stats.record_container_filtered()

            for i in range(0, len(valid_containers), batch_size):
                batch = valid_containers[i:i + batch_size]
                self.stats.record_batch_processed()
    
    def _build_selection_query(
        self, 
        target_line_ids: Optional[Set[int]], 
        min_priority: int
    ) -> Tuple[str, List[Any]]:
        """Построить SQL запрос для выборки"""
        
        # Базовый запрос (Firebird синтаксис)
        base_columns = [
            self.entity_config.primary_key,
            self.entity_config.container_column,
            self.entity_config.status_column,
            self.entity_config.line_column,
            self.entity_config.date_eta,
            self.entity_config.date_etd,
            self.entity_config.date_in,
            self.entity_config.date_railway_loading,
            self.entity_config.date_railway_delivery,
            self.entity_config.remaining_distance,
        ]
        
        query = f"""
        SELECT {', '.join(base_columns)}
        FROM {self.entity_config.table_name}
        WHERE 1=1
        """
        
        params = []
        
        # Исключаем статусы
        if self.entity_config.excluded_status_ids:
            status_placeholders = ','.join(['?'] * len(self.entity_config.excluded_status_ids))
            query += f" AND {self.entity_config.status_column} NOT IN ({status_placeholders})"
            params.extend([int(s) for s in self.entity_config.excluded_status_ids])
        
        # Фильтр по линиям
        if target_line_ids:
            line_placeholders = ','.join(['?'] * len(target_line_ids))
            query += f" AND {self.entity_config.line_column} IN ({line_placeholders})"
            params.extend(list((target_line_ids)))
        
        # Фильтр по приоритету
        if min_priority > 0:
            query += " AND COALESCE(PRIORITY, 0) >= ?"
            params.append(min_priority)
        
        # Сортировка
        query += f" ORDER BY {self.entity_config.primary_key}"
        
        return query, params
    
    def _build_contractor_selection_query(
        self,
        carrier_ids: Optional[Set[int]],
        processed_ids: Optional[Set[int]] = None,
    ) -> Tuple[str, List[Any]]:
        """Построить SQL запрос для выборки по перевозчикам"""

        base_columns = [
            self.entity_config.primary_key,
            self.entity_config.container_column,
            self.entity_config.status_column,
            self.entity_config.line_column,
            self.entity_config.date_eta,
            self.entity_config.date_etd,
            self.entity_config.date_in,
            self.entity_config.date_railway_loading,
            self.entity_config.date_railway_delivery,
            self.entity_config.remaining_distance,
            self.entity_config.railway_carrier_column,
        ]

        query = f"""
        SELECT {', '.join(base_columns)}
        FROM {self.entity_config.table_name}
        WHERE 1=1
        """

        params: List[Any] = []

        if self.entity_config.excluded_status_ids:
            status_placeholders = ','.join(['?'] * len(self.entity_config.excluded_status_ids))
            query += f" AND {self.entity_config.status_column} NOT IN ({status_placeholders})"
            params.extend(sorted(int(s) for s in self.entity_config.excluded_status_ids))

        if carrier_ids:
            carrier_placeholders = ','.join(['?'] * len(carrier_ids))
            query += f" AND {self.entity_config.railway_carrier_column} IN ({carrier_placeholders})"
            params.extend(sorted(carrier_ids))

        if processed_ids:
            id_placeholders = ','.join(['?'] * len(processed_ids))
            query += f" AND {self.entity_config.primary_key} NOT IN ({id_placeholders})"
            params.extend(sorted(processed_ids))

        query += f" ORDER BY {self.entity_config.primary_key}"

        return query, params

    def _create_container_info_from_row(self, row) -> ContainerInfo:
        """Создать ContainerInfo из строки результата"""
        
        return ContainerInfo(
            id=row[0],  # ID
            container_number=str(row[1]).strip() if row[1] else "",
            status_id=row[2],
            status_name=self._get_status_name(row[2]),
            line_id=row[3],  # LEGAL_PERSON_LINE_ID
            remaining_distance=int(row[9]) if row[9] is not None else None,
            current_dates={
                self.entity_config.date_eta: str(row[4]) if row[4] else None,
                self.entity_config.date_etd: str(row[5]) if row[5] else None,
                self.entity_config.date_in: str(row[6]) if row[6] else None,
                self.entity_config.date_railway_loading: str(row[7]) if row[7] else None,
                self.entity_config.date_railway_delivery: str(row[8]) if row[8] else None,
            },
            processing_flags={'loaded_from_firebird': True}
        )
    
    def _get_status_name(self, status_id: Optional[int]) -> str:
        """Получить название статуса по ID"""

        if status_id is None:
            return "UNKNOWN"
        
        try:
            status_enum = EntityStatusID(status_id)
            return status_enum.name
        except ValueError:
            return f"UNKNOWN_STATUS_{status_id}"
    
    def _should_process_container(self, container_info: ContainerInfo) -> bool:
        """
        Дополнительная проверка - нужно ли обрабатывать контейнер
        """

        if not container_info.container_number or not container_info.container_number.strip():
            self.logger.debug(f"⏭️ Пропускаем: пустой номер контейнера ID {container_info.id}")
            return False
        
        return True
    
    # =========================================================================
    # ОБНОВЛЕНИЕ КОНТЕЙНЕРОВ
    # =========================================================================
    
    async def update_container_from_tracking(
        self, 
        container_id: int,
        tracking_result: TrackingResult
    ) -> bool:
        """
        Обновить контейнер данными трекинга FESCO
                
        Args:
            container_id: ID записи в entity таблице
            tracking_result: Результат трекинга от FESCO API
            
        Returns:
            True если обновление успешно
        """
        
        if not tracking_result.success or not tracking_result.last_event:
            self.logger.debug(f"⏭️ Пропускаем обновление ID {container_id}: нет данных трекинга")
            return False
        
        # Находим подходящий маппинг
        operation = tracking_result.last_event.operation
        date_mapping = self.operation_matcher.find_best_mapping(operation)  # type: ignore

        update_data: Dict[str, Any] = {}
        
        if not date_mapping:
            self.logger.debug(f"🤷 Не найден маппинг для операции: {operation}")
        
        # Подготавливаем данные для обновления
        else:
            update_data = self._prepare_update_data(tracking_result, date_mapping)

        # Трансформируем оставшееся расстояние (TRACING_DAYS)
        remaining_raw = getattr(tracking_result.last_event, "remainingDistance", None)
        if remaining_raw is not None:
            remaining_transformed = self.transformer.transform_value(remaining_raw, "INTEGER")
            if remaining_transformed is not None:
                update_data[self.entity_config.remaining_distance] = remaining_transformed
        
        if not update_data:
            self.logger.debug(f"📭 Нет данных для обновления ID {container_id}")
            return False
        
        mapping_for_logging = date_mapping or self.entity_config.date_mappings.get(self.entity_config.remaining_distance)

        # Выполняем обновление
        def _update_sync():
            return self._sync_update_entity_record(container_id, update_data, mapping_for_logging)
        
        try:
            async with self._get_thread_pool() as thread_pool:
                loop = asyncio.get_event_loop()
                success = await loop.run_in_executor(thread_pool, _update_sync)
                
                if success:
                    if date_mapping and date_mapping.entity_column in update_data:
                        self.stats.record_update_success(date_mapping.entity_column, operation)  # type: ignore
                    if self.entity_config.remaining_distance in update_data:
                        self.stats.record_update_success(self.entity_config.remaining_distance, operation)
                else:
                    self.stats.record_update_failure()
                
                return success
                
        except Exception as e:
            self.logger.error(f"❌ Ошибка async обновления контейнера {container_id}: {e}")
            self.stats.record_update_failure()
            return False
    
    def _prepare_update_data(
        self, 
        tracking_result: TrackingResult, 
        date_mapping: EntityColumnMapping
    ) -> Dict[str, Any]:
        """Подготовить данные для обновления"""
        
        update_data = {}
        
        # Получаем значение по полю маппинга
        if date_mapping.fesco_field == "date":
            raw_value = getattr(tracking_result.last_event, "date", None)
        elif date_mapping.fesco_field == "remainingDistance":
            raw_value = getattr(tracking_result.last_event, "remainingDistance", None)
        else:
            raw_value = getattr(tracking_result.last_event, date_mapping.fesco_field, None)
        
        if raw_value:
            # 🔧 КЛЮЧЕВОЕ МЕСТО: выбираем трансформацию по типу колонки
            transformed_value = self.transformer.transform_value(
                raw_value, 
                date_mapping.column_datatype
            )
            
            if transformed_value is not None:
                update_data[date_mapping.entity_column] = transformed_value
                self.logger.debug(f"🔗 Маппинг: {raw_value} → {date_mapping.entity_column} = {transformed_value}")
        
        return update_data
    
    def _sync_update_entity_record(
        self, 
        entity_id: int, 
        update_data: Dict[str, Any],
        date_mapping: EntityColumnMapping
    ) -> bool:
        """Синхронное обновление записи entity"""
        
        start_time = datetime.now()
        
        try:
            with self.connection_manager.get_connection() as connection:
                # ИСПРАВЛЕНО: Правильная работа с транзакциями Firebird
                # transaction = connection.trans()
                # transaction.begin()
                transaction = None
                if hasattr(connection, "trans"):
                    transaction = connection.trans()
                    transaction.begin()
                else:
                    connection.begin()

                try:
                    cursor = connection.cursor()
                    
                    # Строим UPDATE запрос
                    set_clauses = []
                    values = []
                    
                    for column, value in update_data.items():
                        set_clauses.append(f"{column} = ?")
                        values.append(value)
                    
                    # Добавляем CURRENT_TIMESTAMP для updated_at
                    # set_clauses.append("UPDATED_AT = CURRENT_TIMESTAMP")
                    values.append(entity_id)  # ID для WHERE
                    
                    query = f"""
                    UPDATE {self.entity_config.table_name} 
                    SET {', '.join(set_clauses)}
                    WHERE {self.entity_config.primary_key} = ?
                    """
                    
                    self.logger.debug(f"🔥 SQL: {query}")
                    self.logger.debug(f"🔥 Values: {values}")
                    
                    # Выполняем запрос
                    cursor.execute(query, values)
                    
                    # Проверяем количество обновленных строк
                    rows_affected = cursor.rowcount
                    if rows_affected is None or rows_affected == -1:
                        # Fallback: проверяем через SELECT
                        cursor.execute(f"""
                            SELECT COUNT(*) FROM {self.entity_config.table_name} 
                            WHERE {self.entity_config.primary_key} = ?
                        """, [entity_id])
                        exists = cursor.fetchone()[0] > 0
                        rows_affected = 1 if exists else 0
                    
                    if transaction:
                        transaction.commit()
                    else:
                        connection.commit()
                    
                    # Обновляем метрики производительности
                    operation_time = (datetime.now() - start_time).total_seconds() * 1000
                    self.stats.record_operation_time(operation_time)
                    
                    if rows_affected > 0:
                        self.logger.info(f"✅ Обновлена entity ID {entity_id}: {date_mapping.entity_column}")
                        return True
                    else:
                        self.logger.warning(f"⚠️ Entity ID {entity_id} не найдена для обновления")
                        return False
                        
                except Exception as e:
                    if transaction:
                        transaction.rollback()
                    else:
                        connection.rollback()
                    raise e
        except Exception as e:
            self.logger.error(f"❌ Ошибка обновления ID {entity_id}: {e}")
            return False
    
    # =========================================================================
    # УТИЛИТЫ И СТАТИСТИКА
    # =========================================================================
    
    async def test_connection(self) -> bool:
        """Тестовое подключение к Firebird"""

        return await self.connection_manager.test_connection()
    
    async def get_entity_statistics(self) -> Dict[str, Any]:
        """
        Получить подробную статистику entity таблицы
        """
        
        def _get_db_stats():
            try:
                with self.connection_manager.get_connection() as connection:
                    cursor = connection.cursor()
                    
                    stats = {}
                    
                    # Общее количество
                    cursor.execute(f"SELECT COUNT(*) FROM {self.entity_config.table_name}")
                    stats['total_records'] = cursor.fetchone()[0]
                    
                    # По статусам
                    cursor.execute(f"""
                        SELECT {self.entity_config.status_column}, COUNT(*) 
                        FROM {self.entity_config.table_name} 
                        GROUP BY {self.entity_config.status_column}
                    """)
                    stats['status_distribution'] = dict(cursor.fetchall())
                    
                    # Исключенные записи
                    excluded_count = sum(
                        stats['status_distribution'].get(status_id, 0) 
                        for status_id in self.entity_config.excluded_status_ids
                    )
                    stats['excluded_records'] = excluded_count
                    stats['available_for_processing'] = stats['total_records'] - excluded_count
                    
                    return stats
                    
            except Exception as e:
                self.logger.error(f"❌ Ошибка получения статистики БД: {e}")
                return {}
        
        try:
            async with self._get_thread_pool() as thread_pool:
                loop = asyncio.get_event_loop()
                db_stats = await loop.run_in_executor(thread_pool, _get_db_stats)
                
                # Объединяем с runtime статистикой
                return {
                    **db_stats,
                    'runtime_stats': self.stats.get_summary(),
                    'excluded_status_ids': list(self.entity_config.excluded_status_ids)
                }
                
        except Exception as e:
            self.logger.error(f"❌ Ошибка async статистики: {e}")
            return {'runtime_stats': self.stats.get_summary()}

    async def close(self) -> None:
        """Закрыть все ресурсы"""
        if self._thread_pool:
            self._thread_pool.shutdown(wait=True)
            self._thread_pool = None
            self.logger.info("🔥 Thread pool закрыт")

        if hasattr(self, 'connection_manager') and hasattr(self.connection_manager, 'close'):
            await self.connection_manager.close()

# =============================================================================
# ФАБРИЧНЫЕ ФУНКЦИИ
# =============================================================================

def create_firebird_entity_manager(
    host: str,
    database: str,
    user: str,
    password: str,
    entity_config: Optional[EntityTableConfig] = None,
    **kwargs
) -> FirebirdEntityManager:
    """
    Фабричная функция для создания Firebird Entity Manager
    
    Args:
        host: Хост Firebird сервера
        database: Путь к файлу БД (.fdb)
        user: Пользователь (обычно SYSDBA)
        password: Пароль
        **kwargs: Дополнительные параметры конфигурации
        
    Returns:
        Настроенный FirebirdEntityManager
        
    Example:
        >>> manager = create_firebird_entity_manager(
        ...     host="localhost",
        ...     database="C:/shipping.fdb",
        ...     user="SYSDBA",
        ...     password="masterkey"
        ... )
        >>> 
        >>> if await manager.test_connection():
        ...     print("✅ Подключение успешно!")
    """
    
    firebird_config = {
        'host': host,
        'database': database,
        'user': user,
        'password': password
    }
    
    # Дополнительные параметры
    if 'dsn' in kwargs:
        firebird_config['dsn'] = kwargs['dsn']
    
    # Создаем кастомную конфигурацию entity если передана
    entity_config = kwargs.get('entity_config', EntityTableConfig())
    
    return FirebirdEntityManager(firebird_config, entity_config)


async def validate_firebird_config(config: dict) -> dict:
    """Валидация конфигурации Firebird"""
    
    errors = []
    warnings = []
    
    # Проверка обязательных полей для Firebird
    required_fields = ['host', 'database', 'user', 'password']
    for field in required_fields:
        if not config.get(field):
            errors.append(f"Отсутствует обязательное поле: {field}")
    
    # Проверка пути к БД
    database_path = config.get('database', '')
    if database_path and not database_path.lower().endswith('.fdb'):
        warnings.append("Файл БД обычно имеет расширение .fdb")
    
    # Проверка пользователя
    user = config.get('user', '')
    if user and user.upper() != 'SYSDBA':
        warnings.append("Обычно для Firebird используется пользователь SYSDBA")
    
    return {
        'valid': len(errors) == 0,
        'errors': errors,
        'warnings': warnings,
        'config_summary': {
            'host': config.get('host', 'not_set'),
            'database': config.get('database', 'not_set'),
            'user': config.get('user', 'not_set'),
            'has_password': bool(config.get('password'))
        }
    }
