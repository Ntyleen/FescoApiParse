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
from contextlib import contextmanager, asynccontextmanager
from models.container_event import TrackingResult
from utils.logging import get_logger

from .connection import FirebirdConnectionManager
from .transformer import FirebirdDateTransformer
from .matcher import FirebirdOperationMatcher
from .stats import FirebirdStatisticsCollector
from .models import EntityStatusID, EntityColumnMapping, EntityTableConfig

try:
    import firebird.driver as fdb  # pip install firebird-driver
    FIREBIRD_AVAILABLE = True
except ImportError:
    FIREBIRD_AVAILABLE = False
    fdb = None



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
        
        # Исключаем статусы (Firebird использует ? вместо %s)
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
