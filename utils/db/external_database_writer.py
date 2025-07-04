# database/external_writer.py
"""
Модуль для записи отформатированных данных трекинга в БД стороннего приложения
Реализует финальный блок схемы: "Заносим выбранные данные в нужные столбцы в базу данных стороннего приложения"
"""

import asyncio
import aiomysql
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from datetime import datetime
from models.container_event import TrackingResult
from utils.logging import get_logger


@dataclass
class ColumnMapping:
    """Маппинг поля FESCO на колонку БД"""
    fesco_field: str          # Поле из FESCO API (например, 'operation')
    target_column: str        # Колонка в БД (например, 'status_description')
    transform_func: Optional[str] = None  # Функция преобразования ('date', 'upper', 'lower')
    condition_value: Optional[str] = None  # Условие для записи (например, только если operation содержит 'погружен')


@dataclass
class TableConfig:
    """Конфигурация таблицы для записи"""
    table_name: str
    primary_key: str          # Название колонки первичного ключа
    container_column: str     # Колонка с номером контейнера
    columns: Dict[str, ColumnMapping]  # Маппинги колонок


class ExternalDatabaseWriter:
    """
    Писатель данных в стороннюю БД
    
    Основные функции:
    - Подключение к БД стороннего приложения
    - Маппинг данных FESCO на структуру БД
    - Валидация данных перед записью
    - Batch-операции для производительности
    """
    
    def __init__(self, db_config: dict, table_configs: List[TableConfig]):
        """
        Args:
            db_config: Конфигурация подключения к сторонней БД
            table_configs: Список конфигураций таблиц для записи
        """
        self.db_config = db_config
        self.table_configs = {config.table_name: config for config in table_configs}
        self.connection_pool = None
        self.logger = get_logger("fesco_tracker.external_writer")
        
        # Статистика операций
        self.stats = {
            'records_written': 0,
            'records_updated': 0,
            'records_failed': 0,
            'tables_affected': set()
        }
        
        self.logger.debug(f"📝 ExternalDatabaseWriter настроен для {len(table_configs)} таблиц")
    
    async def connect(self) -> None:
        """Создание пула подключений к сторонней БД"""
        try:
            self.connection_pool = await aiomysql.create_pool(
                host=self.db_config['host'],
                port=self.db_config.get('port', 3306),
                user=self.db_config['user'],
                password=self.db_config['password'],
                db=self.db_config['database'],
                charset='utf8mb4',
                autocommit=False,  # Используем транзакции
                minsize=2,
                maxsize=10
            )
            
            # Проверяем подключение
            async with self.connection_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("SELECT 1")
                    await cursor.fetchone()
            
            self.logger.info(f"🔗 Подключение к сторонней БД: {self.db_config['host']}/{self.db_config['database']}")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка подключения к сторонней БД: {e}")
            raise
    
    async def write_tracking_result(self, result: TrackingResult) -> bool:
        """
        Записать результат трекинга в соответствующие таблицы
        
        Args:
            result: Результат трекинга контейнера
            
        Returns:
            True если запись успешна
        """
        
        if not result.success or not result.last_event:
            self.logger.debug(f"⏭️ Пропускаем запись для {result.container_number}: нет данных")
            return False
        
        success_count = 0
        total_tables = len(self.table_configs)
        
        # Записываем в каждую настроенную таблицу
        for table_name, table_config in self.table_configs.items():
            try:
                written = await self._write_to_table(result, table_config)
                if written:
                    success_count += 1
                    self.stats['tables_affected'].add(table_name)
                    
            except Exception as e:
                self.logger.error(f"❌ Ошибка записи в таблицу {table_name}: {e}")
                self.stats['records_failed'] += 1
        
        # Считаем успешной если записали хотя бы в одну таблицу
        overall_success = success_count > 0
        
        if overall_success:
            self.logger.info(f"✅ Данные {result.container_number} записаны в {success_count}/{total_tables} таблиц")
        else:
            self.logger.warning(f"⚠️ Не удалось записать данные {result.container_number} ни в одну таблицу")
        
        return overall_success
    
    async def _write_to_table(self, result: TrackingResult, table_config: TableConfig) -> bool:
        """Записать данные в конкретную таблицу"""
        
        # Подготавливаем данные для записи
        data_to_write = self._prepare_table_data(result, table_config)
        
        if not data_to_write:
            self.logger.debug(f"📭 Нет данных для записи в таблицу {table_config.table_name}")
            return False
        
        # Проверяем существование записи
        record_id = await self._find_existing_record(result.container_number, table_config)
        
        if record_id:
            # Обновляем существующую запись
            success = await self._update_record(record_id, data_to_write, table_config)
            if success:
                self.stats['records_updated'] += 1
        else:
            # Создаем новую запись
            success = await self._insert_record(result.container_number, data_to_write, table_config)
            if success:
                self.stats['records_written'] += 1
        
        return success
    
    def _prepare_table_data(self, result: TrackingResult, table_config: TableConfig) -> Dict[str, Any]:
        """
        Подготовить данные для записи в таблицу согласно маппингу
        
        Это ключевая функция - здесь происходит маппинг данных FESCO
        на структуру БД стороннего приложения
        """
        
        prepared_data = {}
        event = result.last_event
        
        for column_name, mapping in table_config.columns.items():
            try:
                # Получаем значение из результата трекинга
                raw_value = self._extract_value(result, mapping.fesco_field)
                
                if raw_value is None:
                    continue
                
                # Проверяем условие для записи
                if mapping.condition_value:
                    if not self._check_condition(raw_value, mapping.condition_value):
                        continue
                
                # Применяем трансформацию
                transformed_value = self._transform_value(raw_value, mapping.transform_func)
                
                if transformed_value is not None:
                    prepared_data[column_name] = transformed_value
                    self.logger.debug(f"🔗 Маппинг: {mapping.fesco_field} → {column_name} = {transformed_value}")
            
            except Exception as e:
                self.logger.warning(f"⚠️ Ошибка маппинга {mapping.fesco_field}: {e}")
        
        return prepared_data
    
    def _extract_value(self, result: TrackingResult, fesco_field: str) -> Any:
        """Извлечь значение из результата трекинга"""
        
        # Карта полей для извлечения данных
        field_map = {
            'container_number': result.container_number,
            'order_id': result.order_id,
            'events_source': result.events_source,
            'processing_time': result.processing_time,
        }
        
        # Поля последнего события
        if result.last_event:
            field_map.update({
                'date': result.last_event.date,
                'location': result.last_event.location,
                'operation': result.last_event.operation,
                'type': result.last_event.type,
                'transport': result.last_event.transport,
                'remaining_distance': result.last_event.remainingDistance
            })
        
        return field_map.get(fesco_field)
    
    def _transform_value(self, value: Any, transform_func: Optional[str]) -> Any:
        """Применить функцию преобразования к значению"""
        
        if not transform_func or value is None:
            return value
        
        try:
            if transform_func == 'upper':
                return str(value).upper()
            
            elif transform_func == 'lower':
                return str(value).lower()
            
            elif transform_func == 'date':
                # Попытка преобразовать в стандартный формат даты
                if isinstance(value, str):
                    # Здесь можно добавить более сложную логику парсинга дат FESCO
                    return value  # Пока возвращаем как есть
                return value
            
            elif transform_func == 'trim':
                return str(value).strip()
            
            elif transform_func == 'not_null':
                return value if value else 'Unknown'
            
            else:
                self.logger.warning(f"⚠️ Неизвестная функция преобразования: {transform_func}")
                return value
                
        except Exception as e:
            self.logger.warning(f"⚠️ Ошибка преобразования значения {value} с функцией {transform_func}: {e}")
            return value
    
    def _check_condition(self, value: Any, condition: str) -> bool:
        """Проверить условие для записи значения"""
        
        if not condition:
            return True
        
        value_str = str(value).lower()
        condition_str = condition.lower()
        
        # Простые условия
        if condition_str.startswith('contains:'):
            search_term = condition_str.replace('contains:', '').strip()
            return search_term in value_str
        
        elif condition_str.startswith('equals:'):
            target_value = condition_str.replace('equals:', '').strip()
            return value_str == target_value
        
        elif condition_str.startswith('not_empty'):
            return bool(value and str(value).strip())
        
        # По умолчанию - простое содержание
        return condition_str in value_str
    
    async def _find_existing_record(self, container_number: str, table_config: TableConfig) -> Optional[int]:
        """Найти существующую запись по номеру контейнера"""
        
        query = f"""
        SELECT {table_config.primary_key} 
        FROM {table_config.table_name} 
        WHERE {table_config.container_column} = %s
        LIMIT 1
        """
        
        try:
            async with self.connection_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(query, (container_number,))
                    row = await cursor.fetchone()
                    return row[0] if row else None
                    
        except Exception as e:
            self.logger.error(f"❌ Ошибка поиска записи для {container_number}: {e}")
            return None
    
    async def _update_record(self, record_id: int, data: Dict[str, Any], table_config: TableConfig) -> bool:
        """Обновить существующую запись"""
        
        if not data:
            return False
        
        # Строим UPDATE запрос
        set_clauses = []
        values = []
        
        for column, value in data.items():
            set_clauses.append(f"{column} = %s")
            values.append(value)
        
        values.append(record_id)  # Для WHERE условия
        
        query = f"""
        UPDATE {table_config.table_name} 
        SET {', '.join(set_clauses)}, updated_at = NOW()
        WHERE {table_config.primary_key} = %s
        """
        
        try:
            async with self.connection_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(query, values)
                    await conn.commit()
                    
                    rows_affected = cursor.rowcount
                    if rows_affected > 0:
                        self.logger.debug(f"🔄 Обновлена запись ID {record_id} в {table_config.table_name}")
                        return True
                    else:
                        self.logger.warning(f"⚠️ Не удалось обновить запись ID {record_id}")
                        return False
                        
        except Exception as e:
            self.logger.error(f"❌ Ошибка обновления записи ID {record_id}: {e}")
            await conn.rollback()
            return False
    
    async def _insert_record(self, container_number: str, data: Dict[str, Any], table_config: TableConfig) -> bool:
        """Создать новую запись"""
        
        # Добавляем номер контейнера к данным
        data[table_config.container_column] = container_number
        
        # Строим INSERT запрос
        columns = list(data.keys())
        placeholders = ['%s'] * len(columns)
        values = list(data.values())
        
        query = f"""
        INSERT INTO {table_config.table_name} 
        ({', '.join(columns)}, created_at, updated_at) 
        VALUES ({', '.join(placeholders)}, NOW(), NOW())
        """
        
        try:
            async with self.connection_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(query, values)
                    await conn.commit()
                    
                    new_id = cursor.lastrowid
                    self.logger.debug(f"➕ Создана запись ID {new_id} в {table_config.table_name}")
                    return True
                    
        except Exception as e:
            self.logger.error(f"❌ Ошибка создания записи для {container_number}: {e}")
            await conn.rollback()
            return False
    
    async def get_write_statistics(self) -> Dict[str, Any]:
        """Получить статистику записи"""
        return {
            'records_written': self.stats['records_written'],
            'records_updated': self.stats['records_updated'],
            'records_failed': self.stats['records_failed'],
            'tables_affected': list(self.stats['tables_affected']),
            'total_operations': self.stats['records_written'] + self.stats['records_updated']
        }
    
    async def close(self) -> None:
        """Закрыть подключения"""
        if self.connection_pool:
            self.connection_pool.close()
            await self.connection_pool.wait_closed()
            self.logger.info("🔒 Подключения к сторонней БД закрыты")


# Вспомогательные функции для создания конфигураций

def create_shipment_table_config() -> TableConfig:
    """Пример конфигурации для таблицы shipments"""
    return TableConfig(
        table_name='shipments',
        primary_key='shipment_id',
        container_column='container_number',
        columns={
            'last_location': ColumnMapping('location', 'last_location', 'trim'),
            'current_status': ColumnMapping('operation', 'current_status', 'trim'),
            'last_update_date': ColumnMapping('date', 'last_update_date', 'date'),
            'transport_type': ColumnMapping('transport', 'transport_type', 'upper'),
            'tracking_source': ColumnMapping('events_source', 'tracking_source', 'upper'),
            'remaining_distance': ColumnMapping('remaining_distance', 'remaining_distance'),
            
            # Условные маппинги для специальных операций
            'loaded_date': ColumnMapping('date', 'loaded_date', 'date', 'contains:погружен'),
            'discharged_date': ColumnMapping('date', 'discharged_date', 'date', 'contains:выгружен'),
            'customs_cleared_date': ColumnMapping('date', 'customs_cleared_date', 'date', 'contains:таможен'),
        }
    )


def create_tracking_events_table_config() -> TableConfig:
    """Пример конфигурации для таблицы tracking_events"""
    return TableConfig(
        table_name='tracking_events',
        primary_key='event_id',
        container_column='container_no',
        columns={
            'event_timestamp': ColumnMapping('date', 'event_timestamp', 'date'),
            'event_location': ColumnMapping('location', 'event_location', 'trim'),
            'event_description': ColumnMapping('operation', 'event_description'),
            'event_type': ColumnMapping('type', 'event_type', 'upper'),
            'data_source': ColumnMapping('events_source', 'data_source', 'upper'),
        }
    )