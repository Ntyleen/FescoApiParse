# processing/workflow_coordinator.py
"""
Координатор workflow для полной реализации схемы canvas
Объединяет все компоненты в единый процесс обработки
"""

import asyncio
from typing import List, Dict, Set, AsyncGenerator
import aiohttp
from dataclasses import dataclass

from config.settings import Config
from cache.cache_base import CacheBackend
from api.api_client import FescoApiClient
from processing.container_bindings import ContainerBindingManager
from processing.events import EventProcessor
from database.container_source import DatabaseContainerSource, ContainerInfo
from database.external_writer import ExternalDatabaseWriter
from models.container_event import TrackingResult
from models.processing_stats import ProcessingStats
from utils.logging import get_logger


@dataclass
class EngineStats:
    """Статистика выполнения workflow"""
    containers_loaded: int = 0
    containers_processed: int = 0
    containers_successful: int = 0
    orders_discovered: int = 0
    orders_processed: int = 0
    api_calls_saved: int = 0  # Благодаря проверкам привязок
    records_written: int = 0


class ContainerTrackingEngine:
    """
    Главный координатор процесса трекинга контейнеров
    
    Реализует полную схему из canvas:
    1. Получение контейнеров из БД
    2. Поиск/проверка заявок  
    3. Привязка контейнеров к заявкам
    4. Проверка необходимости обработки
    5. Получение данных трекинга
    6. Сохранение в стороннюю БД
    
    Этот класс - как дирижер оркестра, координирующий работу всех компонентов
    """
    
    def __init__(
        self,
        config: Config,
        cache: CacheBackend,
        db_source: DatabaseContainerSource,
        external_writer: ExternalDatabaseWriter
    ):
        self.config = config
        self.cache = cache
        self.db_source = db_source
        self.external_writer = external_writer
        
        # Инициализируем компоненты
        self.stats = ProcessingStats()
        self.workflow_stats = EngineStats()
        self.binding_manager = ContainerBindingManager(cache)
        self.event_processor = EventProcessor()
        self.api_client = FescoApiClient(config, cache, self.stats)
        
        # Отслеживание обработанных заявок в рамках сессии
        self.session_processed_orders: Set[str] = set()
        
        self.logger = get_logger("fesco_tracker.workflow")
        self.logger.info("🎼 Workflow координатор инициализирован")
    
    async def run_full_workflow(self, batch_size: int = 100) -> WorkflowStats:
        """
        Запуск полного workflow обработки
        
        Args:
            batch_size: Размер батча для обработки контейнеров
            
        Returns:
            Статистика выполнения
        """
        
        self.logger.info("🚀 Запуск полного workflow трекинга")
        self.logger.info("="*60)
        
        # Настройка HTTP сессии для API запросов
        connector = aiohttp.TCPConnector(limit_per_host=5, keepalive_timeout=60)
        timeout = aiohttp.ClientTimeout(total=self.config.api.timeout_seconds)
        
        try:
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                
                # Обрабатываем контейнеры батчами
                async for batch in self.db_source.get_containers_batch(batch_size):
                    await self._process_container_batch(session, batch)
                
                # Финальная статистика
                await self._log_final_statistics()
                
        except Exception as e:
            self.logger.error(f"❌ Критическая ошибка в workflow: {e}")
            raise
        
        return self.workflow_stats
    
    async def _process_container_batch(
        self, 
        session: aiohttp.ClientSession, 
        containers: List[ContainerInfo]
    ) -> None:
        """
        Обработка батча контейнеров согласно схеме canvas
        
        Здесь реализуется основная логика схемы:
        - Проверяем привязки
        - Группируем по заявкам
        - Избегаем дублирующих запросов
        """
        
        self.workflow_stats.containers_loaded += len(containers)
        self.logger.info(f"📦 Обработка батча: {len(containers)} контейнеров")
        
        # Группируем контейнеры по заявкам для оптимизации
        order_groups = await self._group_containers_by_orders(session, containers)
        
        # Обрабатываем каждую группу заявки
        for order_id, container_group in order_groups.items():
            if order_id:  # Пропускаем контейнеры без заявок
                await self._process_order_group(session, order_id, container_group)
    
    async def _group_containers_by_orders(
        self, 
        session: aiohttp.ClientSession, 
        containers: List[ContainerInfo]
    ) -> Dict[str, List[ContainerInfo]]:
        """
        Группировка контейнеров по заявкам с реализацией логики из схемы
        
        Это ключевая функция - здесь происходит проверка привязок
        и принятие решений о необходимости API запросов
        """
        
        order_groups: Dict[str, List[ContainerInfo]] = {}
        
        for container in containers:
            container_number = container.container_number
            
            self.logger.debug(f"🔍 Обработка контейнера: {container_number}")
            
            # Шаг 1: Проверяем существующую привязку (из схемы)
            existing_order = await self.binding_manager.get_container_order(container_number)
            
            if existing_order:
                # Контейнер уже привязан к заявке
                self.logger.debug(f"🔗 {container_number} уже привязан к заявке {existing_order}")
                
                # Проверяем, не обрабатывали ли мы эту заявку в текущей сессии
                if existing_order in self.session_processed_orders:
                    self.logger.debug(f"⏭️ Заявка {existing_order} уже обработана в сессии")
                    self.workflow_stats.api_calls_saved += 1
                    continue
                
                order_id = existing_order
                
            else:
                # Шаг 2: Ищем заявку через API (из схемы)
                self.logger.debug(f"🔍 Поиск заявки для {container_number}")
                order_id = await self.api_client.find_order_by_container(session, container_number)
                
                if not order_id:
                    self.logger.warning(f"⚠️ Заявка не найдена для {container_number}")
                    continue
                
                # Шаг 3: Привязываем контейнер к заявке
                await self.binding_manager.bind_container_to_order(container_number, order_id)
                self.workflow_stats.orders_discovered += 1
            
            # Добавляем в группу для обработки
            if order_id not in order_groups:
                order_groups[order_id] = []
            order_groups[order_id].append(container)
        
        self.logger.info(f"📋 Сгруппировано в {len(order_groups)} заявок")
        return order_groups
    
    async def _process_order_group(
        self,
        session: aiohttp.ClientSession,
        order_id: str,
        containers: List[ContainerInfo]
    ) -> None:
        """
        Обработка группы контейнеров одной заявки
        
        Здесь реализуется пакетная оптимизация - 
        делаем минимум API запросов для максимума данных
        """
        
        container_numbers = [c.container_number for c in containers]
        self.logger.info(f"📡 Обработка заявки {order_id}: {len(containers)} контейнеров")
        
        try:
            # Получаем данные заявки одним запросом для всех контейнеров
            order_data = await self.api_client.get_order_tracking(session, order_id)
            
            # Проверяем кэш на изменения (логика из схемы)
            cache_key = f"order_last_check:{order_id}"
            last_check_data = await self.cache.get(cache_key)
            
            # Извлекаем текущие данные для сравнения
            current_order_summary = self._extract_order_summary(order_data, container_numbers)
            
            if last_check_data and self._data_unchanged(last_check_data, current_order_summary):
                self.logger.debug(f"💾 Данные заявки {order_id} не изменились")
                self.workflow_stats.api_calls_saved += len(containers)
                
                # Отмечаем заявку как обработанную в сессии
                self.session_processed_orders.add(order_id)
                return
            
            # Данные изменились - обрабатываем каждый контейнер
            self.logger.info(f"🔄 Данные заявки {order_id} изменились, обрабатываем детально")
            
            # Создаем задачи для параллельной обработки контейнеров
            container_tasks = []
            for container in containers:
                task = self._process_single_container(
                    session, container.container_number, order_id, order_data
                )
                container_tasks.append(task)
            
            # Выполняем параллельно с ограничением
            semaphore = asyncio.Semaphore(self.config.api.max_parallel)
            
            async def process_with_semaphore(task):
                async with semaphore:
                    return await task
            
            results = await asyncio.gather(
                *[process_with_semaphore(task) for task in container_tasks],
                return_exceptions=True
            )
            
            # Обрабатываем результаты
            successful_results = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    self.logger.error(f"❌ Ошибка обработки {containers[i].container_number}: {result}")
                    continue
                
                if result and result.success:
                    successful_results.append(result)
                    self.workflow_stats.containers_successful += 1
                
                self.workflow_stats.containers_processed += 1
            
            # Записываем успешные результаты в стороннюю БД
            if successful_results:
                await self._write_results_to_external_db(successful_results)
            
            # Обновляем кэш проверки
            await self.cache.set(cache_key, current_order_summary, ttl_seconds=3600)
            
            # Отмечаем заявку как обработанную
            await self.binding_manager.mark_order_processed(order_id)
            self.session_processed_orders.add(order_id)
            self.workflow_stats.orders_processed += 1
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка обработки заявки {order_id}: {e}")
    
    async def _process_single_container(
        self,
        session: aiohttp.ClientSession,
        container_number: str,
        order_id: str,
        order_data: dict
    ) -> TrackingResult:
        """
        Детальная обработка одного контейнера
        
        Использует вашу существующую логику обработки событий
        """
        
        result = TrackingResult(container_number=container_number)
        result.order_id = order_id
        
        try:
            # Получаем детальные данные контейнера
            container_data = await self.api_client.get_container_tracking(
                session, order_id, container_number
            )
            
            # Используем ваш существующий EventProcessor
            order_events = self.event_processor.extract_order_events(
                order_data, order_id, container_number
            )
            container_events = self.event_processor.extract_container_events(container_data)
            
            final_event, has_duplicates, source = self.event_processor.merge_and_deduplicate(
                order_events, container_events
            )
            
            result.last_event = final_event
            result.has_duplicates = has_duplicates
            result.events_source = source
            
            if has_duplicates:
                self.stats.deduplicated_events += 1
            
            return result
            
        except Exception as e:
            result.error_message = f"Processing error: {e}"
            return result
    
    async def _write_results_to_external_db(self, results: List[TrackingResult]) -> None:
        """Запись результатов в стороннюю БД"""
        
        written_count = 0
        for result in results:
            try:
                success = await self.external_writer.write_tracking_result(result)
                if success:
                    written_count += 1
                    
            except Exception as e:
                self.logger.error(f"❌ Ошибка записи {result.container_number}: {e}")
        
        self.workflow_stats.records_written += written_count
        self.logger.info(f"💾 Записано {written_count}/{len(results)} результатов в стороннюю БД")
    
    def _extract_order_summary(self, order_data: dict, container_numbers: List[str]) -> dict:
        """Извлечь краткую сводку заявки для проверки изменений"""
        
        summary = {}
        
        try:
            for order_item in order_data.get("data", []):
                for container in order_item.get("containers", []):
                    container_num = container.get("containerNumber", "")
                    if container_num in container_numbers:
                        last_event = container.get("lastEvent", {})
                        summary[container_num] = {
                            "date": last_event.get("date"),
                            "operation": last_event.get("text"),
                            "location": last_event.get("location")
                        }
        except Exception as e:
            self.logger.debug(f"Ошибка извлечения сводки: {e}")
        
        return summary
    
    def _data_unchanged(self, cached_data: dict, current_data: dict) -> bool:
        """Проверка изменения данных"""
        try:
            return cached_data == current_data
        except:
            return False
    
    async def _log_final_statistics(self) -> None:
        """Вывод финальной статистики workflow"""
        
        # Получаем статистику записи
        write_stats = await self.external_writer.get_write_statistics()
        
        self.logger.info("="*60)
        self.logger.info("📊 ФИНАЛЬНАЯ СТАТИСТИКА WORKFLOW")
        self.logger.info("="*60)
        self.logger.info(f"📦 Контейнеров загружено:     {self.workflow_stats.containers_loaded:,}")
        self.logger.info(f"⚙️ Контейнеров обработано:    {self.workflow_stats.containers_processed:,}")
        self.logger.info(f"✅ Успешно обработано:        {self.workflow_stats.containers_successful:,}")
        self.logger.info(f"📋 Заявок обнаружено:         {self.workflow_stats.orders_discovered:,}")
        self.logger.info(f"📡 Заявок обработано:         {self.workflow_stats.orders_processed:,}")
        self.logger.info(f"⚡ API запросов сэкономлено:   {self.workflow_stats.api_calls_saved:,}")
        self.logger.info(f"💾 Записей в стороннюю БД:    {write_stats.get('total_operations', 0):,}")
        
        # Расчет эффективности
        if self.workflow_stats.containers_loaded > 0:
            success_rate = (self.workflow_stats.containers_successful / self.workflow_stats.containers_loaded) * 100
            self.logger.info(f"📈 Процент успеха:             {success_rate:.1f}%")
        
        if self.workflow_stats.api_calls_saved > 0:
            total_potential_calls = self.workflow_stats.containers_loaded * 2  # order + container запросы
            efficiency = (self.workflow_stats.api_calls_saved / total_potential_calls) * 100
            self.logger.info(f"⚡ Эффективность кэша:         {efficiency:.1f}%")
        
        self.logger.info("="*60)


# Фабричная функция для удобного создания workflow
async def create_workflow(
    config: Config,
    cache: CacheBackend,
    db_source_config: dict,
    external_db_config: dict,
    table_configs: list
) -> ContainerTrackingWorkflow:
    """
    Создать и инициализировать полный workflow
    
    Args:
        config: Основная конфигурация приложения
        cache: Кэш для операций
        db_source_config: Конфигурация источника данных
        external_db_config: Конфигурация сторонней БД
        table_configs: Конфигурации таблиц для записи
        
    Returns:
        Готовый к работе workflow координатор
    """
    
    # Создаем компоненты
    db_source = DatabaseContainerSource(db_source_config)
    await db_source.connect()
    
    external_writer = ExternalDatabaseWriter(external_db_config, table_configs)
    await external_writer.connect()
    
    # Создаем workflow
    workflow = ContainerTrackingWorkflow(config, cache, db_source, external_writer)
    
    return workflow