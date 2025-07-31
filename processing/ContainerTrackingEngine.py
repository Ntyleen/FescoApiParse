# processing/ContainerTrackingEngine.py
"""
Координатор workflow для полной реализации с прямой интеграцией Firebird
"""

import asyncio
from typing import List, Dict, Set, AsyncGenerator, Optional, Union, Tuple
import aiohttp
from dataclasses import dataclass

from config.settings import Config
from cache.cache_base import CacheBackend
from api.api_client import FescoApiClient
from processing.container_bindings import ContainerBindingManager
from processing.events import EventProcessor

# НОВОЕ: Прямой импорт Firebird компонентов
from utils.db.firebird_manager import (
    FirebirdEntityManager,
    ContainerInfo,
    create_firebird_entity_manager,
)
from utils.db.models import EntityTableConfig

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
    api_calls_saved: int = 0
    records_written: int = 0
    # НОВОЕ: Firebird специфичные метрики
    firebird_updates: int = 0
    firebird_read_batches: int = 0


class ContainerTrackingEngine:
    """
    Главный координатор процесса трекинга контейнеров
    
    ОБНОВЛЕНО: Теперь использует единый FirebirdEntityManager
    для чтения и записи в entity таблицу
    """
    
    def __init__(
        self,
        config: Config,
        cache: CacheBackend,
        firebird_manager: FirebirdEntityManager  # ИЗМЕНЕНО: Один менеджер вместо двух
    ):
        self.config = config
        self.cache = cache
        self.firebird_manager = firebird_manager  # НОВОЕ: Единый менеджер БД
        
        # Инициализируем компоненты
        self.stats = ProcessingStats()
        self.engine_stats = EngineStats()  # ИЗМЕНЕНО: Переименовано из workflow_stats
        self.binding_manager = ContainerBindingManager(cache)
        self.event_processor = EventProcessor()
        self.api_client = FescoApiClient(config, cache, self.stats)
        
        # Отслеживание обработанных заявок в рамках сессии
        self.session_processed_orders: Set[str] = set()
        
        self.logger = get_logger("fesco_tracker.engine")
        self.logger.info("🎼 ContainerTrackingEngine инициализирован с Firebird интеграцией")
    
    async def run_full_workflow(
        self,
        batch_size: int = 100,
        target_line_ids: Optional[Set[int]] = None
    ) -> EngineStats:
        """
        Запуск полного workflow обработки
        
        Args:
            batch_size: Размер батча для обработки контейнеров
            target_line_ids: ID линий для фильтрации (None = все линии)
            
        Returns:
            Статистика выполнения
        """
        if target_line_ids is None:
            target_line_ids = set(self.config.database.target_line_ids)

        self.logger.info("🚀 Запуск полного workflow трекинга")
        self.logger.info("="*60)
        
        # Проверяем подключение к Firebird
        if not await self.firebird_manager.test_connection():
            raise RuntimeError("❌ Не удается подключиться к Firebird")
        
        self.logger.info("✅ Firebird подключение проверено")
        
        # Настройка HTTP сессии для API запросов
        connector = aiohttp.TCPConnector(limit_per_host=5, keepalive_timeout=60)
        timeout = aiohttp.ClientTimeout(total=self.config.api.timeout_seconds)
        
        try:
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                
                # ИЗМЕНЕНО: Читаем контейнеры напрямую из Firebird
                async for batch in self.firebird_manager.get_containers_for_processing(
                    batch_size=batch_size,
                    target_line_ids=target_line_ids
                ):
                    await self._process_container_batch(session, batch)
                    self.engine_stats.firebird_read_batches += 1
                
                # Финальная статистика
                await self._log_final_statistics()
                
        except Exception as e:
            self.logger.error(f"❌ Критическая ошибка в workflow: {e}")
            raise
        finally:
            # НОВОЕ: Закрываем Firebird ресурсы
            await self.firebird_manager.close()
        
        return self.engine_stats
    
    async def _process_container_batch(
        self, 
        session: aiohttp.ClientSession, 
        containers: List[ContainerInfo]  # ИЗМЕНЕНО: Теперь ContainerInfo из Firebird
    ) -> None:
        """
        Обработка батча контейнеров
        """
        
        self.engine_stats.containers_loaded += len(containers)
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
        containers: List[ContainerInfo]  # ИЗМЕНЕНО: ContainerInfo
    ) -> Dict[str, List[ContainerInfo]]:
        """
        Группировка контейнеров по заявкам
        """
        
        order_groups: Dict[str, List[ContainerInfo]] = {}
        
        for container in containers:
            container_number = container.container_number
            
            self.logger.debug(f"🔍 Обработка контейнера: {container_number}")

            # Пропускаем контейнеры, для которых заявка ранее не была найдена
            if await self.binding_manager.is_container_no_order(container_number):
                self.logger.debug(f"⏭️ Контейнер {container_number} помечен как без заявки")
                continue

            # Шаг 1: Проверяем существующую привязку
            existing_order = await self.binding_manager.get_container_order(container_number)
            
            if existing_order:
                # Контейнер уже привязан к заявке
                self.logger.debug(f"🔗 {container_number} уже привязан к заявке {existing_order}")
                
                if existing_order in self.session_processed_orders:
                    self.logger.debug(f"⏭️ Заявка {existing_order} уже обработана в сессии")
                    self.engine_stats.api_calls_saved += 1
                    continue
                
                order_id = existing_order
                
            else:
                # Шаг 2: Ищем заявку через API
                self.logger.debug(f"🔍 Поиск заявки для {container_number}")
                order_id = await self.api_client.find_order_by_container(session, container_number)
                
                if not order_id:
                    self.logger.warning(f"⚠️ Заявка не найдена для {container_number}")
                    await self.binding_manager.mark_container_no_order(container_number)
                    continue
                
                # Шаг 3: Привязываем контейнер к заявке
                await self.binding_manager.bind_container_to_order(container_number, order_id)
                self.engine_stats.orders_discovered += 1
            
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
        containers: List[ContainerInfo]  # ИЗМЕНЕНО: ContainerInfo
    ) -> None:
        """
        Обработка группы контейнеров одной заявки
        """
        
        container_numbers = [c.container_number.strip() for c in containers]
        self.logger.info(f"📡 Обработка заявки {order_id}: {len(containers)} контейнеров")
        
        try:
            # Получаем данные заявки одним запросом для всех контейнеров
            order_data = await self.api_client.get_order_tracking(session, order_id)
            
            # Проверяем кэш на изменения
            cache_key = f"order_last_check:{order_id}"
            last_check_data = await self.cache.get(cache_key)
            
            # Извлекаем текущие данные для сравнения
            current_order_summary = self._extract_order_summary(order_data, container_numbers)
            
            if last_check_data and self._data_unchanged(last_check_data, current_order_summary):
                self.logger.debug(f"💾 Данные заявки {order_id} не изменились")
                self.engine_stats.api_calls_saved += len(containers)
                
                # Отмечаем заявку как обработанную в сессии
                self.session_processed_orders.add(order_id)
                return
            
            # Данные изменились - обрабатываем каждый контейнер
            self.logger.info(f"🔄 Данные заявки {order_id} изменились, обрабатываем детально")
            
            # Создаем задачи для параллельной обработки контейнеров
            container_tasks = []
            for container in containers:
                task = self._process_single_container(
                    session, container, order_id, order_data  # Передаем ContainerInfo
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
            
            # Обрабатываем результаты с явной типизацией
            successful_results: List[Tuple[ContainerInfo, TrackingResult]] = []
            for i, result in enumerate(results):
                # Увеличиваем счетчик обработанных в любом случае
                self.engine_stats.containers_processed += 1
                
                if isinstance(result, Exception):
                    # Это исключение - логируем и пропускаем
                    self.logger.error(f"❌ Ошибка обработки {containers[i].container_number}: {result}")
                    continue
                
                # Явная проверка типа для IDE
                if not isinstance(result, TrackingResult):
                    self.logger.error(f"❌ Неожиданный тип результата для {containers[i].container_number}: {type(result)}")
                    continue
                
                # Теперь IDE точно знает, что result это TrackingResult
                tracking_result: TrackingResult = result  # Explicit cast для IDE
                if tracking_result.success:
                    successful_results.append((containers[i], tracking_result))
                    self.engine_stats.containers_successful += 1
                else:
                    # Результат получен, но не успешный
                    error_msg = tracking_result.error_message or "Неизвестная ошибка"
                    self.logger.debug(f"⚠️ Неуспешная обработка {containers[i].container_number}: {error_msg}")
            
            # ИЗМЕНЕНО: Записываем результаты напрямую в Firebird
            if successful_results:
                await self._write_results_to_firebird(successful_results)
            
            enriched_summary = self._enrich_summary_with_results(
                current_order_summary, successful_results
            )

            # Обновляем кэш проверки
            ttl_seconds = self.config.cache.ttl_hours * 3600
            await self.cache.set(cache_key, enriched_summary, ttl_seconds=ttl_seconds)
            
            # Отмечаем заявку как обработанную
            await self.binding_manager.mark_order_processed(order_id)
            self.session_processed_orders.add(order_id)
            self.engine_stats.orders_processed += 1
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка обработки заявки {order_id}: {e}")
    
    async def _process_single_container(
        self,
        session: aiohttp.ClientSession,
        container: ContainerInfo,
        order_id: str,
        order_data: dict
    ) -> TrackingResult:
        """
        Детальная обработка одного контейнера
        
        Returns:
            TrackingResult: Всегда возвращает TrackingResult (никогда не None)
        """
        
        result = TrackingResult(container_number=container.container_number)
        result.order_id = order_id
        
        try:
            # Получаем детальные данные контейнера
            container_data = await self.api_client.get_container_tracking(
                session, order_id, container.container_number
            )
            
            # Используем существующий EventProcessor
            order_events = self.event_processor.extract_order_events(
                order_data, order_id, container.container_number
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
    
    async def _write_results_to_firebird(
        self, 
        container_results: List[Tuple[ContainerInfo, TrackingResult]]
    ) -> None:

        
        written_count = 0
        for container_info, tracking_result in container_results:
            try:
                mapping = self.firebird_manager.operation_matcher.find_best_mapping(
                    tracking_result.last_event.operation    # type: ignore
                ) if tracking_result.last_event else None

                new_remaining_raw = (
                    tracking_result.last_event.remainingDistance
                    if tracking_result.last_event else None
                )
                new_remaining = None
                if new_remaining_raw is not None:
                    new_remaining = self.firebird_manager.transformer.transform_value(
                        new_remaining_raw, "INTEGER"
                    )

                if (
                    new_remaining is not None
                    and container_info.remaining_distance is not None
                    and new_remaining == container_info.remaining_distance
                ):
                    self.logger.debug(
                        f"⏭️ {container_info.container_number}: remaining distance unchanged"
                    )
                    continue

                if mapping and mapping.entity_column in container_info.current_dates:
                    new_value = (
                        tracking_result.last_event.date if tracking_result.last_event else None
                    )
                    stored_value = container_info.current_dates.get(mapping.entity_column)

                    if new_value is None:
                        self.logger.debug(
                            f"⏭️ {container_info.container_number}: empty value for {mapping.entity_column}"
                        )
                        continue

                    if stored_value:
                        parsed_new = self.firebird_manager.transformer.transform_value(
                            new_value, mapping.column_datatype
                        )
                        parsed_stored = self.firebird_manager.transformer.transform_value(
                            stored_value, mapping.column_datatype
                        )

                        if (
                            parsed_new is not None
                            and parsed_stored is not None
                            and parsed_new >= parsed_stored
                        ):
                            self.logger.debug(
                                f"⏭️ {container_info.container_number}: existing {mapping.entity_column} {stored_value} is earlier"
                            )
                            continue
                        if parsed_new is None:
                            self.logger.debug(
                                f"⏭️ {container_info.container_number}: unable to parse new value for {mapping.entity_column}"
                            )
                            continue
                # КЛЮЧЕВОЕ: Используем container_info.id для обновления записи
                success = await self.firebird_manager.update_container_from_tracking(
                    container_info.id,  # ID записи в entity таблице
                    tracking_result
                )
                
                if success and mapping:
                    container_info.current_dates[mapping.entity_column] = (
                        tracking_result.last_event.date
                        if tracking_result.last_event
                        else container_info.current_dates.get(mapping.entity_column)
                    )

                if success and new_remaining is not None:
                    container_info.remaining_distance = new_remaining

                if success:
                    written_count += 1
                    self.engine_stats.firebird_updates += 1
                    
            except Exception as e:
                self.logger.error(f"❌ Ошибка записи {tracking_result.container_number}: {e}")
        
        self.engine_stats.records_written += written_count
        self.logger.info(f"💾 Обновлено {written_count}/{len(container_results)} записей в Firebird")
    
    def _extract_order_summary(self, order_data: dict, container_numbers: List[str]) -> dict:
        """Извлечь краткую сводку заявки для проверки изменений"""
        
        summary = {}
        normalized_numbers = {num.strip() for num in container_numbers}

        try:
            for order_item in order_data.get("data", []):
                for container in order_item.get("containers", []):
                    container_num = container.get("containerNumber", "").strip()
                    if container_num in normalized_numbers:
                        last_event = container.get("lastEvent", {})
                        summary[container_num] = {
                            "date": (last_event.get("date") or "").strip(),
                            "operation": (last_event.get("text") or "").strip(),
                            "location": (last_event.get("location") or "").strip(),
                            "remainingDistance": (last_event.get("remainingDistance") or "").strip(),
                        }
        except Exception as e:
            self.logger.debug(f"Ошибка извлечения сводки: {e}")
        
        return summary
    

    def _enrich_summary_with_results(
        self,
        summary: dict,
        container_results: List[Tuple[ContainerInfo, TrackingResult]],
    ) -> dict:
        """Дополнить сводку данными из результатов обработки"""

        enriched = {k: v.copy() for k, v in summary.items()}

        for container_info, result in container_results:
            container_num = container_info.container_number.strip()
            if not result.last_event:
                continue
            last_event = result.last_event
            event_data = {
                "date": (last_event.date or "").strip(),
                "operation": (last_event.operation or "").strip(),
                "location": (last_event.location or "").strip(),
                "remainingDistance": (
                    getattr(last_event, "remainingDistance", "") or ""
                ).strip(),
            }

            existing = enriched.get(container_num)

            if not existing or not any(existing.values()):
                enriched[container_num] = event_data
            else:
                if not existing.get("date") and event_data["date"]:
                    existing["date"] = event_data["date"]
                if not existing.get("operation") and event_data["operation"]:
                    existing["operation"] = event_data["operation"]
                if not existing.get("location") and event_data["location"]:
                    existing["location"] = event_data["location"]
                if not existing.get("remainingDistance") and event_data[
                    "remainingDistance"
                ]:
                    existing["remainingDistance"] = event_data["remainingDistance"]

        return enriched

    def _data_unchanged(self, cached_data: dict, current_data: dict) -> bool:
        """Проверка изменения данных"""
        try:
            return cached_data == current_data
        except:
            return False
    
    async def _log_final_statistics(self) -> None:
        """Вывод финальной статистики workflow"""
        
        # НОВОЕ: Получаем статистику из Firebird менеджера
        firebird_stats = await self.firebird_manager.get_entity_statistics()
        
        self.logger.info("="*60)
        self.logger.info("📊 ФИНАЛЬНАЯ СТАТИСТИКА WORKFLOW")
        self.logger.info("="*60)
        self.logger.info(f"📦 Контейнеров загружено:     {self.engine_stats.containers_loaded:,}")
        self.logger.info(f"⚙️ Контейнеров обработано:    {self.engine_stats.containers_processed:,}")
        self.logger.info(f"✅ Успешно обработано:        {self.engine_stats.containers_successful:,}")
        self.logger.info(f"📋 Заявок обнаружено:         {self.engine_stats.orders_discovered:,}")
        self.logger.info(f"📡 Заявок обработано:         {self.engine_stats.orders_processed:,}")
        self.logger.info(f"⚡ API запросов сэкономлено:   {self.engine_stats.api_calls_saved:,}")
        self.logger.info(f"🔥 Обновлений Firebird:       {self.engine_stats.firebird_updates:,}")
        self.logger.info(f"📊 Батчей прочитано:          {self.engine_stats.firebird_read_batches:,}")
        
        # Расчет эффективности
        if self.engine_stats.containers_loaded > 0:
            success_rate = (self.engine_stats.containers_successful / self.engine_stats.containers_loaded) * 100
            self.logger.info(f"📈 Процент успеха:             {success_rate:.1f}%")
        
        if self.engine_stats.api_calls_saved > 0:
            total_potential_calls = self.engine_stats.containers_loaded * 2  # order + container запросы
            efficiency = (self.engine_stats.api_calls_saved / total_potential_calls) * 100
            self.logger.info(f"⚡ Эффективность кэша:         {efficiency:.1f}%")
        
        # НОВОЕ: Статистика Firebird
        if firebird_stats:
            runtime_stats = firebird_stats.get('runtime_stats', {})
            if runtime_stats:
                self.logger.info(f"🔥 Firebird операций:          {runtime_stats.get('totals', {}).get('records_updated', 0):,}")
                
        self.logger.info("="*60)


# =============================================================================
# ФАБРИЧНАЯ ФУНКЦИЯ - ОБНОВЛЕННАЯ
# =============================================================================

async def create_container_tracking_engine(
    config: Config,
    cache: CacheBackend,
    firebird_config: Optional[dict] = None,
    entity_config: Optional[EntityTableConfig] = None
) -> ContainerTrackingEngine:
    """
    НОВОЕ: Создать и инициализировать engine с Firebird интеграцией
    
    Args:
        config: Основная конфигурация приложения
        cache: Кэш для операций
        firebird_config: Конфигурация Firebird (если None - берется из config)
        entity_config: Конфигурация entity таблицы
        
    Returns:
        Готовый к работе ContainerTrackingEngine
        
    Example:
        >>> config = load_config()
        >>> cache = create_cache()
        >>> 
        >>> engine = await create_container_tracking_engine(config, cache)
        >>> stats = await engine.run_full_workflow(batch_size=100)
    """
    
    # Используем конфигурацию из основного config, если не передана отдельно
    if firebird_config is None:
        firebird_config = config.database.to_firebird_config()
    
    # Создаем Firebird менеджер
    firebird_manager = create_firebird_entity_manager(
        host=firebird_config['host'],
        database=firebird_config['database'],
        user=firebird_config['user'],
        password=firebird_config['password'],
        entity_config=entity_config
    )
    
    # Тестируем подключение
    if not await firebird_manager.test_connection():
        raise RuntimeError(f"❌ Не удается подключиться к Firebird: {firebird_config['host']}")
    
    # Создаем engine
    engine = ContainerTrackingEngine(config, cache, firebird_manager)
    
    return engine
