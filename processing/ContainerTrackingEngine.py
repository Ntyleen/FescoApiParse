# processing/ContainerTrackingEngine.py
"""
Координатор workflow для полной реализации с прямой интеграцией Firebird
"""

import asyncio
import time
from typing import List, Dict, Set, AsyncGenerator, Optional, Union, Tuple, Any
import aiohttp
from dataclasses import dataclass

from config.settings import Config
from cache.cache_base import CacheBackend
from api.api_client import FescoApiClient
from processing.container_bindings import ContainerBindingManager
from processing.events import EventProcessor
from utils.container_utils import normalize_container_number

# НОВОЕ: Прямой импорт Firebird компонентов
from utils.db.firebird_manager import (
    FirebirdEntityManager, 
    ContainerInfo,
    EntityTableConfig,
    create_firebird_entity_manager
)

from models.container_event import TrackingResult, ContainerEvent
from models.processing_stats import ProcessingStats
from utils.logging import get_logger
from utils.metrics import CONTAINERS_PROCESSED, PROCESSING_ERRORS, PROCESSING_DURATION
from processing.google_sheets_sync import GoogleSheetsSync, create_sync_from_config


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
    # НОВОЕ: Метрики синхронизации Google Sheets
    google_rows_updated: int = 0


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
        firebird_manager: FirebirdEntityManager,  # ИЗМЕНЕНО: Один менеджер вместо двух

        google_sync: Optional["GoogleSheetsSync"] = None,

    ):
        self.config = config
        self.cache = cache
        self.firebird_manager = firebird_manager  # НОВОЕ: Единый менеджер БД
        self.google_sync = google_sync
        
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
        if self.google_sync:
            self.logger.info("📝 Google Sheets синхронизация активирована")
        else:
            self.logger.info("📝 Google Sheets синхронизация отключена")
    
    async def run_full_workflow(
        self,
        batch_size: int = 100,
        target_line_ids: Optional[Set[int]] = None,
        target_carrier_ids: Optional[Set[int]] = None
    ) -> EngineStats:
        """
        Запуск полного workflow обработки
        
        Args:
            batch_size: Размер батча для обработки контейнеров
            target_line_ids: ID линий для фильтрации (None = все линии)
            target_carrier_ids: ID ЖД перевозчиков для фильтрации
            
        Returns:
            Статистика выполнения
        """
        if target_line_ids is None:
            target_line_ids = set(self.config.database.target_line_ids)
        if target_carrier_ids is None:
            tc = getattr(self.config.database, "target_carrier_ids", [])
            target_carrier_ids = set(tc or [])

        processed_container_ids: Set[int] = set()

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
                
                self.logger.debug("▶️ Start LINE pass")
                line_selected = 0
                async for batch in self.firebird_manager.get_containers_for_processing(
                    batch_size=batch_size,
                    target_ids=target_line_ids
                ):
                    line_selected += len(batch)
                    await self._process_container_batch(session, batch)
                    processed_container_ids.update(c.id for c in batch)
                    self.engine_stats.firebird_read_batches += 1

                self.logger.debug(f"✅ Finished LINE pass: {line_selected} containers selected")

                if target_carrier_ids:
                    self.logger.debug("▶️ Start CARRIER pass")
                    carrier_selected = 0
                    async for batch in self.firebird_manager.get_containers_for_processing(
                        batch_size=batch_size,
                        target_ids=target_carrier_ids,
                        selection_column=self.firebird_manager.entity_config.railway_carrier_column
                    ):
                        carrier_selected += len(batch)
                        filtered = [c for c in batch if c.id not in processed_container_ids]
                        dedup = len(batch) - len(filtered)
                        if dedup:
                            self.logger.debug(f"🔁 Deduplicated {dedup} containers in carrier pass")
                        if not filtered:
                            continue
                        await self._process_container_batch(session, filtered)
                        processed_container_ids.update(c.id for c in filtered)
                        self.engine_stats.firebird_read_batches += 1

                    self.logger.debug(f"✅ Finished CARRIER pass: {carrier_selected} containers selected")

                await self._log_final_statistics()
                if self.google_sync:
                    await self._sync_google_sheet_from_cache()

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
        
        container_numbers = [normalize_container_number(c.container_number) for c in containers]
        self.logger.info(f"📡 Обработка заявки {order_id}: {len(containers)} контейнеров")
        
        try:
            # Получаем данные заявки одним запросом для всех контейнеров
            order_data = await self.api_client.get_order_tracking(session, order_id)
            
            # Проверяем кэш на изменения
            cache_key = f"order_last_check:{order_id}"
            last_check_data = await self.cache.get(cache_key)

            # Извлекаем текущие данные для сравнения
            current_order_summary = self._extract_order_summary(order_data, container_numbers)

            # Проверяем, есть ли в БД данные с большим remaining_distance
            force_update = False
            if last_check_data:
                for container in containers:
                    norm_cn = normalize_container_number(container.container_number)
                    cached_rem = int(
                        last_check_data.get(norm_cn, {}).get("remainingDistance")
                        or 0
                    )
                    if (
                        container.remaining_distance is not None
                        and container.remaining_distance != cached_rem
                    ):
                        self.logger.debug(
                            f"🔁 Force update for order {order_id}: container {container.container_number} has DB remaining distance {container.remaining_distance} != cached {cached_rem}"
                        )
                        force_update = True
                        break

            if last_check_data and self._data_unchanged(last_check_data, current_order_summary) and not force_update:
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
            
            start_time = time.perf_counter()
            results = await asyncio.gather(
                *[process_with_semaphore(task) for task in container_tasks],
                return_exceptions=True
            )
            PROCESSING_DURATION.observe(time.perf_counter() - start_time)

            # Обрабатываем результаты с явной типизацией
            successful_results: List[Tuple[ContainerInfo, TrackingResult]] = []
            for i, result in enumerate(results):
                # Увеличиваем счетчик обработанных в любом случае
                self.engine_stats.containers_processed += 1
                CONTAINERS_PROCESSED.inc()

                if isinstance(result, Exception):
                    # Это исключение - логируем и пропускаем
                    self.logger.error(f"❌ Ошибка обработки {containers[i].container_number}: {result}")
                    PROCESSING_ERRORS.inc()
                    continue

                # Явная проверка типа для IDE
                if not isinstance(result, TrackingResult):
                    self.logger.error(f"❌ Неожиданный тип результата для {containers[i].container_number}: {type(result)}")
                    PROCESSING_ERRORS.inc()
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
            # Сохраняем все события для последующей агрегации дат
            result.events = [*order_events, *container_events]
            
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
        rows_to_sync: list[Dict[str, object]] = []
        for container_info, tracking_result in container_results:
            row_data_for_google: Optional[Dict[str, object]] = None
            try:
                events = tracking_result.events or ([] if not tracking_result.last_event else [tracking_result.last_event])
                date_in_col = self.firebird_manager.entity_config.date_in
                remaining_col = self.firebird_manager.entity_config.remaining_distance
                candidates = {}
                date_in_events = []
                for event in events:
                    if not event or not event.operation:
                        continue
                    mapping = self.firebird_manager.operation_matcher.find_best_mapping(event.operation)
                    if not mapping or mapping.entity_column == remaining_col:
                        continue
                    if not event.date:
                        continue
                    parsed = self.firebird_manager.transformer.transform_value(event.date, mapping.column_datatype)
                    if parsed is None:
                        self.logger.warning(f"⚠️ {container_info.container_number}: cannot parse '{event.date}' for {mapping.entity_column}")
                        continue
                    if mapping.entity_column == date_in_col:
                        date_in_events.append((parsed, event, mapping))
                    else:
                        cur = candidates.get(mapping.entity_column)
                        if cur is None or parsed < cur[0]:
                            candidates[mapping.entity_column] = (parsed, event, mapping)
                for column,(p_new,event,mapping) in candidates.items():
                    stored_val = container_info.current_dates.get(column)
                    p_stored = self.firebird_manager.transformer.transform_value(stored_val, mapping.column_datatype) if stored_val else None
                    if p_stored is not None and p_new >= p_stored:
                        self.logger.debug(f"⏭️ {container_info.container_number}: keep {column}={stored_val} <={event.date} id={container_info.id} src={event.operation}")
                        continue
                    tr = TrackingResult(container_number=tracking_result.container_number, last_event=ContainerEvent(date=event.date, operation=event.operation))
                    if await self.firebird_manager.update_container_from_tracking(container_info.id, tr):
                        container_info.current_dates[column] = event.date
                        written_count += 1
                        self.engine_stats.firebird_updates += 1
                if date_in_events:
                    date_in_events.sort(key=lambda x: x[0])
                    for p_new,event,mapping in date_in_events:
                        stored_val = container_info.current_dates.get(date_in_col)
                        p_stored = self.firebird_manager.transformer.transform_value(stored_val, mapping.column_datatype) if stored_val else None
                        is_do1 = event.operation in ["Регистрация ДО1","DO1 registration"]
                        do1_over = container_info.processing_flags.get("date_in_do1_overridden", False)
                        if do1_over and not is_do1:
                            self.logger.debug(f"⏭️ {container_info.container_number}: DATE_IN locked after DO1")
                            continue
                        if is_do1:
                            if do1_over:
                                self.logger.debug(f"⏭️ {container_info.container_number}: DATE_IN already overridden by DO1")
                                continue
                            container_info.processing_flags["date_in_do1_overridden"] = True
                            self.logger.debug(f"♻️ {container_info.container_number}: DATE_IN override by DO1")
                        else:
                            if p_stored is not None and p_new >= p_stored:
                                self.logger.debug(f"⏭️ {container_info.container_number}: keep {date_in_col}={stored_val} <={event.date} id={container_info.id} src={event.operation}")
                                continue
                        tr = TrackingResult(container_number=tracking_result.container_number, last_event=ContainerEvent(date=event.date, operation=event.operation))
                        if await self.firebird_manager.update_container_from_tracking(container_info.id, tr):
                            container_info.current_dates[date_in_col] = event.date
                            written_count += 1
                            self.engine_stats.firebird_updates += 1
                new_rem_raw = tracking_result.last_event.remainingDistance if tracking_result.last_event else None
                if new_rem_raw is not None:
                    new_rem = self.firebird_manager.transformer.transform_value(new_rem_raw, "INTEGER")
                    if new_rem is not None and (
                        container_info.remaining_distance is None
                        or new_rem < container_info.remaining_distance
                    ):
                        tr = TrackingResult(
                            container_number=tracking_result.container_number,
                            last_event=ContainerEvent(
                                date=None,
                                operation="",
                                remainingDistance=new_rem_raw,
                            ),
                        )
                        if await self.firebird_manager.update_container_from_tracking(container_info.id, tr):
                            container_info.remaining_distance = new_rem
                            written_count += 1
                            self.engine_stats.firebird_updates += 1
                    else:
                        self.logger.debug(
                            f"⏭️ {container_info.container_number}: remaining distance {new_rem} not lower than {container_info.remaining_distance}"
                        )
                if self.google_sync:
                    distance = new_rem_raw if new_rem_raw is not None else container_info.remaining_distance
                    try:
                        distance_val = float(distance) if distance is not None else 0.0
                    except (TypeError, ValueError):
                        distance_val = 0.0
                    row_data_for_google = {
                        "Контейнер": container_info.container_number,
                        "Отгрузка на ЖД": container_info.current_dates.get(
                            self.firebird_manager.entity_config.date_railway_loading, ""
                        ),
                        "Расстояние до станции назначения": distance_val,
                        "Станция местоположения": getattr(tracking_result.last_event, "location", "")
                        if tracking_result.last_event
                        else "",
                        "Операция": getattr(tracking_result.last_event, "operation", None)
                        if tracking_result.last_event
                        else None,
                    }
            except Exception as e:
                self.logger.error(f"❌ Ошибка записи {tracking_result.container_number}: {e}")
            else:
                if self.google_sync:
                    if row_data_for_google is None and tracking_result.last_event:
                        distance_val = 0.0
                        remaining = getattr(tracking_result.last_event, "remainingDistance", None)
                        if remaining not in (None, ""):
                            try:
                                distance_val = float(remaining)
                            except Exception:
                                distance_val = 0.0
                        row_data_for_google = {
                            "Контейнер": container_info.container_number,
                            "Отгрузка на ЖД": container_info.current_dates.get(
                                self.firebird_manager.entity_config.date_railway_loading, ""
                            ),
                            "Расстояние до станции назначения": distance_val,
                            "Станция местоположения": getattr(tracking_result.last_event, "location", "")
                            if tracking_result.last_event
                            else "",
                            "Операция": getattr(tracking_result.last_event, "operation", None)
                            if tracking_result.last_event
                            else None,
                        }
                    if row_data_for_google is not None:
                        rows_to_sync.append(row_data_for_google)
        self.engine_stats.records_written += written_count
        self.logger.info(f"💾 Обновлено {written_count}/{len(container_results)} записей в Firebird")

        if self.google_sync and rows_to_sync:
            try:
                updated = await self.google_sync.sync_rows(rows_to_sync)
                self.engine_stats.google_rows_updated += updated
            except Exception as gs_err:
                self.logger.error(f"❌ Ошибка пакетной синхронизации Google Sheets: {gs_err}")




    async def _sync_google_sheet_from_cache(self) -> None:
        containers = []
        try:
            containers = self.google_sync.ws.list_containers()
        except Exception as e:
            self.logger.error(f"❌ Ошибка чтения контейнеров из Google Sheets: {e}")
            return
        batch: list[Dict[str, object]] = []
        for container in containers:
            if not container or container == "Контейнер":
                continue
            order_id = await self.binding_manager.get_container_order(container)
            if not order_id:
                continue
            cache_key = f"order_last_check:{order_id}"
            order_summary = await self.cache.get(cache_key)
            if not order_summary:
                continue
            norm_cn = normalize_container_number(container)
            container_data = order_summary.get(norm_cn)
            if not container_data:
                continue
            row_idx = self.google_sync.ws.find_row(container)
            if row_idx is None:
                continue
            existing = self.google_sync.ws.get_row(row_idx) or {}
            rem_raw = container_data.get("remainingDistance")
            distance: Optional[float] = None
            if rem_raw not in (None, ""):
                try:
                    distance = float(rem_raw)
                except Exception:
                    self.logger.debug(
                        f"Невалидное значение remainingDistance '{rem_raw}' для {container}"
                    )
            if distance is None:
                if not existing:
                    # Не создавать новую строку с неизвестным расстоянием
                    continue
                try:
                    distance_val = float(
                        existing.get("Расстояние до станции назначения", 0)
                    )
                except Exception:
                    distance_val = 0.0
                operation = container_data.get("operation") or existing.get("Операция", "")
            else:
                distance_val = distance
                operation = container_data.get("operation", "")
            data = {
                "Контейнер": container,
                "Отгрузка на ЖД": existing.get("Отгрузка на ЖД", ""),
                "Дата обновления слежения": container_data.get("date", ""),
                "Операция": operation,
                "Расстояние до станции назначения": distance_val,
                "Станция местоположения": container_data.get("location", ""),
            }
            batch.append(data)

        if batch:
            try:
                await self.google_sync.sync_rows(batch)
            except Exception as e:
                self.logger.error(
                    f"❌ Ошибка синхронизации контейнеров ({len(batch)} записей): {e}"
                )

    def _extract_order_summary(self, order_data: dict, container_numbers: List[str]) -> dict:
        """Извлечь краткую сводку заявки для проверки изменений"""
        
        summary = {}
        normalized_numbers = {normalize_container_number(num) for num in container_numbers}

        try:
            for order_item in order_data.get("data", []):
                for container in order_item.get("containers", []):
                    container_num = normalize_container_number(
                        container.get("containerNumber", "")
                    )
                    if container_num in normalized_numbers:
                        last_event = container.get("lastEvent", {})
                        date = last_event.get("date")
                        operation = last_event.get("text")
                        location = last_event.get("location")
                        remaining = last_event.get("remainingDistance")
                        summary[container_num] = {
                            "date": "" if date is None else str(date).strip(),
                            "operation": "" if operation is None else str(operation).strip(),
                            "location": "" if location is None else str(location).strip(),
                            "remainingDistance": "" if remaining is None else str(remaining).strip(),
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
            container_num = normalize_container_number(
                container_info.container_number
            )
            if not result.last_event:
                continue
            last_event = result.last_event
            date = last_event.date
            operation = last_event.operation
            location = last_event.location
            remaining = getattr(last_event, "remainingDistance", None)
            event_data = {
                "date": "" if date is None else str(date).strip(),
                "operation": "" if operation is None else str(operation).strip(),
                "location": "" if location is None else str(location).strip(),
                "remainingDistance": "" if remaining is None else str(remaining).strip(),
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
                if existing.get("remainingDistance") != event_data[
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
        if self.google_sync:
            self.logger.info(
                f"📝 Обновлений Google Sheets:    {self.engine_stats.google_rows_updated:,}"
            )
        
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
    
    logger = get_logger("processing.engine_factory")

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

    google_sync: Optional[GoogleSheetsSync] = None
    gs_cfg = getattr(config, "google_sheets", None)
    if gs_cfg is not None:
        sheet_id = getattr(gs_cfg, "sheet_id", "").strip()
        client_secret = getattr(gs_cfg, "client_secret_file", "").strip()
        if sheet_id and client_secret:
            logger.info(
                "📝 Настройка интеграции с Google Sheets для листа %s", gs_cfg.worksheet
            )
            try:
                google_sync = await asyncio.to_thread(create_sync_from_config, gs_cfg)
                logger.info("✅ Google Sheets интеграция активирована")
            except ImportError as exc:
                logger.warning(
                    "⚠️ Google Sheets интеграция недоступна: %s", exc
                )
            except Exception as exc:
                logger.error(
                    "⚠️ Не удалось инициализировать Google Sheets: %s", exc
                )
        elif sheet_id or client_secret:
            logger.warning(
                "⚠️ Google Sheets конфигурация неполная — интеграция отключена"
            )

    # Создаем engine
    engine = ContainerTrackingEngine(
        config,
        cache,
        firebird_manager,
        google_sync=google_sync,
    )

    return engine
