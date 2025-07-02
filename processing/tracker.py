# processing/tracker_log.py
import asyncio
import time
from typing import List, AsyncGenerator
import aiohttp

from config.settings import Config
from cache.cache_base import CacheBackend
from models.container_event import TrackingResult
from models.processing_stats import ProcessingStats
from api.api_client import FescoApiClient, FescoApiError
from .events import EventProcessor

# НОВОЕ: Импорт логирования
from utils.logging import get_logger, log_execution_time


class ContainerTracker:
    """
    Главный класс для трекинга контейнеров
    
    Координирует работу всех компонентов:
    - API клиента
    - Обработчика событий  
    - Кэша
    - Статистики
    """
    
    def __init__(self, config: Config, cache: CacheBackend):
        self.config = config
        self.cache = cache
        self.stats = ProcessingStats()
        
        # НОВОЕ: Создаем логгер для трекера
        self.logger = get_logger("fesco_tracker.tracker")
        
        # Инициализация компонентов
        self.api_client = FescoApiClient(config, cache, self.stats)
        self.event_processor = EventProcessor()
        
        # Логируем инициализацию
        self.logger.info(f"🚀 ContainerTracker initialized")
        self.logger.debug(f"⚡ Parallels: {config.api.max_parallel}")
        self.logger.debug(f"⏱️ Timeout: {config.api.timeout_seconds}s")
        self.logger.debug(f"🔄 Retries: {config.api.max_retries}")
    
    @log_execution_time()
    async def track_single_container(
        self, 
        session: aiohttp.ClientSession, 
        container_number: str
    ) -> TrackingResult:
        """
        Трекинг одного контейнера

        Args:
            session: HTTP сессия
            container_number: Номер контейнера для трекинга
        
        Returns:
            Результат трекинга контейнера
        """
        
        # Создаем контекстный логгер для этого контейнера
        container_logger = get_logger(f"fesco_tracker.container.{container_number}")
        container_logger.info("🚀 Starting tracking")
        
        result = TrackingResult(container_number=container_number)
        
        try:
            # Шаг 1: Найти номер заявки по контейнеру
            container_logger.debug("🔍 Шаг 1: Поиск заявки")
            order_id = await self.api_client.find_order_by_container(session, container_number)
            
            if not order_id:
                container_logger.warning("⚠️ Заявка не найдена")
                result.error_message = "Заявка не найдена"
                self.stats.failed_tracks += 1
                return result
            
            result.order_id = order_id
            container_logger.info(f"✅ Заявка найдена: {order_id}")
            
            # Шаг 2: Параллельно получить данные трекинга
            container_logger.debug("📡 Шаг 2: Получение данных трекинга")
            
            order_task = self.api_client.get_order_tracking(session, order_id)
            container_task = self.api_client.get_container_tracking(session, order_id, container_number)
            
            order_data, container_data = await asyncio.gather(
                order_task, container_task, return_exceptions=True
            )
            
            # Проверка на ошибки в запросах
            if isinstance(order_data, Exception):
                container_logger.error(f"❌ Ошибка получения данных заявки: {order_data}")
                order_data = None
            else:
                container_logger.debug("✅ Данные заявки получены")
                
            if isinstance(container_data, Exception):
                container_logger.error(f"❌ Ошибка получения данных контейнера: {container_data}")
                container_data = None
            else:
                container_logger.debug("✅ Данные контейнера получены")
            
            # Шаг 3: Извлечение событий из данных
            container_logger.debug("📋 Шаг 3: Извлечение событий")
            
            order_events = []
            container_events = []
            
            if order_data:
                order_events = self.event_processor.extract_order_events(
                    order_data, order_id, container_number
                )
                container_logger.debug(f"📊 Извлечено {len(order_events)} событий из заявки")
            
            if container_data:
                container_events = self.event_processor.extract_container_events(container_data)
                container_logger.debug(f"📊 Извлечено {len(container_events)} событий из контейнера")
            
            # Шаг 4: Объединение и дедупликация событий
            container_logger.debug("🔄 Шаг 4: Объединение и дедупликация")
            
            final_event, has_duplicates, source = self.event_processor.merge_and_deduplicate(
                order_events, container_events
            )
            
            # Заполнение результата
            result.last_event = final_event
            result.has_duplicates = has_duplicates
            result.events_source = source
            
            # Обновление статистики
            if has_duplicates:
                self.stats.deduplicated_events += 1
                container_logger.info(f"🔄 Обнаружены дубликаты, источник: {source}")
            
            if result.success:
                self.stats.successful_tracks += 1
                container_logger.info(
                    f"✅ Трекинг успешен | Источник: {source} | "
                    f"Операция: {final_event.operation if final_event else 'N/A'}"
                )
            else:
                self.stats.failed_tracks += 1
                container_logger.warning("⚠️ Нет валидных событий")
            
            return result
            
        except FescoApiError as e:
            # Ошибки API
            result.error_message = f"API Error: {e}"
            self.stats.failed_tracks += 1
            container_logger.error(f"❌ API ошибка: {e}")
            return result
            
        except Exception as e:
            # Неожиданные ошибки
            result.error_message = f"Unexpected error: {e}"
            self.stats.failed_tracks += 1
            container_logger.error(f"❌ Неожиданная ошибка: {e}")
            return result
    
    @log_execution_time()
    async def track_containers(self, container_numbers: List[str]) -> AsyncGenerator[TrackingResult, None]:
        """
        Трекинг списка контейнеров с поддержкой параллельной обработки
        
        Args:
            container_numbers: Список номеров контейнеров
        
        Yields:
            TrackingResult: Результат трекинга для каждого контейнера
        """
        
        # Инициализация статистики
        self.stats.total_containers = len(container_numbers)
        self.stats.start_time = time.perf_counter()
        
        self.logger.info(f"🚀 Запуск трекинга {len(container_numbers)} контейнеров")
        self.logger.debug(f"📋 Список контейнеров: {', '.join(container_numbers[:5])}{'...' if len(container_numbers) > 5 else ''}")
        
        # Настройка HTTP клиента
        connector = aiohttp.TCPConnector(
            limit_per_host=5, 
            keepalive_timeout=60,
            enable_cleanup_closed=True
        )
        timeout = aiohttp.ClientTimeout(total=self.config.api.timeout_seconds)
        semaphore = asyncio.Semaphore(self.config.api.max_parallel)
        
        self.logger.debug(f"🔧 HTTP настройки: limit_per_host=5, timeout={self.config.api.timeout_seconds}s")
        
        async def track_with_semaphore(container_num: str) -> TrackingResult:
            """Трекинг с ограничением параллельности"""
            async with semaphore:
                return await self.track_single_container(session, container_num)
        
        try:
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                
                self.logger.debug("✅ HTTP сессия создана")
                
                # Создание задач для всех контейнеров
                tasks = [
                    track_with_semaphore(container_num) 
                    for container_num in container_numbers
                ]
                
                # Возвращение результатов по мере готовности
                completed_count = 0
                for completed_task in asyncio.as_completed(tasks):
                    try:
                        result = await completed_task
                        completed_count += 1
                        
                        # Логирование прогресса
                        progress = f"[{completed_count}/{len(container_numbers)}]"
                        percent = (completed_count / len(container_numbers)) * 100
                        status = "✅" if result.success else "❌"
                        
                        self.logger.info(
                            f"{progress} ({percent:.1f}%) {status} {result.container_number}"
                        )
                        
                        yield result
                        
                    except Exception as e:
                        # Это не должно происходить, но на всякий случай
                        self.logger.error(f"❌ Критическая ошибка в task: {e}")
                        completed_count += 1
                        
                        # Создаем результат с ошибкой
                        error_result = TrackingResult(container_number="UNKNOWN")
                        error_result.error_message = f"Task error: {e}"
                        yield error_result
        
        except Exception as e:
            self.logger.error(f"❌ Критическая ошибка в трекере: {e}")
            raise
        
        finally:
            # Финализация статистики
            self.stats.end_time = time.perf_counter()
            
            # Детальная статистика
            success_rate = (self.stats.successful_tracks / self.stats.total_containers * 100) if self.stats.total_containers > 0 else 0
            
            self.logger.info("="*60)
            self.logger.info("📊 ИТОГОВАЯ СТАТИСТИКА:")
            self.logger.info(f"📦 Всего контейнеров: {self.stats.total_containers}")
            self.logger.info(f"✅ Успешно: {self.stats.successful_tracks} ({success_rate:.1f}%)")
            self.logger.info(f"❌ Ошибки: {self.stats.failed_tracks}")
            self.logger.info(f"💾 Из кэша: {self.stats.cached_requests}")
            self.logger.info(f"🔄 Дедупликаций: {self.stats.deduplicated_events}")
            self.logger.info(f"⏱️ Время выполнения: {self.stats.duration:.2f} сек")
            self.logger.info(f"⚡ Скорость: {self.stats.total_containers / self.stats.duration:.2f} контейнеров/сек")
            self.logger.info("="*60)
            
            # Закрытие ресурсов
            await self.cache.close()
            self.logger.debug("🔒 Кэш закрыт")
    
    async def track_containers_batch(
        self, 
        container_numbers: List[str], 
        batch_size: int = 50
    ) -> AsyncGenerator[List[TrackingResult], None]:
        """
        Трекинг контейнеров батчами для больших списков
        
        Args:
            container_numbers: Список номеров контейнеров
            batch_size: Размер батча
        
        Yields:
            List[TrackingResult]: Результаты трекинга для батча контейнеров
        """
        
        total_batches = (len(container_numbers) + batch_size - 1) // batch_size
        
        self.logger.info(f"📦 Батчевая обработка: {len(container_numbers)} контейнеров в {total_batches} батчах по {batch_size}")
        
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(container_numbers))
            batch = container_numbers[start_idx:end_idx]
            
            self.logger.info(f"📦 Обработка батча {batch_num + 1}/{total_batches}: {len(batch)} контейнеров")
            
            batch_results = []
            async for result in self.track_containers(batch):
                batch_results.append(result)
            
            yield batch_results
            
            # Небольшая пауза между батчами для снижения нагрузки на API
            if batch_num < total_batches - 1:
                pause_seconds = self.config.processing.pause_between_batches
                self.logger.debug(f"⏸️ Пауза между батчами: {pause_seconds} сек")
                await asyncio.sleep(pause_seconds)