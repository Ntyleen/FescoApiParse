# -*- coding: utf-8 -*-
"""main.py — единый скрипт для трекинга FESCO с улучшенной логикой

Новые возможности:
* Проверка дублирования событий между order и container API
* Кэширование данных для избежания повторных запросов
* Улучшенная обработка ошибок
* Статистика выполнения
* Возможность фильтрации по датам
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from dataclasses import dataclass, asdict
from typing import Any, AsyncGenerator, List, Dict, Tuple, Optional
from datetime import datetime, timedelta
from pathlib import Path
import time
import hashlib

import aiohttp
from dotenv import load_dotenv

# ------------------------------------------------------------
# 0. Логи и статистика
# ------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

@dataclass
class ProcessingStats:
    """Статистика выполнения"""
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    cached_requests: int = 0
    deduplicated_events: int = 0
    start_time: float = 0
    end_time: float = 0
    
    @property
    def duration(self) -> float:
        return self.end_time - self.start_time
    
    def __str__(self) -> str:
        return (f"Stats: {self.successful_requests}/{self.total_requests} успешно, "
                f"{self.failed_requests} ошибок, {self.cached_requests} из кэша, "
                f"{self.deduplicated_events} дедуплицировано, {self.duration:.2f}s")

def log_time(fn):
    """Декоратор: выводит, сколько выполнялась функция."""
    if asyncio.iscoroutinefunction(fn):
        async def wrapper(*args, **kwargs):
            start = time.perf_counter()
            result = await fn(*args, **kwargs)
            dur = time.perf_counter() - start
            logging.info("%-24s %.3f s", fn.__name__, dur)
            return result
        return wrapper
    else:
        def wrapper(*args, **kwargs):
            start = time.perf_counter()
            result = fn(*args, **kwargs)
            dur = time.perf_counter() - start
            logging.info("%-24s %.3f s", fn.__name__, dur)
            return result
        return wrapper

# ------------------------------------------------------------
# 1. Конфигурация и константы
# ------------------------------------------------------------
load_dotenv(dotenv_path='E:\Repositories\FescoApiParse\deploy\.env')

trackingUrl: str = "https://api.fesco.com/api/v1/lk/tracking/"
tokenType: str = os.getenv("FESCO_TOKEN_TYPE")
authToken: str = os.getenv("FESCO_TOKEN")

if not tokenType or not authToken:
    raise EnvironmentError("Не найден TOKEN/TYPE в переменных окружения")

TIMEOUT = aiohttp.ClientTimeout(total=15)
CACHE_DIR = Path("cache")
CACHE_DIR.mkdir(exist_ok=True)

# ------------------------------------------------------------
# 2. dataclass-структуры
# ------------------------------------------------------------
@dataclass(slots=True)
class TrackRequest:
    """Входная пара: orderNumber + containerNumber"""
    orderNumbers: str
    containerNumbers: str
    
    @property
    def cache_key(self) -> str:
        """Ключ для кэширования"""
        return f"{self.orderNumbers}_{self.containerNumbers}"

@dataclass(slots=True)
class EventInfo:
    """Информация о событии"""
    date: str | None
    location: str | None
    text: str | None
    transport: str | None = None
    
    def __hash__(self) -> int:
        """Хэш для сравнения событий"""
        return hash((self.date, self.location, self.text))
    
    def __eq__(self, other) -> bool:
        if not isinstance(other, EventInfo):
            return False
        return (self.date == other.date and 
                self.location == other.location and 
                self.text == other.text)

@dataclass(slots=True)
class TrackInfo:
    """Выходные данные с агрегированной информацией."""
    order_number: str
    container_number: str
    
    # Унифицированное последнее событие (после дедупликации)
    last_event_date: str | None
    last_event_location: str | None
    last_event_text: str | None
    last_event_transport: str | None
    
    # Источник последнего события
    last_event_source: str  # "order", "container", "merged"
    
    # Дополнительная информация
    has_duplicates: bool = False
    processing_timestamp: str = ""
    
    def __post_init__(self):
        self.processing_timestamp = datetime.now().isoformat()

# ------------------------------------------------------------
# 3. Кэширование
# ------------------------------------------------------------
class CacheManager:
    """Менеджер кэша для API запросов"""
    
    def __init__(self, cache_dir: Path, ttl_hours: int = 1):
        self.cache_dir = cache_dir
        self.ttl = timedelta(hours=ttl_hours)
    
    def _get_cache_path(self, key: str, endpoint: str) -> Path:
        """Путь к файлу кэша"""
        hash_key = hashlib.md5(f"{key}_{endpoint}".encode()).hexdigest()
        return self.cache_dir / f"{hash_key}.json"
    
    def _is_cache_valid(self, cache_path: Path) -> bool:
        """Проверка актуальности кэша"""
        if not cache_path.exists():
            return False
        
        mtime = datetime.fromtimestamp(cache_path.stat().st_mtime)
        return datetime.now() - mtime < self.ttl
    
    async def get(self, key: str, endpoint: str) -> Dict[str, Any] | None:
        """Получить данные из кэша"""
        cache_path = self._get_cache_path(key, endpoint)
        
        if not self._is_cache_valid(cache_path):
            return None
        
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            return None
    
    async def set(self, key: str, endpoint: str, data: Dict[str, Any]) -> None:
        """Сохранить данные в кэш"""
        cache_path = self._get_cache_path(key, endpoint)
        
        try:
            with open(cache_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logging.warning(f"Failed to cache data: {e}")

# ------------------------------------------------------------
# 4. Парсинг и дедупликация
# ------------------------------------------------------------
def _parse_order_events(order_json: Dict[str, Any], 
                       order_no: str, 
                       cont_no: str) -> List[EventInfo]:
    """Извлекаем все события из order API"""
    events = []
    
    for ob in order_json.get("data", []):
        if ob.get("orderNumber") != order_no:
            continue
            
        for cont in ob.get("containers", []):
            if cont.get("containerNumber") != cont_no:
                continue
                
            # Последнее событие
            last_event = cont.get("lastEvent") or {}
            if any(last_event.get(k) for k in ["date", "location", "text"]):
                events.append(EventInfo(
                    date=last_event.get("date"),
                    location=last_event.get("location"),
                    text=last_event.get("text")
                ))
    
    return events

def _parse_container_events(container_json: Dict[str, Any]) -> List[EventInfo]:
    """Извлекаем события из container API (отсортированы по дате убыванию)"""
    events = []
    
    for item in container_json.get("data", []):
        events.append(EventInfo(
            date=item.get("date"),
            location=item.get("location"),
            text=item.get("operation"),
            transport=item.get("transport")
        ))
    
    return events

def _deduplicate_and_merge(order_events: List[EventInfo], 
                          container_events: List[EventInfo]) -> Tuple[EventInfo | None, bool, str]:
    """
    Дедупликация событий:
    1. Берем последнее событие из order
    2. Если оно совпадает с первым событием из container - пропускаем
    3. Если нет - проверяем второе событие из container
    4. Если второе совпадает - берем первое из container
    5. Иначе - берем событие из order
    """
    if not order_events and not container_events:
        return None, False, "no_events"
    
    # Если есть только order события
    if order_events and not container_events:
        return order_events[0], False, "order_only"
    
    # Если есть только container события  
    if container_events and not order_events:
        return container_events[0], False, "container_only"
    
    # Основная логика дедупликации
    order_last = order_events[0]
    container_first = container_events[0]
    
    # Проверяем совпадение с первым событием container
    if order_last == container_first:
        # Совпадает - не записываем дубликат
        return None, True, "duplicate_first"
    
    # Проверяем второе событие container (если есть)
    if len(container_events) > 1:
        container_second = container_events[1]
        if order_last == container_second:
            # Совпадает со вторым - берем первое из container
            return container_first, True, "merged_with_second"
    
    # Не совпадает - берем событие из order
    return order_last, False, "order_priority"

def transform_with_deduplication(order_json: Dict[str, Any],
                                container_json: Dict[str, Any],
                                order_no: str,
                                cont_no: str) -> TrackInfo:
    """Преобразование с дедупликацией событий"""
    
    order_events = _parse_order_events(order_json, order_no, cont_no)
    container_events = _parse_container_events(container_json)
    
    final_event, has_duplicates, source = _deduplicate_and_merge(order_events, container_events)
    
    if final_event:
        return TrackInfo(
            order_number=order_no,
            container_number=cont_no,
            last_event_date=final_event.date,
            last_event_location=final_event.location,
            last_event_text=final_event.text,
            last_event_transport=final_event.transport,
            last_event_source=source,
            has_duplicates=has_duplicates
        )
    else:
        return TrackInfo(
            order_number=order_no,
            container_number=cont_no,
            last_event_date=None,
            last_event_location=None,
            last_event_text=None,
            last_event_transport=None,
            last_event_source=source,
            has_duplicates=has_duplicates
        )

# ------------------------------------------------------------
# 5. Улучшенные сетевые операции
# ------------------------------------------------------------
class FescoApiClient:
    """Клиент для работы с FESCO API"""
    
    def __init__(self, base_url: str, token_type: str, auth_token: str):
        self.base_url = base_url
        self.headers = {
            "Authorization": f"{token_type} {auth_token}",
            "Accept": "application/json",
        }
        self.cache = CacheManager(CACHE_DIR)
        self.stats = ProcessingStats()
    
    async def _fetch_json_with_cache(self, session: aiohttp.ClientSession, 
                                   path: str, cache_key: str, **params) -> Dict[str, Any]:
        """GET с кэшированием"""
        # Пробуем кэш
        cached = await self.cache.get(cache_key, path)
        if cached:
            self.stats.cached_requests += 1
            logging.debug(f"Cache hit for {cache_key}:{path}")
            return cached
        
        # Запрос к API
        try:
            async with session.get(path, params=params) as response:
                response.raise_for_status()
                data = await response.json()
                
                # Сохраняем в кэш
                await self.cache.set(cache_key, path, data)
                return data
                
        except Exception as e:
            logging.error(f"API request failed for {path}: {e}")
            raise
    
    async def get_tracking_data(self, session: aiohttp.ClientSession, 
                              req: TrackRequest) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """Получение данных трекинга для пары заявка-контейнер"""
        cache_key = req.cache_key
        
        order_task = self._fetch_json_with_cache(
            session, "fit", cache_key, numbers=req.orderNumbers
        )
        container_task = self._fetch_json_with_cache(
            session, "fit/container", cache_key,
            orderNumber=req.orderNumbers,
            containerNumber=req.containerNumbers
        )
        
        return await asyncio.gather(order_task, container_task)

# ------------------------------------------------------------
# 6. Основной генератор с улучшенной обработкой ошибок
# ------------------------------------------------------------
async def track_many_improved(requests: List[TrackRequest],
                            token_type: str,
                            auth_token: str,
                            parallel: int = 10,
                            max_retries: int = 3) -> AsyncGenerator[TrackInfo, None]:
    """Улучшенный генератор для обработки запросов"""
    
    client = FescoApiClient(trackingUrl, token_type, auth_token)
    client.stats.total_requests = len(requests)
    client.stats.start_time = time.perf_counter()
    
    connector = aiohttp.TCPConnector(limit_per_host=5, keepalive_timeout=60)
    sem = asyncio.Semaphore(parallel)
    
    async with aiohttp.ClientSession(
        base_url=trackingUrl,
        headers=client.headers,
        connector=connector,
        timeout=TIMEOUT
    ) as session:
        
        async def worker_with_retry(req: TrackRequest) -> TrackInfo | None:
            """Обработчик с повторными попытками"""
            async with sem:
                for attempt in range(max_retries):
                    try:
                        order_json, container_json = await client.get_tracking_data(session, req)
                        result = transform_with_deduplication(
                            order_json, container_json, 
                            req.orderNumbers, req.containerNumbers
                        )
                        
                        if result.has_duplicates:
                            client.stats.deduplicated_events += 1
                        
                        client.stats.successful_requests += 1
                        return result
                        
                    except Exception as e:
                        if attempt == max_retries - 1:
                            logging.error(f"Failed to process {req.cache_key} after {max_retries} attempts: {e}")
                            client.stats.failed_requests += 1
                            return None
                        else:
                            await asyncio.sleep(2 ** attempt)  # Exponential backoff
                
                return None
        
        # Запускаем все задачи
        tasks = [worker_with_retry(req) for req in requests]
        
        # Возвращаем результаты по мере готовности
        for task in asyncio.as_completed(tasks):
            result = await task
            if result is not None:
                yield result
    
    client.stats.end_time = time.perf_counter()
    logging.info(str(client.stats))

# ------------------------------------------------------------
# 7. Утилиты
# ------------------------------------------------------------
def build_requests(mapping: Dict[str, List[str]]) -> List[TrackRequest]:
    """Преобразуем dict{'ORD1':[c1,c2]} → list[TrackRequest]."""
    return [TrackRequest(order, cont)
            for order, conts in mapping.items()
            for cont in conts]

def filter_by_date(results: List[TrackInfo], 
                  min_date: Optional[str] = None,
                  max_date: Optional[str] = None) -> List[TrackInfo]:
    """Фильтрация результатов по дате"""
    if not min_date and not max_date:
        return results
    
    filtered = []
    for item in results:
        if not item.last_event_date:
            continue
            
        try:
            event_date = datetime.fromisoformat(item.last_event_date.replace('Z', '+00:00'))
            
            if min_date and event_date < datetime.fromisoformat(min_date):
                continue
            if max_date and event_date > datetime.fromisoformat(max_date):
                continue
                
            filtered.append(item)
        except ValueError:
            # Если дата не парсится, включаем элемент
            filtered.append(item)
    
    return filtered

@log_time
async def run_improved(mapping: Dict[str, List[str]], 
                      outfile: str = "tracking_many.json",
                      min_date: Optional[str] = None,
                      max_date: Optional[str] = None,
                      parallel: int = 10) -> None:
    """Улучшенная главная функция"""
    
    reqs = build_requests(mapping)
    logging.info(f"Processing {len(reqs)} requests with {parallel} parallel workers")
    
    results: List[TrackInfo] = []
    async for info in track_many_improved(reqs, tokenType, authToken, parallel):
        results.append(info)
    
    # Фильтрация по дате
    if min_date or max_date:
        results = filter_by_date(results, min_date, max_date)
        logging.info(f"After date filtering: {len(results)} records")
    
    # Сортировка
    results.sort(key=lambda x: (x.order_number, x.container_number))
    
    # Сохранение
    with open(outfile, "w", encoding="utf-8") as f:
        json.dump([asdict(i) for i in results], f, ensure_ascii=False, indent=2)
    
    # Статистика
    duplicates_count = sum(1 for r in results if r.has_duplicates)
    print(f"✅ Saved {len(results)} items -> {outfile}")
    print(f"📊 Deduplicated events: {duplicates_count}")
    print(f"🔍 Event sources: {dict(Counter(r.last_event_source for r in results))}")

# ------------------------------------------------------------
# 8. Точка входа
# ------------------------------------------------------------
if __name__ == "__main__":
    from collections import Counter
    
    # Пример данных
    ORDERS: Dict[str, List[str]] = {
        "003132855": ["TDSU6005411"]
    }
    
    # Можно задать фильтры по дате
    # min_date = "2024-01-01T00:00:00"
    # max_date = "2024-12-31T23:59:59"
    
    asyncio.run(run_improved(
        ORDERS, 
        outfile="tracking_improved.json",
        parallel=5  # Можно настроить количество параллельных запросов
    ))