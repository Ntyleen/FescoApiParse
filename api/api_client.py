# api/client.py
import asyncio
from typing import Dict, Any
import aiohttp

from config.settings import Config
from cache import CacheBackend
from models.processing_stats import ProcessingStats

from utils.logging import get_logger


class FescoApiError(Exception):
    """Базовое исключение для FESCO API"""
    pass


class AuthenticationError(FescoApiError):
    """Ошибка аутентификации"""
    pass


class ApiRequestError(FescoApiError):
    """Ошибка выполнения запроса к API"""
    pass


class FescoApiClient:
    """
    Клиент для работы с FESCO API
    Предоставляет методы для:
    - Поиска заявок по контейнерам
    - Получения данных трекинга
    - Кэширования ответов
    """
    
    def __init__(self, config: Config, cache: CacheBackend, stats: ProcessingStats):
        self.config = config
        self.cache = cache
        self.stats = stats
        
        # Создаем логгер для API клиента
        self.logger = get_logger("fesco_tracker.api")
        
        self.headers = {
            "Authorization": f"{config.api.token_type} {config.auth_token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": self._get_user_agent()
        }
        
        # Логируем инициализацию
        self.logger.info(f"🌐 FescoApiClient инициализирован")
        self.logger.debug(f"🔧 Base URL: {config.api.base_url}")
        self.logger.debug(f"⏱️ Timeout: {config.api.timeout_seconds}s")
        self.logger.debug(f"🔗 Max parallel: {config.api.max_parallel}")
    
    def _get_user_agent(self) -> str:
        """Получить User-Agent для запросов"""
        return ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36")
    
    async def _make_request(
        self, 
        session: aiohttp.ClientSession, 
        url: str, 
        cache_key: str, 
        **params
    ) -> Dict[str, Any]:
        """
        Универсальный метод для выполнения HTTP запросов с кэшированием
        
        Args:
            session: HTTP сессия
            url: URL для запроса
            cache_key: Ключ для кэширования
            **params: Параметры запроса
        
        Returns:
            Данные ответа
        
        Raises:
            AuthenticationError: При ошибке аутентификации
            ApiRequestError: При других ошибках API
        """
        
        # Логируем попытку запроса (кратко)
        endpoint = url.replace(self.config.api.base_url, "")
        self.logger.debug(f"🌐 Запрос: {endpoint}")
        
        # Проверяем кэш, но не прерываем запрос
        cached_data = await self.cache.get(cache_key)
        if cached_data:
            self.stats.cached_requests += 1
            self.logger.debug(f"💾 Cache HIT: {cache_key}")
        
        # Выполняем HTTP запрос
        try:
            self.logger.debug(f"🔗 HTTP GET {url}")
            self.logger.debug(f"📝 Params: {params}")
            
            async with session.get(
                url, 
                params=params, 
                headers=self.headers,
                timeout=aiohttp.ClientTimeout(total=self.config.api.timeout_seconds)
            ) as response:
                
                # Логируем результат запроса
                status_emoji = "✅" if response.status < 400 else "❌"
                self.logger.info(f"{status_emoji} : response: {response.status}, {endpoint}")
                
                # Обработка ошибок HTTP
                await self._handle_response_errors(response)
                
                # Проверка Content-Type
                self._validate_content_type(response)
                
                # Парсинг JSON
                data = await response.json()
                
                # Логируем размер ответа
                response_size = len(str(data))
                self.logger.debug(f"📊 Получено {response_size} символов")
                
                # Сохранение в кэш только при изменении данных
                if cached_data != data:
                    await self.cache.set(cache_key, data, int(self.config.cache.ttl_hours * 3600))
                else:
                    self.logger.debug(f"💾 Cache unchanged, skip update: {cache_key}")

                return data
                
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            error_msg = f"Ошибка сети для {endpoint}: {e}"
            self.logger.error(f"🌐 {error_msg}")
            raise ApiRequestError(error_msg) from e
        
        except Exception as e:
            error_msg = f"Неожиданная ошибка для {endpoint}: {e}"
            self.logger.error(f"💥 {error_msg}")
            raise ApiRequestError(error_msg) from e
    
    async def _handle_response_errors(self, response: aiohttp.ClientResponse) -> None:
        """Обработка ошибок HTTP ответа"""
        if response.status == 401:
            self.logger.error("🔒 Ошибка аутентификации - неверный токен")
            raise AuthenticationError("Неверный токен аутентификации")
        
        elif response.status == 403:
            self.logger.error("🚫 Доступ запрещен - проверьте права токена")
            raise AuthenticationError("Доступ запрещен. Проверьте права токена")
        
        elif response.status == 429:
            self.logger.warning("⏳ Превышен лимит запросов")
            raise ApiRequestError("Превышен лимит запросов. Попробуйте позже")
        
        elif response.status >= 500:
            error_text = await response.text()
            self.logger.error(f"🔥 Ошибка сервера FESCO: {response.status}")
            self.logger.debug(f"📄 Ответ сервера: {error_text[:200]}...")
            raise ApiRequestError(f"Ошибка сервера FESCO ({response.status}): {error_text[:200]}")
        
        elif response.status >= 400:
            error_text = await response.text()
            self.logger.error(f"⚠️ Ошибка клиента: {response.status}")
            self.logger.debug(f"📄 Ответ сервера: {error_text[:200]}...")
            raise ApiRequestError(f"Ошибка клиента ({response.status}): {error_text[:200]}")
    
    def _validate_content_type(self, response: aiohttp.ClientResponse) -> None:
        """Проверка Content-Type ответа"""
        content_type = response.headers.get('content-type', '').lower()
        if 'application/json' not in content_type:
            self.logger.error(f"📄 Неожиданный Content-Type: {content_type}")
            raise ApiRequestError(f"Ожидался JSON, получен: {content_type}")
    
    async def find_order_by_container(
        self, 
        session: aiohttp.ClientSession, 
        container_number: str
    ) -> str | None:
        """
        Поиск номера заявки по контейнеру
        
        Args:
            session: HTTP сессия
            container_number: Номер контейнера
        
        Returns:
            Номер заявки или None, если не найдена
        """
        
        # Создаем контекстный логгер для этого контейнера
        container_logger = get_logger(f"fesco_tracker.api.container.{container_number}")
        container_logger.debug("🔍 Поиск заявки по контейнеру")
        
        try:
            cache_key = f"order_lookup:{container_number}"
            url = f"{self.config.api.base_url}orders"
            
            data = await self._make_request(
                session, url, cache_key, text=container_number
            )
            
            # Парсинг ответа для получения order_id
            container_logger.debug(f"📊 Получено {len(data.get('data', []))} заявок")
            
            for item in data.get("data", []):
                order_id = item.get("orderId") or item.get("number")
                if order_id:
                    self.stats.orders_resolved += 1
                    container_logger.info(f"✅ Найдена заявка: {order_id} из {container_number}")
                    return str(order_id)
            
            container_logger.warning("⚠️ Заявка не найдена")
            return None
            
        except Exception as e:
            container_logger.error(f"❌ Ошибка поиска заявки: {e}")
            return None
    
    async def get_order_tracking(
        self, 
        session: aiohttp.ClientSession, 
        order_id: str
    ) -> Dict[str, Any]:
        """
        Получение данных трекинга по заявке
        
        Args:
            session: HTTP сессия
            order_id: Номер заявки
        
        Returns:
            Данные трекинга заявки
        """
        self.logger.debug(f"📡 Получение трекинга заявки {order_id}")
        
        cache_key = f"order_track:{order_id}"
        url = f"{self.config.api.base_url}tracking/fit"
        
        data = await self._make_request(
            session, url, cache_key, numbers=order_id
        )
        
        # Логируем количество полученных данных
        containers_count = 0
        for order_item in data.get("data", []):
            containers_count += len(order_item.get("containers", []))
        
        self.logger.debug(f"📦 Получено данных по {containers_count} контейнерам")
        
        return data
    
    async def get_container_tracking(
        self, 
        session: aiohttp.ClientSession, 
        order_id: str, 
        container_number: str
    ) -> Dict[str, Any]:
        """
        Получение детализированных данных по контейнеру
        
        Args:
            session: HTTP сессия
            order_id: Номер заявки
            container_number: Номер контейнера
        
        Returns:
            Детализированные данные контейнера
        """
        container_logger = get_logger(f"fesco_tracker.api.container.{container_number}")
        container_logger.debug(f"🔍 Получение детальных данных (заявка {order_id})")
        
        cache_key = f"container_track:{order_id}:{container_number}"
        url = f"{self.config.api.base_url}tracking/fit/container"
        
        data = await self._make_request(
            session, 
            url, 
            cache_key,
            orderNumber=order_id,
            containerNumber=container_number
        )
        
        # Логируем количество событий
        events_count = len(data.get("data", []))
        container_logger.debug(f"📊 Получено {events_count} событий")
        
        return data