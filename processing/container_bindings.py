# processing/container_bindings.py
"""
Модуль для работы с привязками контейнеров к заявкам
Использует Redis для быстрого доступа к привязкам
"""

import asyncio
from typing import List, Optional, Set, Dict
from cache.cache_base import CacheBackend
from utils.logging import get_logger


class ContainerBindingManager:
    """
    Менеджер привязок контейнеров к заявкам
    
    Основные функции:
    - Привязка контейнера к заявке
    - Проверка существующих привязок
    - Получение всех контейнеров заявки
    - Проверка необходимости обработки заявки
    """
    
    def __init__(self, cache: CacheBackend):
        self.cache = cache
        self.logger = get_logger("fesco_tracker.bindings")
        
        # Префиксы для ключей Redis
        self.CONTAINER_KEY_PREFIX = "container_to_order:"
        self.ORDER_KEY_PREFIX = "order_to_containers:"
        self.PROCESSED_ORDERS_KEY = "processed_orders"
        self.NO_ORDER_PREFIX = "no_order:"
        
        self.logger.debug("🔗 ContainerBindingManager инициализирован")
    
    async def bind_container_to_order(self, container_number: str, order_id: str) -> bool:
        """
        Привязать контейнер к заявке
        
        Args:
            container_number: Номер контейнера
            order_id: Номер заявки
            
        Returns:
            True если привязка успешна
        """
        
        try:
            # Проверяем, не привязан ли контейнер к другой заявке
            existing_order = await self.get_container_order(container_number)
            
            if existing_order and existing_order != order_id:
                self.logger.debug(f"🔄 Перепривязка {container_number}: {existing_order} → {order_id}")
                await self._remove_container_from_order(container_number, existing_order)
            
            # Создаем прямую привязку: контейнер → заявка
            container_key = f"{self.CONTAINER_KEY_PREFIX}{container_number}"
            await self.cache.set(container_key, {"order_id": order_id}, ttl_seconds=86400)
            
            # Создаем обратную привязку: заявка → список контейнеров
            await self._add_container_to_order_list(container_number, order_id)
            
            self.logger.info(f"🔗 Контейнер {container_number} привязан к заявке {order_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка привязки {container_number} к {order_id}: {e}")
            return False
    
    async def get_container_order(self, container_number: str) -> Optional[str]:
        """
        Получить номер заявки для контейнера
        
        Args:
            container_number: Номер контейнера
            
        Returns:
            Номер заявки или None
        """
        
        try:
            container_key = f"{self.CONTAINER_KEY_PREFIX}{container_number}"
            binding_data = await self.cache.get(container_key)
            
            if binding_data:
                order_id = binding_data.get("order_id")
                self.logger.debug(f"🔍 Найдена привязка: {container_number} → {order_id}")
                return order_id
            
            return None
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка получения привязки для {container_number}: {e}")
            return None
    
    async def get_order_containers(self, order_id: str) -> List[str]:
        """
        Получить все контейнеры заявки
        
        Args:
            order_id: Номер заявки
            
        Returns:
            Список номеров контейнеров
        """
        
        try:
            order_key = f"{self.ORDER_KEY_PREFIX}{order_id}"
            order_data = await self.cache.get(order_key)
            
            if order_data:
                containers = order_data.get("containers", [])
                self.logger.debug(f"📦 Заявка {order_id} содержит {len(containers)} контейнеров")
                return containers
            
            return []
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка получения контейнеров для заявки {order_id}: {e}")
            return []
    
    async def is_order_processed(self, order_id: str) -> bool:
        """
        Проверить, была ли заявка уже обработана
        
        Args:
            order_id: Номер заявки
            
        Returns:
            True если заявка уже обработана
        """
        
        try:
            processed_orders = await self.cache.get(self.PROCESSED_ORDERS_KEY)
            
            if processed_orders:
                order_set = set(processed_orders.get("orders", []))
                is_processed = order_id in order_set
                
                if is_processed:
                    self.logger.debug(f"⏭️ Заявка {order_id} уже обработана")
                
                return is_processed
            
            return False
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка проверки обработки заявки {order_id}: {e}")
            return False
    
    async def mark_order_processed(self, order_id: str) -> bool:
        """
        Отметить заявку как обработанную
        
        Args:
            order_id: Номер заявки
            
        Returns:
            True если успешно отмечена
        """
        
        try:
            # Получаем текущий список обработанных заявок
            processed_orders = await self.cache.get(self.PROCESSED_ORDERS_KEY) or {"orders": []}
            
            order_set = set(processed_orders.get("orders", []))
            order_set.add(order_id)
            
            # Ограничиваем размер списка (оставляем последние 1000 заявок)
            if len(order_set) > 1000:
                order_list = list(order_set)
                order_set = set(order_list[-1000:])
            
            # Сохраняем обновленный список
            updated_data = {"orders": list(order_set)}
            await self.cache.set(self.PROCESSED_ORDERS_KEY, updated_data, ttl_seconds=86400)
            
            self.logger.debug(f"✅ Заявка {order_id} отмечена как обработанная")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка отметки заявки {order_id}: {e}")
            return False

    async def mark_container_no_order(self, container_number: str) -> bool:
        """Отметить контейнер как не имеющий заявки"""
        try:
            key = f"{self.NO_ORDER_PREFIX}{container_number}"
            await self.cache.set(key, {"no_order": True}, ttl_seconds=86400)
            self.logger.debug(
                f"🚫 Контейнер {container_number} отмечен как без заявки"
            )
            return True
        except Exception as e:
            self.logger.error(
                f"❌ Ошибка отметки контейнера {container_number} без заявки: {e}"
            )
            return False

    async def is_container_no_order(self, container_number: str) -> bool:
        """Проверить, отмечен ли контейнер как не имеющий заявки"""
        try:
            key = f"{self.NO_ORDER_PREFIX}{container_number}"
            return await self.cache.exists(key)
        except Exception as e:
            self.logger.error(
                f"❌ Ошибка проверки отметки контейнера {container_number}: {e}"
            )
            return False
    
    async def should_process_container(self, container_number: str, order_id: str) -> bool:
        """
        Определить, нужно ли обрабатывать контейнер
        
        Логика согласно схеме canvas:
        1. Если контейнер не привязан - привязываем и обрабатываем
        2. Если контейнер привязан к этой заявке - проверяем обработку заявки
        3. Если заявка уже обработана - пропускаем
        
        Args:
            container_number: Номер контейнера
            order_id: Номер заявки
            
        Returns:
            True если нужно обрабатывать
        """
        
        # Сначала проверяем, не помечен ли контейнер как не имеющий заявки
        if await self.is_container_no_order(container_number):
            self.logger.debug(
                f"⏭️ Контейнер {container_number} помечен как без заявки"
            )
            return False

        # Проверяем привязку контейнера
        existing_order = await self.get_container_order(container_number)
        
        if not existing_order:
            # Контейнер не привязан - привязываем
            await self.bind_container_to_order(container_number, order_id)
            self.logger.debug(f"🆕 Новый контейнер {container_number}, нужна обработка")
            return True
        
        elif existing_order == order_id:
            # Контейнер привязан к этой заявке - проверяем обработку
            is_processed = await self.is_order_processed(order_id)
            
            if is_processed:
                self.logger.debug(f"⏭️ Заявка {order_id} уже обработана, пропускаем {container_number}")
                return False
            else:
                self.logger.debug(f"📋 Заявка {order_id} требует обработки")
                return True
        
        else:
            # Контейнер привязан к другой заявке - это странно, но обновляем привязку
            self.logger.warning(f"⚠️ Контейнер {container_number} перепривязан: {existing_order} → {order_id}")
            await self.bind_container_to_order(container_number, order_id)
            return True
    
    async def get_binding_statistics(self) -> Dict[str, int]:
        """
        Получить статистику привязок
        
        Returns:
            Словарь со статистикой
        """
        
        try:
            # Получаем список обработанных заявок
            processed_orders = await self.cache.get(self.PROCESSED_ORDERS_KEY) or {"orders": []}
            processed_count = len(processed_orders.get("orders", []))
            
            # Для полной статистики нужно было бы пройти по всем ключам Redis,
            # но это дорого. Поэтому возвращаем доступную информацию
            
            return {
                "processed_orders": processed_count,
                "cache_ttl_hours": 24  # Информация о времени жизни кэша
            }
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка получения статистики: {e}")
            return {}
    
    async def _add_container_to_order_list(self, container_number: str, order_id: str) -> None:
        """Добавить контейнер в список контейнеров заявки"""
        
        order_key = f"{self.ORDER_KEY_PREFIX}{order_id}"
        order_data = await self.cache.get(order_key) or {"containers": []}
        
        containers = order_data.get("containers", [])
        if container_number not in containers:
            containers.append(container_number)
            order_data["containers"] = containers
            
            await self.cache.set(order_key, order_data, ttl_seconds=86400)
    
    async def _remove_container_from_order(self, container_number: str, order_id: str) -> None:
        """Удалить контейнер из списка контейнеров заявки"""
        
        order_key = f"{self.ORDER_KEY_PREFIX}{order_id}"
        order_data = await self.cache.get(order_key)
        
        if order_data:
            containers = order_data.get("containers", [])
            if container_number in containers:
                containers.remove(container_number)
                order_data["containers"] = containers
                
                await self.cache.set(order_key, order_data, ttl_seconds=86400)
    
    async def cleanup_expired_bindings(self) -> int:
        """
        Очистка устаревших привязок (вспомогательный метод)
        
        Returns:
            Количество очищенных привязок
        """
        
        # Этот метод сложно реализовать эффективно с обычным кэшем,
        # так как нужно было бы перебирать все ключи.
        # В реальной реализации с Redis можно использовать SCAN
        
        self.logger.debug("🧹 Очистка устаревших привязок (автоматически через TTL)")
        return 0