# processing/events.py
from typing import Dict, Any, List, Tuple

from models.container_event import ContainerEvent

# НОВОЕ: Импорт логирования
from utils.logging import get_logger


class EventProcessor:
    """
    Обработчик событий трекинга контейнеров

    Отвечает за:
    - Извлечение событий из API ответов
    - Дедупликацию событий
    - Объединение данных из разных источников
    """
    
    def __init__(self):
        # НОВОЕ: Создаем логгер для обработки событий
        self.logger = get_logger("fesco_tracker.events")
        self.logger.debug("🔄 EventProcessor инициализирован")
    
    def extract_order_events(
        self, 
        order_data: Dict[str, Any], 
        order_id: str, 
        container_number: str
    ) -> List[ContainerEvent]:
        """
        Извлечение событий из данных заявки
        
        Args:
            order_data: Данные заявки от API
            order_id: Номер заявки
            container_number: Номер контейнера
        
        Returns:
            Список событий контейнера
        """
        
        self.logger.debug(f"📦 Извлечение order events для {container_number}")
        
        events = []
        
        try:
            data_items = order_data.get("data", [])
            self.logger.debug(f"📊 Получено {len(data_items)} элементов данных заявки")
            
            for order_item in data_items:
                # Проверяем соответствие номера заявки
                item_order_id = str(order_item.get("orderNumber", ""))
                if item_order_id != str(order_id):
                    self.logger.debug(f"⏭️ Пропускаем заявку {item_order_id} (ищем {order_id})")
                    continue
                
                # Ищем нужный контейнер
                containers = order_item.get("containers", [])
                self.logger.debug(f"🔍 Проверяем {len(containers)} контейнеров в заявке {order_id}")
                
                for container in containers:
                    container_num = container.get("containerNumber", "")
                    if container_num != container_number:
                        self.logger.debug(f"⏭️ Пропускаем контейнер {container_num}")
                        continue
                    
                    # Извлекаем последнее событие
                    last_event = container.get("lastEvent", {})
                    if self._is_valid_event_data(last_event):
                        event = ContainerEvent(
                            date=last_event.get("date"),
                            location=last_event.get("location"),
                            operation=last_event.get("text"),
                            remainingDistance=last_event.get("remainingDistance")
                        )
                        events.append(event)
                        
                        self.logger.debug(f"📦 Order event найден: {event.operation} в {event.location}")
                    else:
                        self.logger.debug("⚠️ Последнее событие пустое или невалидное")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка извлечения order events: {e}")
            self.logger.debug("🔍 Детали ошибки:", exc_info=True)
        
        self.logger.debug(f"✅ Извлечено {len(events)} order events")
        return events
    
    def extract_container_events(self, container_data: Dict[str, Any]) -> List[ContainerEvent]:
        """
        Извлечение событий из детализированных данных контейнера
        
        Args:
            container_data: Детализированные данные контейнера
        
        Returns:
            Список событий контейнера
        """
        
        self.logger.debug("🔍 Извлечение container events")
        
        events = []
        
        try:
            data_items = container_data.get("data", [])
            self.logger.debug(f"📊 Получено {len(data_items)} элементов детальных данных")
            
            for item in data_items:
                if self._is_valid_event_data(item):
                    event = ContainerEvent(
                        date=item.get("date"),
                        type=item.get("type"),
                        remainingDistance=item.get("remainingDistance"),
                        location=item.get("location"),
                        operation=item.get("operation"),
                        transport=item.get("transport")
                    )
                    events.append(event)
                    
                    self.logger.debug(f"🔍 Container event: {event.operation} в {event.location}")
                else:
                    self.logger.debug("⏭️ Пропускаем невалидное событие")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка извлечения container events: {e}")
            self.logger.debug("🔍 Детали ошибки:", exc_info=True)
        
        self.logger.debug(f"✅ Извлечено {len(events)} container events")
        return events
    
    def merge_and_deduplicate(
        self, 
        order_events: List[ContainerEvent], 
        container_events: List[ContainerEvent]
    ) -> Tuple[ContainerEvent | None, bool, str]:
        """
        Объединение и дедупликация событий из разных источников
        
        Args:
            order_events: События из данных заявки
            container_events: События из детализированных данных контейнера
        
        Returns:
            Tuple[итоговое_событие, есть_дубликаты, источник_данных]
        """
        
        self.logger.debug(f"🔀 Дедупликация: order={len(order_events)}, container={len(container_events)}")
        
        # Случай 1: Нет событий вообще
        if not order_events and not container_events:
            self.logger.debug("📭 Нет событий для обработки")
            return None, False, "no_events"
        
        # Случай 2: Только события заявки
        if order_events and not container_events:
            self.logger.debug("📦 Только order events доступны")
            best_event = self._get_best_event(order_events)
            self.logger.info(f"✅ Выбрано order event: {best_event.operation}")
            return best_event, False, "order"
        
        # Случай 3: Только события контейнера
        if container_events and not order_events:
            self.logger.debug("🔍 Только container events доступны")
            best_event = self._get_best_event(container_events)
            self.logger.info(f"✅ Выбрано container event: {best_event.operation}")
            return best_event, False, "container"
        
        # Случай 4: События из обоих источников - нужна дедупликация
        self.logger.debug("🔀 Объединение событий из двух источников")
        return self._merge_events(order_events, container_events)
    
    def _merge_events(
        self, 
        order_events: List[ContainerEvent], 
        container_events: List[ContainerEvent]
    ) -> Tuple[ContainerEvent, bool, str]:
        """
        Объединение событий из двух источников с дедупликацией

        Args:
            order_events: События из заявки
            container_events: События из контейнера
        
        Returns:
            Tuple[итоговое_событие, есть_дубликаты, источник]
        """
        
        order_event = self._get_best_event(order_events)
        container_event = self._get_best_event(container_events)
        
        self.logger.debug(f"📦 Order event: {order_event.operation} в {order_event.location}")
        self.logger.debug(f"🔍 Container event: {container_event.operation} в {container_event.location}")
        
        # Проверяем на дубликаты
        if order_event.matches(container_event):
            self.logger.debug("🔄 Найдены дубликаты событий")
            
            # Выбираем более подробное событие
            chosen_event = self._choose_better_event(order_event, container_event)
            
            # Логируем выбор
            if chosen_event == container_event:
                self.logger.info("✅ Выбрано container event (более детальное)")
            else:
                self.logger.info("✅ Выбрано order event")
            
            self.logger.info(f"🔄 Дедупликация: {chosen_event.operation} в {chosen_event.location}")
            return chosen_event, True, "merged"
        
        # События разные - берем из контейнера (обычно более актуальные)
        self.logger.debug("🔍 События не совпадают, выбираем container event")
        self.logger.info(f"✅ Выбрано container event: {container_event.operation}")
        return container_event, False, "container"
    
    def _get_best_event(self, events: List[ContainerEvent]) -> ContainerEvent:
        """
        Выбор лучшего события из списка с логированием
        
        Args:
            events: Список событий
        
        Returns:
            Лучшее событие (обычно самое последнее по времени)
        """
        if not events:
            self.logger.debug("📭 Список событий пуст, возвращаем пустое событие")
            return ContainerEvent()
        
        if len(events) == 1:
            self.logger.debug("📄 Единственное событие в списке")
            return events[0]
        
        # Сортируем по дате (самые новые первыми)
        # Если дата не парсится, событие идет в конец
        def date_key(event: ContainerEvent) -> tuple:
            try:
                if event.date:
                    # Предполагаем формат даты ISO или подобный
                    return (0, event.date)
            except:
                pass
            return (1, "")  # События без даты идут в конец
        
        sorted_events = sorted(events, key=date_key, reverse=True)
        best_event = sorted_events[0]
        
        self.logger.debug(f"📅 Выбрано самое свежее событие из {len(events)}: {best_event.date}")
        return best_event
    
    def _choose_better_event(
        self, 
        event1: ContainerEvent, 
        event2: ContainerEvent
    ) -> ContainerEvent:
        """
        Выбор более информативного события из двух

        Args:
            event1: Первое событие
            event2: Второе событие
        
        Returns:
            Более информативное событие
        """
        
        # Подсчитываем количество заполненных полей
        def count_fields(event: ContainerEvent) -> int:
            filled_fields = 0
            for field_name in ['date', 'type', 'location', 'operation', 'transport', 'remainingDistance']:
                if getattr(event, field_name, None):
                    filled_fields += 1
            return filled_fields
        
        fields1 = count_fields(event1)
        fields2 = count_fields(event2)
        
        self.logger.debug(f"📊 Сравнение событий: event1={fields1} полей, event2={fields2} полей")
        
        if fields2 > fields1:
            self.logger.debug(f"✅ Выбрано event2: больше данных ({fields2} vs {fields1})")
            return event2
        elif fields1 > fields2:
            self.logger.debug(f"✅ Выбрано event1: больше данных ({fields1} vs {fields2})")
            return event1
        else:
            # Если одинаково заполнены, предпочитаем событие с transport
            if event2.transport and not event1.transport:
                self.logger.debug("✅ Выбрано event2: есть информация о транспорте")
                return event2
            elif event1.transport and not event2.transport:
                self.logger.debug("✅ Выбрано event1: есть информация о транспорте")
                return event1
            else:
                self.logger.debug("✅ Выбрано event1: равнозначные события")
                return event1
    
    def _is_valid_event_data(self, event_data: Dict[str, Any]) -> bool:
        """
        Проверка валидности данных события
        
        Args:
            event_data: Данные события
        
        Returns:
            True, если событие содержит минимально необходимые данные
        """
        if not isinstance(event_data, dict):
            self.logger.debug("⚠️ Данные события не являются словарем")
            return False
        
        # Проверяем наличие хотя бы одного ключевого поля
        key_fields = ['date', 'location', 'operation', 'text']
        valid_fields = []
        
        for field in key_fields:
            value = event_data.get(field)
            if value and str(value).strip():
                valid_fields.append(field)
        
        is_valid = len(valid_fields) > 0
        
        if is_valid:
            self.logger.debug(f"✅ Валидное событие с полями: {valid_fields}")
        else:
            self.logger.debug(f"⚠️ Невалидное событие: отсутствуют ключевые поля {key_fields}")
        
        return is_valid