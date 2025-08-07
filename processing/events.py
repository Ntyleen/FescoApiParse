# processing/events.py
from typing import Dict, Any, List, Tuple

from models.container_event import ContainerEvent

from datetime import datetime

from utils.db.firebird_manager import FirebirdDateTransformer

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
        self.date_transformer = FirebirdDateTransformer()
    
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
                    container_num = container.get("containerNumber", "").strip()
                    if container_num != container_number.strip():
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
        container_events: List[ContainerEvent],
        prefer_earliest: bool = False,
    ) -> Tuple[List[ContainerEvent], bool, str]:
        """Объединение и дедупликация событий из разных источников.

        Возвращает список уникальных событий, отсортированный по дате от
        самых ранних к самым поздним.
        """

        self.logger.debug(
            f"🔀 Дедупликация: order={len(order_events)}, container={len(container_events)}"
        )

        if not order_events and not container_events:
            self.logger.debug("📭 Нет событий для обработки")
            return [], False, "no_events"

        # Определяем источник
        if order_events and container_events:
            source = "merged"
        elif order_events:
            source = "order"
        else:
            source = "container"

        events = order_events + container_events
        deduped: List[ContainerEvent] = []
        has_duplicates = False

        for event in events:
            duplicate = next((e for e in deduped if e.matches(event)), None)
            if duplicate:
                has_duplicates = True
                better = self._choose_better_event(duplicate, event)
                deduped[deduped.index(duplicate)] = better
            else:
                deduped.append(event)

        deduped = self._sort_events(deduped, prefer_earliest)

        self.logger.debug(
            f"📚 После объединения: {len(deduped)} событий, дубликаты={has_duplicates}"
        )

        return deduped, has_duplicates, source

    def _sort_events(
        self, events: List[ContainerEvent], prefer_earliest: bool
    ) -> List[ContainerEvent]:
        """Сортировка событий по дате.

        События без валидной даты помещаются в конец списка.
        """

        def date_key(event: ContainerEvent) -> tuple:
            if not event.date:
                return (1, datetime.max)
            parsed_date = self.date_transformer.transform_value(event.date, "TIMESTAMP")
            if parsed_date is None:
                try:
                    parsed_date = datetime.strptime(event.date, "%Y-%m-%d %H:%M:%S")
                except Exception:
                    return (1, datetime.max)
            return (0, parsed_date)

        sorted_events = sorted(events, key=date_key)
        return sorted_events
    
    def _get_best_event(self, events: List[ContainerEvent], prefer_earliest) -> ContainerEvent:
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
            if not event.date:
               # Предполагаем формат даты ISO или подобный
                return (1, "")
            parsed_date = self.date_transformer.transform_value(event.date, "TIMESTAMP")
            if parsed_date is None:
                try:
                    parsed_date = datetime.strptime(event.date, "%Y-%m-%d %H:%M:%S")
                except Exception:
                    return (1, event.date)

            ts = parsed_date.timestamp()        
            return (0, ts if prefer_earliest else -ts)
        
        sorted_events = sorted(events, key=date_key)
        best_event = sorted_events[0]
        
        self.logger.debug(
            "📅 Выбрано %s событие из %d: %s",
            "самое раннее" if prefer_earliest else "самое свежее",
            len(events),
            best_event.date,
        )
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
