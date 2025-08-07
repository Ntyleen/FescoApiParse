from datetime import datetime
from dataclasses import dataclass, field
from typing import List


@dataclass(slots=True)
class ContainerEvent:
    """Событие трекинга контейнера"""
    date: str | None = None
    type: str | None = None
    location: str | None = None
    operation: str | None = None
    transport: str | None = None
    remainingDistance: str | None = None
    
    def is_empty(self) -> bool:
        """Проверяет, пустое ли событие"""
        return not any([self.date, self.location, self.operation])
    
    def matches(self, other: 'ContainerEvent') -> bool:
        """Проверяет совпадение событий для дедупликации"""
        return (self.date == other.date and 
                self.location == other.location and 
                self.operation == other.operation)

@dataclass(slots=True)
class TrackingResult:
    """Результат трекинга контейнера"""
    container_number: str
    order_id: str | None = None

    # Список всех событий
    events: List[ContainerEvent] = field(default_factory=list)
    
    # Основное событие
    last_event: ContainerEvent | None = None

    # Метаданные
    events_source: str = "unknown"  # "order", "container", "merged", "no_events"
    has_duplicates: bool = False
    processing_time: str = ""
    error_message: str | None = None
    
    def __post_init__(self):
        self.processing_time = datetime.now().isoformat()

    @property
    def success(self) -> bool:
        """Успешно ли обработан контейнер"""
        return (
            self.error_message is None
            and self.last_event is not None
            and not self.last_event.is_empty()
        )

