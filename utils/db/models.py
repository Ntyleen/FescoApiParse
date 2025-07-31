from dataclasses import dataclass, field
from enum import IntEnum
from typing import Dict, Tuple, Set


class EntityStatusID(IntEnum):
    """Важные статусы entity как Enum."""
    NEW = 1
    LOCATION_RECEIVED = 2
    SEA = 3
    ARRIVED_AT_STATION = 4
    TERMINAL_OPERATION = 5
    WAITING_DEPARTURE = 6
    RAILWAY = 7
    TRANSPORTATION_CLOSED = 8
    DELIVERED = 9
    DOCUMENTS_RECEIVED = 12
    ATTENTION = 13
    WAITING_SHIPMENT = 15
    RAID = 16
    DIRECT_CAR = 17
    RAIL = 23
    CANCELLED = 24
    UNLOADING = 25
    CLIENT_DEBT = 26
    OUTPUT = 27
    LCL_RU = 28
    NO_AVP = 29
    RAILWAY_OUTPUT = 30
    PP_DIRECT_RAILWAY = 31
    LOCAL_ISSUANCE = 32
    AWAITS_AVAILABILITY = 33
    LOOKING_PLACE = 34
    WAITING_PLACE = 35
    TRANSHIPMET = 36
    GIVEN_FOR_CLOSE = 37
    SEA_THROUGH = 41
    RAID_THROUGH = 42
    UNLOADING_THROUGH = 43
    TERMINAL_OPERATION_THROUGH = 44
    RELEASE_THROUGH = 45
    WAITING_SHIPMENT_THROUGH = 46
    WAITING_EMPTY = 52
    AUTO_CN = 55
    AUTO_BORDER_CROSSING = 56
    TERMINAL_OPERATION_AUTO = 57
    FTL_RELEASE = 58

    @classmethod
    def get_excluded_statuses(cls) -> Set[int]:
        return {
            int(cls.TRANSPORTATION_CLOSED),
            int(cls.DELIVERED),
            int(cls.CANCELLED),
        }


@dataclass(frozen=True)
class EntityColumnMapping:
    """Связь колонки entity с полем FESCO."""

    entity_column: str
    fesco_field: str
    operation_patterns: Tuple[str, ...]
    transform_func: str = "firebird_date"
    priority: int = 0
    description: str = ""
    column_datatype: str = "DATE"

    def __post_init__(self):
        if not self.operation_patterns:
            raise ValueError("operation_patterns должен cодержать хотя бы один паттерн")

    def get_transform_datatype(self) -> str:
        if self.column_datatype == "TIMESTAMP":
            return "firebird_timestamp"
        elif self.column_datatype == "DATE":
            return "firebird_date_only"
        elif self.column_datatype == "INTEGER":
            return "firebird_integer"
        return "firebird_date"

    def matches_operation(self, operation: str) -> float:
        if not operation:
            return 0.0

        operation_lower = operation.lower().strip()
        max_score = 0.0

        for pattern in self.operation_patterns:
            pattern_lower = pattern.lower().strip()
            if pattern_lower == operation_lower:
                max_score = max(max_score, 1.0)
            elif pattern_lower in operation_lower:
                coverage = len(pattern_lower) / len(operation_lower)
                position_bonus = 0.2 if operation_lower.startswith(pattern_lower) else 0.1
                score = 0.8 * coverage + position_bonus
                max_score = max(max_score, score)
            elif operation_lower in pattern_lower:
                score = 0.6 * len(operation_lower) / len(pattern_lower)
                max_score = max(max_score, score)

        priority_bonus = min(0.1, self.priority / 100)
        return min(1.0, max_score + priority_bonus)


@dataclass
class EntityTableConfig:
    """Конфигурация entity таблицы."""

    table_name: str = "ENTITY"
    primary_key: str = "ID"
    container_column: str = "NAME"
    status_column: str = "SP_ENTITY_STATUS_ID"
    line_column: str = "LEGAL_PERSON_LINE_ID"

    date_eta: str = "DATE_ETA"
    date_etd: str = "DATE_ETD"
    date_in: str = "DATE_IN"
    date_railway_loading: str = "DATE_RAILWAY_LOADING"
    date_railway_delivery: str = "DATE_RAILWAY_DELIVERY"
    remaining_distance: str = "TRACING_DAYS"

    date_mappings: Dict[str, EntityColumnMapping] = field(default_factory=dict)
    excluded_status_ids: Set[int] = field(default_factory=lambda: EntityStatusID.get_excluded_statuses())

    def __post_init__(self):
        if not self.date_mappings:
            self.date_mappings = self._create_default_mappings()
        self._validate_config()
        self.excluded_status_ids = {int(v) for v in self.excluded_status_ids}

    def _validate_config(self):
        if not self.table_name or not self.table_name.strip():
            raise ValueError("table_name не может быть пустым")

        required_columns = [
            self.primary_key,
            self.container_column,
            self.status_column,
            self.line_column,
        ]
        for column in required_columns:
            if not column or not column.strip():
                raise ValueError(f"Обязательная колонка не может быть пустой: {column}")

    def _create_default_mappings(self) -> Dict[str, EntityColumnMapping]:
        return {
            self.date_eta: EntityColumnMapping(
                entity_column=self.date_eta,
                fesco_field="date",
                operation_patterns=["Выгружается груженным"],
                priority=10,
                description="Estimated Time of Arrival",
                column_datatype="DATE",
            ),
            self.date_etd: EntityColumnMapping(
                entity_column=self.date_etd,
                fesco_field="date",
                operation_patterns=["Грузится на фидер", "Loading Feeder Full"],
                priority=10,
                description="Estimated Time of Departure",
                column_datatype="DATE",
            ),
            self.date_in: EntityColumnMapping(
                entity_column=self.date_in,
                fesco_field="date",
                operation_patterns=[
                    "Прием с моря",
                    "Регистрация ДО1",
                    "Discharged from vessel",
                    "DO1 registration",
                ],
                priority=8,
                description="Выгрузка на терминал",
                column_datatype="TIMESTAMP",
            ),
            self.date_railway_loading: EntityColumnMapping(
                entity_column=self.date_railway_loading,
                fesco_field="date",
                operation_patterns=[
                    "Отправление вагона со станции",
                    "Wagon has left the station",
                ],
                priority=8,
                description="Отгрузка на платформу",
                column_datatype="DATE",
            ),
            self.date_railway_delivery: EntityColumnMapping(
                entity_column=self.date_railway_delivery,
                fesco_field="date",
                operation_patterns=[
                    "Документы для отправки по ЖД приняты",
                    "Documents for sending by railway accepted",
                ],
                priority=8,
                description="Сдача на ж/д",
                column_datatype="DATE",
            ),
            self.remaining_distance: EntityColumnMapping(
                entity_column=self.remaining_distance,
                fesco_field="remainingDistance",
                operation_patterns=("",),
                priority=10,
                description="Слежение",
                column_datatype="INTEGER",
            ),
        }
