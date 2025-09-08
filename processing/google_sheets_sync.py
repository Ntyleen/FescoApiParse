"""Synchronise container tracking data with Google Sheets."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Any, Optional
from zoneinfo import ZoneInfo

from utils.logging import get_logger
from utils.db.firebird_manager import FirebirdEntityManager


ALLOWED_OPERATIONS = {
    "В пути",
    "прибыл на станцию назначения",
    "простой в пути",
}


@dataclass
class SheetRow:
    container: str
    railway_loading: str
    distance: float
    station_id: Optional[int]
    operation: Optional[str] = None
    destination_station: Optional[str] = None


class SheetInterface:
    """Simple interface for sheet operations used for testing."""

    def get(self, container: str) -> Optional[Dict[str, Any]]:  # pragma: no cover - interface
        raise NotImplementedError

    def update(self, container: str, data: Dict[str, Any]) -> None:  # pragma: no cover
        raise NotImplementedError

    def add(self, data: Dict[str, Any]) -> None:  # pragma: no cover
        raise NotImplementedError


class GoogleSheetsSync:
    """Synchronise data with Google Sheets keeping business rules."""

    def __init__(self, sheet: SheetInterface, firebird: FirebirdEntityManager, timezone: str = "Asia/Vladivostok"):
        self.sheet = sheet
        self.firebird = firebird
        self.tz = ZoneInfo(timezone)
        self.logger = get_logger("google_sheets_sync")

    # ------------------------------------------------------------------
    def _today_str(self) -> str:
        return datetime.now(self.tz).date().isoformat()

    # ------------------------------------------------------------------
    async def _station_name(self, station_id: Optional[int]) -> str:
        if station_id is None:
            return ""
        name = await self.firebird.get_station_name_by_id(station_id)
        return name or ""

    def _map_operation(
        self,
        raw_operation: Optional[str],
        distance: float,
        station_name: str,
        destination: Optional[str],
        previous_distance: Optional[float] = None,
    ) -> str:
        if raw_operation:
            op = raw_operation.strip().lower()
            if op in {"в пути", "простой в пути", "прибыл на станцию назначения"}:
                return raw_operation.strip()
        # Fallback rules
        if distance == 0 and destination and station_name and station_name == destination:
            return "прибыл на станцию назначения"
        return "В пути" if distance >= 0 else "В пути"

    async def upsert_row(self, row: SheetRow) -> None:
        """Insert or update a row in the sheet according to rules."""
        existing = self.sheet.get(row.container)
        station_name = await self._station_name(row.station_id)

        today = self._today_str()
        data = {
            "Контейнер": row.container,
            "Отгрузка на ЖД": row.railway_loading,
            "Расстояние до станции назначения": row.distance,
            "Станция местоположения": station_name,
        }

        op = self._map_operation(row.operation, row.distance, station_name, row.destination_station)
        data["Операция"] = op

        if existing:
            prev_dist = existing.get("Расстояние до станции назначения")
            prev_date = existing.get("Дата обновления слежения")
            if prev_dist is None or float(prev_dist) != float(row.distance):
                data["Дата обновления слежения"] = today
            else:
                data["Дата обновления слежения"] = prev_date
            self.sheet.update(row.container, data)
        else:
            data["Дата обновления слежения"] = today
            self.sheet.add(data)

        self.logger.info(f"Синхронизирован контейнер {row.container}")
