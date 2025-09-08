"""Utilities for synchronising data with Google Sheets.

This module implements the behaviour described in task 2. The logic is kept
framework agnostic so that unit tests can use a lightweight in-memory
``Worksheet`` replacement. When used in production a real gspread worksheet can
be supplied.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional, Callable
from zoneinfo import ZoneInfo

from utils.logging import get_logger
from config.settings import GoogleSheetsConfig

try:  # pragma: no cover - optional dependency for real usage
    import gspread  # type: ignore
except Exception:  # pragma: no cover
    gspread = None  # type: ignore

logger = get_logger("google_sheets_sync")


@dataclass
class SheetRow:
    """Representation of a row in the target Google Sheet."""

    container: str
    loading_date: str
    tracking_date: str
    operation: str
    distance: float
    station_name: str

    def to_list(self) -> list[str]:
        return [
            self.container,
            self.loading_date,
            self.tracking_date,
            self.operation,
            str(self.distance),
            self.station_name,
        ]


class WorksheetAdapter:
    """Minimal interface used by :class:`GoogleSheetsSync`.

    Real gspread worksheets already provide ``append_row`` and ``batch_update``
    methods. The adapter allows simple in-memory implementations for tests.
    """

    def __init__(self, worksheet):
        self.worksheet = worksheet

    def find_row(self, container: str) -> Optional[int]:
        if hasattr(self.worksheet, "find_row"):
            return self.worksheet.find_row(container)
        try:  # pragma: no cover - real gspread
            cell = self.worksheet.find(container)
            return cell.row - 1
        except Exception:
            return None

    def get_row(self, index: int) -> Dict[str, str]:
        if hasattr(self.worksheet, "get_row"):
            return self.worksheet.get_row(index)
        # pragma: no cover - real gspread
        values = self.worksheet.row_values(index + 1)
        headers = [
            "Контейнер",
            "Отгрузка на ЖД",
            "Дата обновления слежения",
            "Операция",
            "Расстояние до станции назначения",
            "Станция местоположения",
        ]
        return dict(zip(headers, values))

    def append_row(self, row: SheetRow) -> None:
        if hasattr(self.worksheet, "update_row"):
            self.worksheet.append_row(row)
        else:  # pragma: no cover - real gspread
            self.worksheet.append_row(row.to_list(), value_input_option="USER_ENTERED")

    def update_row(self, index: int, row: SheetRow) -> None:
        if hasattr(self.worksheet, "update_row"):
            self.worksheet.update_row(index, row)
        else:  # pragma: no cover - real gspread
            rng = f"A{index+1}:F{index+1}"
            self.worksheet.batch_update([{ "range": rng, "values": [row.to_list()]}])


class GoogleSheetsSync:
    """Synchronise container data with a Google Sheet."""

    def __init__(
        self,
        worksheet: WorksheetAdapter,
        timezone: str = "Asia/Vladivostok",
        now_func: Callable[[], datetime] | None = None,
    ):
        self.ws = worksheet
        self.tz = ZoneInfo(timezone)
        self.now_func = now_func or (lambda: datetime.now(self.tz))
        self.logger = get_logger("google_sheets_sync")

    # ------------------------------------------------------------------
    @staticmethod
    def map_operation(
        status: Optional[str],
        distance: float,
        at_destination: bool,
        stagnant_days: int = 0,
    ) -> str:
        """Map arbitrary *status* to one of three allowed values."""
        allowed = {
            "в пути": "В пути",
            "прибыл на станцию назначения": "прибыл на станцию назначения",
            "простой в пути": "простой в пути",
        }
        if status:
            key = status.strip().lower()
            if key in allowed:
                return allowed[key]

        if at_destination and distance == 0:
            return "прибыл на станцию назначения"
        if stagnant_days >= 1:
            return "простой в пути"
        return "В пути"

    # ------------------------------------------------------------------
    def sync_row(self, data: Dict[str, object], stagnant_days: int = 0) -> bool:
        """Upsert a single row in the worksheet.

        Parameters
        ----------
        data:
            Dictionary with keys ``Контейнер``, ``Отгрузка на ЖД``,
            ``Расстояние до станции назначения``, ``Станция местоположения`` and
            optionally ``Операция`` and ``Дата обновления слежения``.
        stagnant_days:
            Number of days without movement used to detect the "простой в пути"
            status.
        """
        container = str(data["Контейнер"])
        distance = float(data["Расстояние до станции назначения"])
        station_name = str(data["Станция местоположения"])
        loading_date = str(data["Отгрузка на ЖД"])
        status = data.get("Операция")
        at_destination = bool(data.get("at_destination", False))
        operation = self.map_operation(status, distance, at_destination, stagnant_days)

        existing_index = self.ws.find_row(container)
        today = self.now_func().strftime("%d-%m-%Y")

        if existing_index is None:
            row = SheetRow(container, loading_date, today, operation, distance, station_name)
            self.ws.append_row(row)
            self.logger.info("Row added for %s", container)
            return True

        current = self.ws.get_row(existing_index)
        tracking_date = current.get("Дата обновления слежения", today)
        row = SheetRow(
            container,
            loading_date,
            tracking_date,
            operation,
            distance,
            station_name,
        )

        # Update tracking date only when distance changed
        try:
            existing_distance = float(
                current.get("Расстояние до станции назначения", distance)
            )
        except ValueError:
            existing_distance = distance
        if existing_distance != distance:
            row.tracking_date = today

        self.ws.update_row(existing_index, row)
        self.logger.info("Row updated for %s", container)
        return existing_distance != distance


def worksheet_from_config(cfg: GoogleSheetsConfig) -> WorksheetAdapter:
    """Create a :class:`WorksheetAdapter` from configuration."""
    if gspread is None:
        raise RuntimeError("gspread is required for Google Sheets synchronisation")
    client = gspread.service_account(filename=cfg.credentials_file)
    sheet = client.open_by_key(cfg.sheet_key)
    ws = sheet.worksheet(cfg.worksheet_name)
    return WorksheetAdapter(ws)
