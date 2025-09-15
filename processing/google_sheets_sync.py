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
import os

from utils.logging import get_logger
from config.settings import GoogleSheetsConfig

try:  # pragma: no cover - optional dependency for real usage
    import gspread  # type: ignore
    from google.auth.transport.requests import Request  # type: ignore
    from google.oauth2.credentials import Credentials  # type: ignore
    from google_auth_oauthlib.flow import InstalledAppFlow  # type: ignore
except Exception:  # pragma: no cover
    gspread = None  # type: ignore
    Request = Credentials = InstalledAppFlow = None  # type: ignore

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

    def list_containers(self) -> list[str]:
        if hasattr(self.worksheet, "list_containers"):
            return self.worksheet.list_containers()
        try:  # pragma: no cover - real gspread
            return self.worksheet.col_values(1)
        except Exception:
            return []

    def append_row(self, row: SheetRow) -> None:
        """Append *row* to the worksheet.

        The adapter tries the real gspread API first and falls back to the
        in-memory interface used in tests when the call fails.
        """

        try:
            self.worksheet.append_row(row.to_list(), value_input_option="USER_ENTERED")
        except Exception:  # pragma: no cover - fake worksheet branch
            self.worksheet.append_row(row)

    def update_row(self, index: int, row: SheetRow) -> None:
        """Update the row at *index* with new data."""

        try:
            rng = f"A{index+1}:F{index+1}"
            self.worksheet.batch_update([{ "range": rng, "values": [row.to_list()]}])
        except Exception:  # pragma: no cover - fake worksheet branch
            self.worksheet.update_row(index, row)


# Default scopes for OAuth2 authentication
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def get_authenticated_worksheet(
    sheet_id: str,
    worksheet_name: str,
    client_secret_file: str,
    token_file: str = "token.json",
) -> WorksheetAdapter:
    """Return an authenticated :class:`WorksheetAdapter` for Google Sheets.

    The function performs the OAuth flow using the provided ``client_secret``
    file generated in Google Cloud.  Credentials are cached in ``token_file``
    so the interactive flow is required only once.

    Parameters
    ----------
    sheet_id:
        Identifier of the Google Sheet document.
    worksheet_name:
        Name of the worksheet within the document.
    client_secret_file:
        Path to the OAuth 2.0 Client ID JSON file downloaded from Google Cloud.
    token_file:
        Location where the access/refresh token pair will be stored.  Defaults
        to ``token.json`` in the current directory.
    """

    if gspread is None or Credentials is None:
        raise ImportError("gspread and google-auth libraries are required for OAuth")

    creds: Optional[Credentials] = None
    if os.path.exists(token_file):
        creds = Credentials.from_authorized_user_file(token_file, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(client_secret_file, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(token_file, "w", encoding="utf-8") as token:
            token.write(creds.to_json())

    client = gspread.authorize(creds)
    worksheet = client.open_by_key(sheet_id).worksheet(worksheet_name)
    return WorksheetAdapter(worksheet)


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


def create_sync_from_config(cfg: GoogleSheetsConfig) -> GoogleSheetsSync:
    """Utility to build :class:`GoogleSheetsSync` from config."""

    worksheet = get_authenticated_worksheet(
        cfg.sheet_id, cfg.worksheet, cfg.client_secret_file, cfg.token_file
    )
    return GoogleSheetsSync(worksheet, timezone=cfg.timezone)
