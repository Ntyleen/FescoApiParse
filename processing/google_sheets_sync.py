"""Utilities for synchronising data with Google Sheets.

This module implements the behaviour described in task 2. The logic is kept
framework agnostic so that unit tests can use a lightweight in-memory
``Worksheet`` replacement. When used in production a real gspread worksheet can
be supplied.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional, Callable, Iterable, List
from zoneinfo import ZoneInfo
import asyncio
import json
import os

from utils.logging import get_logger
from config.settings import GoogleSheetsConfig

try:  # pragma: no cover - optional dependency for real usage
    import gspread  # type: ignore
    from google.auth.transport.requests import Request  # type: ignore
    from google.oauth2 import service_account  # type: ignore
    from google.oauth2.credentials import Credentials  # type: ignore
    from google_auth_oauthlib.flow import InstalledAppFlow  # type: ignore
except Exception:  # pragma: no cover
    gspread = None  # type: ignore
    Request = Credentials = InstalledAppFlow = service_account = None  # type: ignore

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

    def batch_update_rows(self, updates: Iterable[tuple[int, SheetRow]]) -> None:
        """Update multiple rows using a single batch call when possible."""

        updates = list(updates)
        if not updates:
            return
        try:
            if hasattr(self.worksheet, "batch_update_rows"):
                self.worksheet.batch_update_rows(updates)
                return
        except Exception:
            pass

        payload = [
            {
                "range": f"A{index+1}:F{index+1}",
                "values": [row.to_list()],
            }
            for index, row in updates
        ]
        try:
            self.worksheet.batch_update(payload)
        except Exception:  # pragma: no cover - fallback for fake worksheet
            for index, row in updates:
                self.worksheet.update_row(index, row)


# Default scopes for OAuth2 authentication
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def get_authenticated_worksheet(
    sheet_id: str,
    worksheet_name: str,
    client_secret_file: str = "",
    token_file: str = "token.json",
    service_account_file: Optional[str] = None,
    service_account_info: Optional[Dict[str, Any] | str] = None,
    subject: Optional[str] = None,
    scopes: Optional[List[str]] = None,
) -> WorksheetAdapter:
    """Return an authenticated :class:`WorksheetAdapter` for Google Sheets.

    The helper supports both the traditional OAuth flow (using ``client_secret``
    files) and service account credentials.  When a service account file or
    JSON payload is provided the OAuth flow is skipped entirely, allowing the
    application to run in headless environments without user interaction.

    Parameters
    ----------
    sheet_id:
        Identifier of the Google Sheet document.
    worksheet_name:
        Name of the worksheet within the document.
    client_secret_file:
        Path to the OAuth 2.0 Client ID JSON file downloaded from Google Cloud.
        Used when service account credentials are not supplied.
    token_file:
        Location where the access/refresh token pair will be stored for OAuth.
    service_account_file:
        Optional path to the service account JSON key.
    service_account_info:
        Optional JSON payload (``dict`` or JSON string) with service account
        credentials.  This is useful when credentials are provided via
        environment variables or secret stores.
    subject:
        Optional subject for domain-wide delegation.
    scopes:
        Optional list of scopes to request.  Defaults to :data:`SCOPES` when
        ``None``.
    """

    if gspread is None:
        raise ImportError("gspread and google-auth libraries are required")

    scopes_to_use = SCOPES if scopes is None else scopes

    creds: Any
    # ------------------------------------------------------------------
    # Service account authentication (preferred when available)
    if (service_account_file and service_account_file.strip()) or service_account_info:
        if service_account is None:
            raise ImportError(
                "google.oauth2.service_account is required for service account authentication"
            )

        info_payload: Optional[Dict[str, Any]] = None
        if service_account_info is not None:
            if isinstance(service_account_info, dict):
                info_payload = service_account_info
            elif isinstance(service_account_info, str):
                candidate = service_account_info.strip()
                if candidate:
                    if os.path.exists(candidate):
                        with open(candidate, "r", encoding="utf-8") as fp:
                            info_payload = json.load(fp)
                    else:
                        try:
                            info_payload = json.loads(candidate)
                        except json.JSONDecodeError as exc:
                            raise ValueError(
                                "service_account_info must be a JSON string, dict or path to a JSON file"
                            ) from exc
            else:
                raise TypeError(
                    "service_account_info must be either a dict, JSON string or path to a JSON file"
                )

        if info_payload is not None:
            creds = service_account.Credentials.from_service_account_info(
                info_payload, scopes=scopes_to_use
            )
        elif service_account_file and service_account_file.strip():
            creds = service_account.Credentials.from_service_account_file(
                service_account_file, scopes=scopes_to_use
            )
        else:
            raise ValueError(
                "Service account configuration requires either service_account_file or service_account_info"
            )

        if subject:
            creds = creds.with_subject(subject)

    # ------------------------------------------------------------------
    # OAuth authentication (interactive, cached locally)
    else:
        if Credentials is None or Request is None or InstalledAppFlow is None:
            raise ImportError(
                "google-auth oauth client libraries are required for interactive authentication"
            )
        if not client_secret_file:
            raise ValueError(
                "client_secret_file must be provided when service account credentials are not supplied"
            )

        oauth_creds: Optional[Credentials] = None
        if os.path.exists(token_file):
            oauth_creds = Credentials.from_authorized_user_file(token_file, scopes_to_use)

        if not oauth_creds or not oauth_creds.valid:
            if oauth_creds and oauth_creds.expired and oauth_creds.refresh_token:
                oauth_creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    client_secret_file, scopes_to_use
                )
                oauth_creds = flow.run_local_server(port=0)
            with open(token_file, "w", encoding="utf-8") as token:
                token.write(oauth_creds.to_json())

        creds = oauth_creds

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
        batch_size: int = 20,
    ):
        self.ws = worksheet
        self.tz = ZoneInfo(timezone)
        self.now_func = now_func or (lambda: datetime.now(self.tz))
        self.logger = get_logger("google_sheets_sync")
        self.batch_size = max(1, batch_size)

    # ------------------------------------------------------------------
    @staticmethod
    def map_operation(
        status: Optional[str],
        distance: float,
        at_destination: bool,
        stagnant_days: int = 0,
    ) -> str:
        """Map arbitrary *status* to one of four allowed values."""
        allowed = {
            "в пути": "В пути",
            "простой в пути": "Простой в пути",
            "прибыл на станцию": "Прибыл на станцию",
            "прибыл на станцию назначения": "Прибыл на станцию",
            "отгружен": "Отгружен",
        }
        if status:
            key = status.strip().lower()
            if key in allowed:
                return allowed[key]

        if at_destination or distance == 0:
            return "Прибыл на станцию"
        if stagnant_days >= 1:
            return "Простой в пути"
        return "В пути"

    # ------------------------------------------------------------------
    async def sync_row(self, data: Dict[str, object], stagnant_days: int = 0) -> bool:
        """Asynchronously upsert a single row in the worksheet."""

        return await asyncio.to_thread(self._sync_row, data, stagnant_days)

    async def sync_rows(
        self,
        rows: List[Dict[str, object]],
        stagnant_days: int = 0,
    ) -> int:
        """Batch update multiple rows and return number of changed entries."""

        return await asyncio.to_thread(self._sync_rows, rows, stagnant_days)

    # ------------------------------------------------------------------
    def _prepare_row_update(
        self, data: Dict[str, object], stagnant_days: int = 0
    ) -> Optional[tuple[int, SheetRow, bool]]:
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

        existing_index = self.ws.find_row(container)
        if existing_index is None:
            self.logger.info(
                "Row for %s not found in Google Sheet; skipping update", container
            )
            return None

        operation = self.map_operation(status, distance, at_destination, stagnant_days)
        today = self.now_func().strftime("%d-%m-%Y")

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

        return existing_index, row, existing_distance != distance

    def _sync_row(self, data: Dict[str, object], stagnant_days: int = 0) -> bool:
        prepared = self._prepare_row_update(data, stagnant_days)
        if not prepared:
            return False
        index, row, changed = prepared
        self.ws.batch_update_rows([(index, row)])
        self.logger.info("Row updated for %s", row.container)
        return changed

    def _sync_rows(self, rows: List[Dict[str, object]], stagnant_days: int = 0) -> int:
        updates: list[tuple[int, SheetRow]] = []
        changed = 0
        for data in rows:
            prepared = self._prepare_row_update(data, stagnant_days)
            if not prepared:
                continue
            index, row, is_changed = prepared
            updates.append((index, row))
            if is_changed:
                changed += 1
            if len(updates) >= self.batch_size:
                self.ws.batch_update_rows(updates)
                updates.clear()
        if updates:
            self.ws.batch_update_rows(updates)
        return changed


def create_sync_from_config(cfg: GoogleSheetsConfig) -> GoogleSheetsSync:
    """Utility to build :class:`GoogleSheetsSync` from config."""

    info = getattr(cfg, "service_account_info", None)
    if isinstance(info, str) and not info.strip():
        info = None

    worksheet = get_authenticated_worksheet(
        cfg.sheet_id,
        cfg.worksheet,
        client_secret_file=getattr(cfg, "client_secret_file", ""),
        token_file=getattr(cfg, "token_file", "token.json"),
        service_account_file=(getattr(cfg, "service_account_file", "") or None),
        service_account_info=info,
        subject=getattr(cfg, "delegated_subject", None),
        scopes=getattr(cfg, "scopes", None),
    )
    return GoogleSheetsSync(
        worksheet,
        timezone=cfg.timezone,
        batch_size=getattr(cfg, "batch_size", 20),
    )
