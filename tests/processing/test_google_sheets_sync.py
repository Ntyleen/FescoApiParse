import json
import os
import sys
from datetime import datetime
from types import SimpleNamespace
from typing import Optional

import pytest

# Ensure project root is available for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from processing.google_sheets_sync import (
    WorksheetAdapter,
    GoogleSheetsSync,
    SheetRow,
    create_sync_from_config,
)
import processing.google_sheets_sync as gs_sync
from config.settings import GoogleSheetsConfig


class FakeWorksheet:
    def __init__(self):
        self.rows = []  # list of SheetRow

    def find_row(self, container):
        for idx, row in enumerate(self.rows):
            if row.container == container:
                return idx
        return None

    def get_row(self, index):
        row = self.rows[index]
        return {
            "Контейнер": row.container,
            "Отгрузка на ЖД": row.loading_date,
            "Дата обновления слежения": row.tracking_date,
            "Операция": row.operation,
            "Расстояние до станции назначения": row.distance,
            "Станция местоположения": row.station_name,
        }

    def append_row(self, row):
        self.rows.append(row)

    def update_row(self, index, row):
        self.rows[index] = row

    def batch_update(self, payload):
        for update in payload:
            start = int(update["range"][1:].split(":")[0]) - 1
            values = update["values"][0]
            converted = values[:]
            converted[4] = float(converted[4]) if converted[4] != "" else 0.0
            self.rows[start] = SheetRow(*converted)


class GspreadLikeWorksheet:
    """Minimal gspread-like worksheet for adapter tests."""

    def __init__(self):
        self.appended = []
        self.updated = []

    def find(self, _):  # pragma: no cover - not used in test
        raise NotImplementedError

    def row_values(self, row_number):
        return ["A", "B", "C", "D", "E", "F"]

    def append_row(self, values, value_input_option=None):
        self.appended.append((values, value_input_option))

    def batch_update(self, data):
        self.updated.append(data)


def test_map_operation_rules():
    assert GoogleSheetsSync.map_operation(None, 10, False, 0) == "В пути"
    assert GoogleSheetsSync.map_operation("прибыл на станцию", 0, True) == "Прибыл на станцию"
    assert GoogleSheetsSync.map_operation("что-то", 0, True) == "Прибыл на станцию"
    assert GoogleSheetsSync.map_operation("что-то", 5, False, 2) == "Простой в пути"
    assert GoogleSheetsSync.map_operation("Отгружен", 5, False, 0) == "Отгружен"


@pytest.mark.asyncio
async def test_upsert_updates_tracking_date_only_on_distance_change():
    sheet = WorksheetAdapter(FakeWorksheet())
    fixed_now = lambda: datetime(2025, 9, 8, 10, 0, 0)
    sync = GoogleSheetsSync(sheet, now_func=fixed_now)

    sheet.worksheet.append_row(
        SheetRow(
            "MSCU1234567",
            "01-09-2025",
            "02-09-2025",
            "Отгружен",
            225.0,
            "Москва",
        )
    )
    initial_len = len(sheet.worksheet.rows)

    data = {
        "Контейнер": "MSCU1234567",
        "Отгрузка на ЖД": "01-09-2025",
        "Расстояние до станции назначения": 225.0,
        "Станция местоположения": "Москва",
        "Операция": "В пути",
    }
    await sync.sync_row(data)
    assert len(sheet.worksheet.rows) == initial_len
    first_date = sheet.worksheet.rows[0].tracking_date
    assert first_date == "02-09-2025"
    assert sheet.worksheet.rows[0].operation == "В пути"

    # Second call with same distance -> date not changed
    await sync.sync_row(data)
    assert sheet.worksheet.rows[0].tracking_date == first_date
    assert sheet.worksheet.rows[0].operation == "В пути"

    # Third call with changed distance -> date updated
    sync.now_func = lambda: datetime(2025, 9, 9, 10, 0, 0)
    data["Расстояние до станции назначения"] = 218.0
    await sync.sync_row(data)
    assert sheet.worksheet.rows[0].tracking_date == "09-09-2025"
    assert sheet.worksheet.rows[0].distance == 218.0
    assert sheet.worksheet.rows[0].operation == "В пути"


@pytest.mark.asyncio
async def test_sync_row_skips_missing_container():
    sheet = WorksheetAdapter(FakeWorksheet())
    sync = GoogleSheetsSync(sheet)

    data = {
        "Контейнер": "TGHU7654321",
        "Отгрузка на ЖД": "01-09-2025",
        "Расстояние до станции назначения": 120.0,
        "Станция местоположения": "Владивосток",
        "Операция": "В пути",
    }

    result = await sync.sync_row(data)
    assert result is False
    assert len(sheet.worksheet.rows) == 0


def test_adapter_with_gspread_like_object():
    ws = WorksheetAdapter(GspreadLikeWorksheet())
    row = SheetRow("C1", "01-09-2025", "02-09-2025", "В пути", 10.0, "Station")

    ws.append_row(row)
    assert ws.worksheet.appended[0][0] == row.to_list()
    assert ws.worksheet.appended[0][1] == "USER_ENTERED"

    ws.update_row(0, row)
    update = ws.worksheet.updated[0][0]
    assert update["range"] == "A1:F1"
    assert update["values"][0] == row.to_list()


def test_create_sync_from_config(monkeypatch):
    cfg = GoogleSheetsConfig(
        sheet_id="ID1", worksheet="Лист1", client_secret_file="secret.json"
    )
    fake_ws = WorksheetAdapter(FakeWorksheet())

    def fake_auth(
        sheet_id,
        worksheet_name,
        client_secret_file="",
        token_file="token.json",
        service_account_file=None,
        service_account_info=None,
        subject=None,
        scopes=None,
    ):
        assert sheet_id == "ID1"
        assert worksheet_name == "Лист1"
        assert client_secret_file == "secret.json"
        assert token_file == "token.json"
        assert service_account_file in (None, "")
        assert service_account_info is None
        assert subject is None
        assert scopes is None
        return fake_ws

    monkeypatch.setattr(
        "processing.google_sheets_sync.get_authenticated_worksheet", fake_auth
    )
    sync = create_sync_from_config(cfg)
    assert isinstance(sync, GoogleSheetsSync)
    assert sync.ws is fake_ws


def test_get_authenticated_worksheet_with_service_account(monkeypatch):
    captured: dict[str, object] = {}

    class DummyCreds:
        def __init__(self, source: str):
            self.source = source
            self.subject: Optional[str] = None

        def with_subject(self, subject: str):
            self.subject = subject
            return self

    class DummyCredentialsFactory:
        @staticmethod
        def from_service_account_file(filename, scopes=None):
            captured["file"] = filename
            captured["scopes"] = scopes
            return DummyCreds("file")

        @staticmethod
        def from_service_account_info(info, scopes=None):
            captured["info"] = info
            captured["scopes"] = scopes
            return DummyCreds("info")

    class DummyServiceAccountModule:
        Credentials = DummyCredentialsFactory

    class DummySpreadsheet:
        def worksheet(self, name):
            captured["worksheet_name"] = name
            return SimpleNamespace(name=name)

    class DummyClient:
        def __init__(self):
            self.sheet = DummySpreadsheet()

        def open_by_key(self, key):
            captured["sheet_id"] = key
            return self.sheet

    def fake_authorize(creds):
        captured["authorized_creds"] = creds
        return DummyClient()

    monkeypatch.setattr(gs_sync, "service_account", DummyServiceAccountModule)
    monkeypatch.setattr(gs_sync, "gspread", SimpleNamespace(authorize=fake_authorize))

    adapter = gs_sync.get_authenticated_worksheet(
        "sheet-1",
        "Лист1",
        service_account_file="/path/key.json",
        subject="user@example.com",
    )

    assert isinstance(adapter, WorksheetAdapter)
    assert captured["file"] == "/path/key.json"
    assert captured["sheet_id"] == "sheet-1"
    assert captured["worksheet_name"] == "Лист1"
    assert captured["authorized_creds"].source == "file"
    assert captured["authorized_creds"].subject == "user@example.com"
    assert captured["scopes"] == gs_sync.SCOPES

    info_payload = {"type": "service_account", "client_email": "robot@example.com"}
    adapter = gs_sync.get_authenticated_worksheet(
        "sheet-2",
        "Лист2",
        service_account_info=json.dumps(info_payload),
        scopes=["scope1"],
    )

    assert isinstance(adapter, WorksheetAdapter)
    assert captured["info"] == info_payload
    assert captured["sheet_id"] == "sheet-2"
    assert captured["worksheet_name"] == "Лист2"
    assert captured["authorized_creds"].source == "info"
    assert captured["authorized_creds"].subject is None
    assert captured["scopes"] == ["scope1"]


@pytest.mark.asyncio
async def test_sync_rows_batch_updates():
    sheet = WorksheetAdapter(FakeWorksheet())
    sheet.worksheet.append_row(
        SheetRow("CNT1", "01-01-2024", "01-01-2024", "В пути", 100.0, "Москва")
    )
    sheet.worksheet.append_row(
        SheetRow("CNT2", "02-01-2024", "02-01-2024", "В пути", 50.0, "Москва")
    )

    sync = GoogleSheetsSync(sheet, batch_size=1, now_func=lambda: datetime(2024, 1, 5))
    rows = [
        {
            "Контейнер": "CNT1",
            "Отгрузка на ЖД": "01-01-2024",
            "Расстояние до станции назначения": 80.0,
            "Станция местоположения": "Москва",
            "Операция": "В пути",
        },
        {
            "Контейнер": "CNT2",
            "Отгрузка на ЖД": "02-01-2024",
            "Расстояние до станции назначения": 50.0,
            "Станция местоположения": "Москва",
            "Операция": "В пути",
        },
    ]

    changed = await sync.sync_rows(rows)
    assert changed == 1
    assert sheet.worksheet.rows[0].distance == 80.0
    assert sheet.worksheet.rows[0].tracking_date == "05-01-2024"
