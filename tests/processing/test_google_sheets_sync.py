from datetime import datetime

from processing.google_sheets_sync import (
    WorksheetAdapter,
    GoogleSheetsSync,
    SheetRow,
    create_sync_from_config,
)
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
    assert GoogleSheetsSync.map_operation("прибыл на станцию назначения", 0, True) == "прибыл на станцию назначения"
    assert GoogleSheetsSync.map_operation("что-то", 0, True) == "прибыл на станцию назначения"
    assert GoogleSheetsSync.map_operation("что-то", 5, False, 2) == "простой в пути"


def test_upsert_updates_tracking_date_only_on_distance_change():
    sheet = WorksheetAdapter(FakeWorksheet())
    fixed_now = lambda: datetime(2025, 9, 8, 10, 0, 0)
    sync = GoogleSheetsSync(sheet, now_func=fixed_now)

    data = {
        "Контейнер": "MSCU1234567",
        "Отгрузка на ЖД": "01-09-2025",
        "Расстояние до станции назначения": 225.0,
        "Станция местоположения": "Москва",
        "Операция": "В пути",
    }
    sync.sync_row(data)
    assert len(sheet.worksheet.rows) == 1
    first_date = sheet.worksheet.rows[0].tracking_date

    # Second call with same distance -> date not changed
    sync.sync_row(data)
    assert sheet.worksheet.rows[0].tracking_date == first_date

    # Third call with changed distance -> date updated
    sync.now_func = lambda: datetime(2025, 9, 9, 10, 0, 0)
    data["Расстояние до станции назначения"] = 218.0
    sync.sync_row(data)
    assert sheet.worksheet.rows[0].tracking_date == "09-09-2025"
    assert sheet.worksheet.rows[0].distance == 218.0


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

    def fake_auth(sheet_id, worksheet_name, client_secret_file, token_file="token.json"):
        assert sheet_id == "ID1"
        assert worksheet_name == "Лист1"
        assert client_secret_file == "secret.json"
        assert token_file == "token.json"
        return fake_ws

    monkeypatch.setattr(
        "processing.google_sheets_sync.get_authenticated_worksheet", fake_auth
    )
    sync = create_sync_from_config(cfg)
    assert isinstance(sync, GoogleSheetsSync)
    assert sync.ws is fake_ws
