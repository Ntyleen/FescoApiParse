from datetime import datetime
from datetime import datetime

from processing.google_sheets_sync import WorksheetAdapter, GoogleSheetsSync, SheetRow


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
