import os
import sys
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from processing.google_sheets_sync import GoogleSheetsSync, SheetRow, SheetInterface


class SimpleSheet(SheetInterface):
    def __init__(self):
        self.rows = {}

    def get(self, container: str):
        return self.rows.get(container)

    def update(self, container: str, data):
        self.rows[container] = data

    def add(self, data):
        self.rows[data["Контейнер"]] = data


class DummyFirebird:
    async def get_station_name_by_id(self, station_id):
        return {4451: "Москва-Каланчёвская"}.get(station_id)


@pytest.mark.asyncio
async def test_upsert_updates_distance_and_date():
    sheet = SimpleSheet()
    fb = DummyFirebird()
    sync = GoogleSheetsSync(sheet, fb)

    row = SheetRow(
        container="MSCU1234567",
        railway_loading="2025-09-01",
        distance=225.0,
        station_id=4451,
    )
    await sync.upsert_row(row)
    saved = sheet.get("MSCU1234567")
    assert saved["Станция местоположения"] == "Москва-Каланчёвская"
    first_date = saved["Дата обновления слежения"]

    row2 = SheetRow(
        container="MSCU1234567",
        railway_loading="2025-09-01",
        distance=225.0,
        station_id=4451,
    )
    await sync.upsert_row(row2)
    saved = sheet.get("MSCU1234567")
    assert saved["Дата обновления слежения"] == first_date

    row3 = SheetRow(
        container="MSCU1234567",
        railway_loading="2025-09-01",
        distance=218.0,
        station_id=4451,
    )
    await sync.upsert_row(row3)
    saved = sheet.get("MSCU1234567")
    assert saved["Дата обновления слежения"] == sync._today_str()
    assert saved["Операция"] == "В пути"
