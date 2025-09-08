import os
import sys
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from processing.station_updater import StationUpdater


class DummyFirebird:
    def __init__(self):
        self.stations = {"МОСКВА": [(4451, "Москва-Каланчёвская")]}
        self.entities = {}

    async def find_station_by_name(self, name):
        return self.stations.get(name, [])

    async def get_entity_station_id(self, entity_id):
        return self.entities.get(entity_id)

    async def update_entity_station_id(self, entity_id, station_id):
        self.entities[entity_id] = station_id
        return True


@pytest.mark.asyncio
async def test_station_updater_updates_only_changed():
    fb = DummyFirebird()
    updater = StationUpdater(fb)
    records = [{"entity_id": 1, "location": " ст. Москва "}]
    stats = await updater.process_records(records)
    assert fb.entities[1] == 4451
    assert stats["updated"] == 1

    # повторная обработка не должна обновлять
    stats = await updater.process_records(records)
    assert stats["skipped"] == 1


def test_normalize_removes_prefixes():
    assert StationUpdater.normalize(" ст. Владивосток ") == "ВЛАДИВОСТОК"
