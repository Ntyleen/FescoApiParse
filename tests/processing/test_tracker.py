import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from config.settings import Config, FirebirdDatabaseConfig
from processing.tracker import ContainerTracker
from models.container_event import ContainerEvent, TrackingResult
from cache.cache_base import CacheBackend


class DummyCache(CacheBackend):
    def __init__(self):
        self.store = {}

    async def get(self, key: str):
        return self.store.get(key)

    async def set(self, key: str, data, ttl_seconds: int = 3600):
        self.store[key] = data

    async def delete(self, key: str):
        self.store.pop(key, None)
        return True

    async def clear(self):
        self.store.clear()

    async def exists(self, key: str) -> bool:
        return key in self.store

    async def close(self):
        pass


@pytest.fixture
def tracker_with_mocks(monkeypatch):
    api_client = MagicMock()
    event_processor = MagicMock()
    monkeypatch.setattr("processing.tracker.FescoApiClient", MagicMock(return_value=api_client))
    monkeypatch.setattr("processing.tracker.EventProcessor", MagicMock(return_value=event_processor))
    config = Config(database=FirebirdDatabaseConfig(database="dummy.fdb"))
    tracker = ContainerTracker(config, DummyCache())
    return tracker, api_client, event_processor


@pytest.mark.asyncio
async def test_track_single_container_success(tracker_with_mocks):
    tracker, api_client, event_proc = tracker_with_mocks

    session = MagicMock()
    api_client.find_order_by_container = AsyncMock(return_value="ORD1")
    api_client.get_order_tracking = AsyncMock(return_value={"data": []})
    api_client.get_container_tracking = AsyncMock(return_value={"data": []})

    event = ContainerEvent(date="2024-01-01", location="VVO", operation="ARRIVED")
    event_proc.extract_order_events.return_value = [event]
    event_proc.extract_container_events.return_value = []
    event_proc.merge_and_deduplicate.return_value = (event, False, "order")

    result = await tracker.track_single_container(session, "CONT1")

    assert result.success
    assert result.last_event == event
    assert tracker.stats.successful_tracks == 1
    assert tracker.stats.failed_tracks == 0
    api_client.find_order_by_container.assert_awaited_once_with(session, "CONT1")
    api_client.get_order_tracking.assert_awaited_once_with(session, "ORD1")
    api_client.get_container_tracking.assert_awaited_once_with(session, "ORD1", "CONT1")


@pytest.mark.asyncio
async def test_track_single_container_no_order(tracker_with_mocks):
    tracker, api_client, event_proc = tracker_with_mocks

    session = MagicMock()
    api_client.find_order_by_container = AsyncMock(return_value=None)

    result = await tracker.track_single_container(session, "CONT2")

    assert not result.success
    assert result.error_message == "Заявка не найдена"
    assert tracker.stats.failed_tracks == 1
    api_client.get_order_tracking.assert_not_called()
    event_proc.extract_order_events.assert_not_called()


@pytest.mark.asyncio
async def test_track_containers_with_mocked_tasks(monkeypatch):
    monkeypatch.setattr("processing.tracker.FescoApiClient", MagicMock())

    config = Config(database=FirebirdDatabaseConfig(database="dummy.fdb"))
    tracker = ContainerTracker(config, DummyCache())

    async def fake_track(self, session, number):
        await asyncio.sleep(0)
        result = TrackingResult(container_number=number)
        result.last_event = ContainerEvent(date="2024-01-01")
        self.stats.successful_tracks += 1
        return result

    monkeypatch.setattr(ContainerTracker, "track_single_container", fake_track)

    class DummySession:
        async def __aenter__(self):
            return self
        async def __aexit__(self, exc_type, exc, tb):
            pass

    monkeypatch.setattr("processing.tracker.aiohttp.ClientSession", lambda *args, **kwargs: DummySession())

    numbers = ["A", "B", "C"]
    results = [r async for r in tracker.track_containers(numbers)]

    assert {r.container_number for r in results} == set(numbers)
    assert tracker.stats.total_containers == len(numbers)
    assert tracker.stats.successful_tracks == len(numbers)
    assert tracker.stats.end_time >= tracker.stats.start_time
