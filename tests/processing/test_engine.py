import asyncio
import types
import os
import sys
from unittest.mock import AsyncMock, MagicMock, call

import pytest

# Ensure project root is on sys.path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from config.settings import Config, FirebirdDatabaseConfig
from models.container_event import TrackingResult, ContainerEvent
from processing.ContainerTrackingEngine import ContainerTrackingEngine
from utils.db.firebird_manager import ContainerInfo
from cache.cache_base import CacheBackend


class DummyCache(CacheBackend):
    def __init__(self):
        self.store = {}

    async def get(self, key: str):
        return self.store.get(key)

    async def set(self, key: str, data, ttl_seconds: int = 3600):
        self.store[key] = data

    async def delete(self, key: str) -> bool:
        return self.store.pop(key, None) is not None

    async def clear(self):
        self.store.clear()

    async def exists(self, key: str) -> bool:
        return key in self.store

    async def close(self):
        pass


class DummyFirebirdManager:
    def __init__(self, containers):
        self.containers = containers
        self.updated = []
        self.entity_config = MagicMock(date_railway_loading="DATE_RAILWAY_LOADING")
        mapping = MagicMock()
        mapping.entity_column = "DATE_ETA"
        self.operation_matcher = MagicMock(
            find_best_mapping=MagicMock(return_value=mapping),
            set_railway_mode=MagicMock(),
        )
        self.processing_called = False
        self.contractor_called = False

    async def test_connection(self):
        return True

    async def get_containers_for_processing(self, batch_size=100, target_line_ids=None):
        self.processing_called = True
        yield self.containers

    async def get_containers_for_contractors(self, batch_size=100, target_carrier_ids=None):
        self.contractor_called = True
        yield self.containers

    async def update_container_from_tracking(self, container_id, events):
        self.updated.append((container_id, events))
        return True

    async def get_entity_statistics(self):
        return {"runtime_stats": {"records_updated": len(self.updated)}}

    async def close(self):
        pass


@pytest.mark.asyncio
async def test_run_full_workflow_groups_and_updates():
    containers = [
        ContainerInfo(id=1, container_number="CONT1", line_id=1, current_dates={}),
        ContainerInfo(id=2, container_number="CONT2", line_id=1, current_dates={}),
        ContainerInfo(id=3, container_number="CONT3", line_id=1, current_dates={}),
    ]
    firebird = DummyFirebirdManager(containers)
    cache = DummyCache()
    config = Config(database=FirebirdDatabaseConfig(database="test.fdb", password="pass"))
    engine = ContainerTrackingEngine(config, cache, firebird)

    order_map = {"CONT1": "ORD1", "CONT2": "ORD1", "CONT3": "ORD2"}
    engine.api_client.find_order_by_container = AsyncMock(side_effect=lambda s, cn: order_map[cn])
    engine.api_client.get_order_tracking = AsyncMock(return_value={"data": []})
    engine._data_unchanged = lambda cached, current: False

    async def dummy_process_single_container(self, session, container, order_id, order_data, prefer_earliest=False):
        res = TrackingResult(container_number=container.container_number)
        res.order_id = order_id
        res.last_event = ContainerEvent(date="2024-01-01", operation="Load", location="Test")
        return res

    engine._process_single_container = types.MethodType(dummy_process_single_container, engine)

    order_calls = []
    original_process_order_group = engine._process_order_group

    async def spy_process_order_group(self, session, order_id, conts, use_railway_mappings=False):
        order_calls.append((order_id, len(conts)))
        return await original_process_order_group(session, order_id, conts, use_railway_mappings)

    engine._process_order_group = types.MethodType(spy_process_order_group, engine)

    stats = await engine.run_full_workflow(batch_size=10)

    assert stats.containers_loaded == 3
    assert stats.records_written == 3
    assert stats.orders_processed == 2
    assert sorted(order_calls) == [("ORD1", 2), ("ORD2", 1)]
    assert [cid for cid, _ in firebird.updated] == [1, 2, 3]


@pytest.mark.asyncio
async def test_multiple_events_per_container():
    container = ContainerInfo(id=1, container_number="CONT1", line_id=1, current_dates={})
    firebird = DummyFirebirdManager([container])
    cache = DummyCache()
    config = Config(database=FirebirdDatabaseConfig(database="test.fdb", password="pass"))
    engine = ContainerTrackingEngine(config, cache, firebird)

    engine.api_client.find_order_by_container = AsyncMock(return_value="ORD1")
    engine.api_client.get_order_tracking = AsyncMock(return_value={"data": []})
    engine._data_unchanged = lambda cached, current: False

    event1 = ContainerEvent(date="2024-01-01", operation="Load", location="A")
    event2 = ContainerEvent(date="2024-01-02", operation="Unload", location="B")

    async def dummy_process_single_container(self, session, container, order_id, order_data, prefer_earliest=False):
        res = TrackingResult(container_number=container.container_number)
        res.order_id = order_id
        res.last_event = event2
        res.events = [event1, event2]
        return res

    engine._process_single_container = types.MethodType(dummy_process_single_container, engine)

    await engine.run_full_workflow(batch_size=10)

    assert len(firebird.updated) == 1
    cid, events = firebird.updated[0]
    assert cid == 1
    assert [e.operation for e in events] == ["Load", "Unload"]

@pytest.mark.asyncio
async def test_skip_update_if_existing_date_is_earlier():
    container = ContainerInfo(
        id=10,
        container_number="CONT4",
        line_id=1,
        current_dates={"DATE_RAILWAY_LOADING": "2024-01-01"},
    )
    firebird = DummyFirebirdManager([container])
    mapping = MagicMock()
    mapping.entity_column = "DATE_RAILWAY_LOADING"
    mapping.column_datatype = "DATE"
    firebird.operation_matcher.find_best_mapping.return_value = mapping
    cache = DummyCache()
    config = Config(database=FirebirdDatabaseConfig(database="test.fdb", password="pass"))
    engine = ContainerTrackingEngine(config, cache, firebird)

    engine.api_client.find_order_by_container = AsyncMock(return_value="ORD1")
    engine.api_client.get_order_tracking = AsyncMock(return_value={"data": []})
    engine._data_unchanged = lambda cached, current: False

    async def dummy_process_single_container(self, session, container, order_id, order_data, prefer_earliest=False):
        res = TrackingResult(container_number=container.container_number)
        res.order_id = order_id
        res.last_event = ContainerEvent(
            date="2024-02-01",
            operation="Отправление вагона со станции",
            location="Test",
        )
        return res

    engine._process_single_container = types.MethodType(dummy_process_single_container, engine)

    await engine.run_full_workflow(batch_size=10)

    assert firebird.updated == []

@pytest.mark.asyncio
async def test_skip_container_with_no_order():
    containers = [
        ContainerInfo(id=1, container_number="CONT1", line_id=1, current_dates={}),
    ]
    firebird = DummyFirebirdManager(containers)
    cache = DummyCache()
    config = Config(database=FirebirdDatabaseConfig(database="test.fdb", password="pass"))
    engine = ContainerTrackingEngine(config, cache, firebird)

    engine.api_client.find_order_by_container = AsyncMock(return_value=None)
    stats = await engine.run_full_workflow(batch_size=10)

    # Container should be marked and skipped
    assert stats.containers_loaded == 1
    assert stats.orders_processed == 0
    assert await engine.binding_manager.is_container_no_order("CONT1") is True


@pytest.mark.asyncio
async def test_skip_already_processed_container():
    containers = [
        ContainerInfo(id=1, container_number="CONT1", line_id=1, current_dates={}),
    ]
    firebird = DummyFirebirdManager(containers)
    cache = DummyCache()
    config = Config(database=FirebirdDatabaseConfig(database="test.fdb", password="pass"))
    engine = ContainerTrackingEngine(config, cache, firebird)

    await engine.binding_manager.mark_container_processed("CONT1")

    async def filtered_get_containers(self, batch_size=100, target_line_ids=None):
        if await engine.binding_manager.is_container_processed("CONT1"):
            yield []
        else:
            yield self.containers

    firebird.get_containers_for_processing = types.MethodType(
        filtered_get_containers, firebird
    )

    engine.api_client.find_order_by_container = AsyncMock(return_value="ORD1")
    engine.api_client.get_order_tracking = AsyncMock(return_value={"data": []})
    engine._data_unchanged = lambda cached, current: False

    engine._process_single_container = AsyncMock()

    stats = await engine.run_full_workflow(batch_size=10)

    assert engine._process_single_container.call_count == 0
    assert stats.containers_processed == 0
    assert firebird.updated == []

@pytest.mark.asyncio
async def test_sequential_passes_switch_mappings():
    containers = [
        ContainerInfo(id=1, container_number="CONT1", line_id=1, current_dates={}),
    ]
    firebird = DummyFirebirdManager(containers)
    cache = DummyCache()
    config = Config(database=FirebirdDatabaseConfig(database="test.fdb", password="pass"))
    engine = ContainerTrackingEngine(config, cache, firebird)

    engine.api_client.find_order_by_container = AsyncMock(return_value="ORD1")
    engine.api_client.get_order_tracking = AsyncMock(return_value={"data": []})
    engine._data_unchanged = lambda cached, current: False

    async def dummy_process_single_container(self, session, container, order_id, order_data, prefer_earliest=False):
        res = TrackingResult(container_number=container.container_number)
        res.order_id = order_id
        res.last_event = ContainerEvent(date="2024-01-01", operation="Load", location="Test")
        return res

    engine._process_single_container = types.MethodType(dummy_process_single_container, engine)

    await engine.run_full_workflow(batch_size=10, target_line_ids={1})
    await engine.run_full_workflow(batch_size=10, target_railway_carrier_ids={2})

    assert firebird.processing_called
    assert firebird.contractor_called
    firebird.operation_matcher.set_railway_mode.assert_has_calls([call(False), call(True)])


@pytest.mark.asyncio
async def test_prefer_earliest_passed_for_railway_mode():
    container = ContainerInfo(id=1, container_number="CONT1", line_id=1, current_dates={})
    firebird = DummyFirebirdManager([container])
    cache = DummyCache()
    config = Config(database=FirebirdDatabaseConfig(database="test.fdb", password="pass"))
    engine = ContainerTrackingEngine(config, cache, firebird)

    engine.api_client.find_order_by_container = AsyncMock(return_value="ORD1")
    engine.api_client.get_order_tracking = AsyncMock(return_value={"data": []})
    engine._data_unchanged = lambda cached, current: False

    engine._process_single_container = AsyncMock(
        return_value=TrackingResult(
            container_number="CONT1",
            last_event=ContainerEvent(date="2024-01-01", operation="Load"),
        )
    )

    await engine.run_full_workflow(batch_size=10, target_railway_carrier_ids={2})

    engine._process_single_container.assert_called()
    assert engine._process_single_container.call_args.kwargs.get("prefer_earliest") is True