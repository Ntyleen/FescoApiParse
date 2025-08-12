import asyncio
import types
import os
import sys
from unittest.mock import AsyncMock, MagicMock

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
        self.operation_matcher = MagicMock(find_best_mapping=MagicMock(return_value=mapping))

    async def test_connection(self):
        return True

    async def get_containers_for_processing(self, batch_size=100, target_line_ids=None, target_carrier_ids=None):
        yield self.containers

    async def update_container_from_tracking(self, container_id, tracking_result):
        self.updated.append(container_id)
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

    async def dummy_process_single_container(self, session, container, order_id, order_data):
        res = TrackingResult(container_number=container.container_number)
        res.order_id = order_id
        res.last_event = ContainerEvent(date="2024-01-01", operation="Load", location="Test")
        return res

    engine._process_single_container = types.MethodType(dummy_process_single_container, engine)

    order_calls = []
    original_process_order_group = engine._process_order_group

    async def spy_process_order_group(self, session, order_id, conts):
        order_calls.append((order_id, len(conts)))
        return await original_process_order_group(session, order_id, conts)

    engine._process_order_group = types.MethodType(spy_process_order_group, engine)

    stats = await engine.run_full_workflow(batch_size=10)

    assert stats.containers_loaded == 3
    assert stats.records_written == 3
    assert stats.orders_processed == 2
    assert sorted(order_calls) == [("ORD1", 2), ("ORD2", 1)]
    assert firebird.updated == [1, 2, 3]

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

    async def dummy_process_single_container(self, session, container, order_id, order_data):
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
async def test_date_in_override_do1():
    container = ContainerInfo(
        id=5,
        container_number="CONT5",
        line_id=1,
        current_dates={"DATE_IN": "2025-08-01 10:00"},
        processing_flags={"date_in_source": "Прием с моря"},
    )

    class Do1FirebirdManager(DummyFirebirdManager):
        def __init__(self):
            super().__init__([container])
            mapping = MagicMock()
            mapping.entity_column = "DATE_IN"
            mapping.column_datatype = "TIMESTAMP"
            self.operation_matcher.find_best_mapping.return_value = mapping
            self.entity_config.date_in = "DATE_IN"
            self.entity_config.remaining_distance = "TRACING_DAYS"
            self.transformer = MagicMock(transform_value=lambda v, t: v)

        async def update_container_from_tracking(self, container_id, tracking_result):
            self.updated.append(container_id)
            return True

    firebird = Do1FirebirdManager()
    cache = DummyCache()
    config = Config(database=FirebirdDatabaseConfig(database="test.fdb", password="pass"))
    engine = ContainerTrackingEngine(config, cache, firebird)

    tr = TrackingResult(container_number="CONT5")
    tr.last_event = ContainerEvent(
        date="2025-08-02 12:00",
        operation="Регистрация ДО1",
        location="Test",
    )

    await engine._write_results_to_firebird([(container, tr)])

    assert firebird.updated == [5]
    assert container.current_dates["DATE_IN"] == "2025-08-02 12:00"


@pytest.mark.asyncio
async def test_date_in_no_override_after_do1():
    container = ContainerInfo(
        id=6,
        container_number="CONT6",
        line_id=1,
        current_dates={"DATE_IN": "2025-08-01 10:00"},
        processing_flags={"date_in_source": "Прием с моря"},
    )

    class Do1FirebirdManager(DummyFirebirdManager):
        def __init__(self):
            super().__init__([container])
            mapping = MagicMock()
            mapping.entity_column = "DATE_IN"
            mapping.column_datatype = "TIMESTAMP"
            self.operation_matcher.find_best_mapping.return_value = mapping
            self.entity_config.date_in = "DATE_IN"
            self.entity_config.remaining_distance = "TRACING_DAYS"
            self.transformer = MagicMock(transform_value=lambda v, t: v)

        async def update_container_from_tracking(self, container_id, tracking_result):
            self.updated.append(container_id)
            return True

    firebird = Do1FirebirdManager()
    cache = DummyCache()
    config = Config(database=FirebirdDatabaseConfig(database="test.fdb", password="pass"))
    engine = ContainerTrackingEngine(config, cache, firebird)

    first = TrackingResult(container_number="CONT6")
    first.last_event = ContainerEvent(
        date="2025-08-02 12:00",
        operation="Регистрация ДО1",
        location="Test",
    )
    await engine._write_results_to_firebird([(container, first)])

    second = TrackingResult(container_number="CONT6")
    second.last_event = ContainerEvent(
        date="2025-08-04 09:00",
        operation="Регистрация ДО1",
        location="Test",
    )
    await engine._write_results_to_firebird([(container, second)])

    assert firebird.updated == [6]
    assert container.current_dates["DATE_IN"] == "2025-08-02 12:00"


@pytest.mark.asyncio
async def test_remaining_distance_updates_only_when_changed():
    container = ContainerInfo(
        id=7,
        container_number="CONT7",
        line_id=1,
        current_dates={},
        remaining_distance=100,
    )

    class RdFirebirdManager(DummyFirebirdManager):
        def __init__(self):
            super().__init__([container])
            self.operation_matcher.find_best_mapping.return_value = None
            self.entity_config.remaining_distance = "TRACING_DAYS"
            self.transformer = MagicMock(transform_value=lambda v, t: v)

        async def update_container_from_tracking(self, container_id, tracking_result):
            self.updated.append(container_id)
            container.remaining_distance = tracking_result.last_event.remainingDistance
            return True

    firebird = RdFirebirdManager()
    cache = DummyCache()
    config = Config(database=FirebirdDatabaseConfig(database="test.fdb", password="pass"))
    engine = ContainerTrackingEngine(config, cache, firebird)

    same = TrackingResult(container_number="CONT7")
    same.last_event = ContainerEvent(
        date="2024-01-01",
        operation="X",
        location="L",
        remainingDistance=100,
    )
    await engine._write_results_to_firebird([(container, same)])
    assert firebird.updated == []

    changed = TrackingResult(container_number="CONT7")
    changed.last_event = ContainerEvent(
        date="2024-01-02",
        operation="X",
        location="L",
        remainingDistance=80,
    )
    await engine._write_results_to_firebird([(container, changed)])
    assert firebird.updated == [7]
    assert container.remaining_distance == 80