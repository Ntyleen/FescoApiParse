import asyncio
import types
import os
import sys
from unittest.mock import AsyncMock, MagicMock

import pytest
from datetime import datetime, timezone

# Ensure project root is on sys.path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from config.settings import Config, FirebirdDatabaseConfig
from models.container_event import TrackingResult, ContainerEvent
from processing.ContainerTrackingEngine import ContainerTrackingEngine
from utils.db.firebird_manager import ContainerInfo
from cache.cache_base import CacheBackend


class SimpleTransformer:
    def transform_value(self, value, datatype):
        if datatype == "INTEGER":
            try:
                return int(value)
            except Exception:
                return None
        if datatype in ("DATE", "TIMESTAMP"):
            try:
                dt = datetime.fromisoformat(value)
                if dt.tzinfo:
                    dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
                return dt if datatype == "TIMESTAMP" else dt.date()
            except Exception:
                return None
        return value


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
        self.entity_config = MagicMock(
            date_railway_loading="DATE_RAILWAY_LOADING",
            date_in="DATE_IN",
            remaining_distance="TRACING_DAYS",
            railway_carrier_column="LEGAL_PERSON_RAILWAY_CARRIER_ID",
        )
        mapping = MagicMock()
        mapping.entity_column = "DATE_ETA"
        mapping.column_datatype = "DATE"
        self.operation_matcher = MagicMock(find_best_mapping=MagicMock(return_value=mapping))
        self.transformer = SimpleTransformer()

    async def test_connection(self):
        return True

    async def get_containers_for_processing(self, batch_size=100, target_ids=None, selection_column=None):
        yield self.containers

    async def update_container_from_tracking(self, container_id, tracking_result):
        self.updated.append((container_id, getattr(tracking_result.last_event, "remainingDistance", None)))
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
    assert firebird.updated == [(1, None), (2, None), (3, None)]

@pytest.mark.asyncio
async def test_date_mapping_earliest_wins_simple_case():
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
async def test_update_remaining_distance_when_date_skipped():
    container = ContainerInfo(
        id=20,
        container_number="CONT5",
        line_id=1,
        current_dates={"DATE_RAILWAY_LOADING": "2024-01-01"},
        remaining_distance=10000,
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
            remainingDistance="500",
        )
        return res

    engine._process_single_container = types.MethodType(dummy_process_single_container, engine)

    await engine.run_full_workflow(batch_size=10)

    assert firebird.updated == [20]

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


class TwoPassFirebirdManager:
    def __init__(self, line_containers, carrier_containers):
        self.line_containers = line_containers
        self.carrier_containers = carrier_containers
        self.updated = []
        self.entity_config = MagicMock(
            railway_carrier_column="LEGAL_PERSON_RAILWAY_CARRIER_ID",
            date_in="DATE_IN",
            remaining_distance="TRACING_DAYS",
        )
        mapping = MagicMock()
        mapping.entity_column = "DATE_IN"
        mapping.column_datatype = "TIMESTAMP"
        self.operation_matcher = MagicMock(find_best_mapping=MagicMock(return_value=mapping))
        self.transformer = SimpleTransformer()

    async def test_connection(self):
        return True

    async def get_containers_for_processing(self, batch_size=100, target_ids=None, selection_column=None):
        if selection_column == self.entity_config.railway_carrier_column:
            yield list(self.carrier_containers)
        else:
            yield list(self.line_containers)

    async def update_container_from_tracking(self, container_id, tracking_result):
        self.updated.append(container_id)
        return True

    async def get_entity_statistics(self):
        return {"runtime_stats": {"records_updated": len(self.updated)}}

    async def close(self):
        pass


@pytest.mark.asyncio
async def test_two_pass_processing_line_then_carrier_dedup():
    c1 = ContainerInfo(id=1, container_number="CONT1", line_id=1, current_dates={})
    c2 = ContainerInfo(id=2, container_number="CONT2", line_id=None, current_dates={})
    firebird = TwoPassFirebirdManager([c1], [c1, c2])
    cache = DummyCache()
    config = Config(database=FirebirdDatabaseConfig(database="test.fdb", password="pass"))
    engine = ContainerTrackingEngine(config, cache, firebird)

    engine.api_client.find_order_by_container = AsyncMock(return_value="ORD1")
    engine.api_client.get_order_tracking = AsyncMock(return_value={"data": []})
    engine._data_unchanged = lambda cached, current: False

    async def dummy_process_single_container(self, session, container, order_id, order_data):
        res = TrackingResult(container_number=container.container_number)
        res.order_id = order_id
        res.last_event = ContainerEvent(date="2024-01-01", operation="Load")
        return res

    engine._process_single_container = types.MethodType(dummy_process_single_container, engine)

    await engine.run_full_workflow(batch_size=10, target_line_ids={1}, target_carrier_ids={2})

    assert firebird.updated == [1, 2]


@pytest.mark.asyncio
async def test_carrier_only_container_without_line_id_is_processed():
    c3 = ContainerInfo(id=3, container_number="CONT3", line_id=None, current_dates={})
    firebird = TwoPassFirebirdManager([], [c3])
    cache = DummyCache()
    config = Config(database=FirebirdDatabaseConfig(database="test.fdb", password="pass"))
    engine = ContainerTrackingEngine(config, cache, firebird)

    engine.api_client.find_order_by_container = AsyncMock(return_value="ORD1")
    engine.api_client.get_order_tracking = AsyncMock(return_value={"data": []})
    engine._data_unchanged = lambda cached, current: False

    async def dummy_process_single_container(self, session, container, order_id, order_data):
        res = TrackingResult(container_number=container.container_number)
        res.order_id = order_id
        res.last_event = ContainerEvent(date="2024-01-01", operation="Load")
        return res

    engine._process_single_container = types.MethodType(dummy_process_single_container, engine)

    await engine.run_full_workflow(batch_size=10, target_line_ids={1}, target_carrier_ids={2})

    assert firebird.updated == [3]


class CallTrackingFirebirdManager:
    def __init__(self):
        self.calls = []
        self.updated = []
        self.entity_config = MagicMock(railway_carrier_column="LEGAL_PERSON_RAILWAY_CARRIER_ID", date_in="DATE_IN", remaining_distance="TRACING_DAYS")
        self.operation_matcher = MagicMock(find_best_mapping=MagicMock(return_value=None))
        self.transformer = SimpleTransformer()

    async def test_connection(self):
        return True

    async def get_containers_for_processing(self, batch_size=100, target_ids=None, selection_column=None):
        self.calls.append(selection_column)
        yield []

    async def update_container_from_tracking(self, container_id, tracking_result):
        return True

    async def get_entity_statistics(self):
        return {"runtime_stats": {"records_updated": 0}}

    async def close(self):
        pass


@pytest.mark.asyncio
async def test_selection_query_line_pass_only_when_no_carrier_ids():
    firebird = CallTrackingFirebirdManager()
    cache = DummyCache()
    config = Config(database=FirebirdDatabaseConfig(database="test.fdb", password="pass"))
    engine = ContainerTrackingEngine(config, cache, firebird)

    await engine.run_full_workflow(batch_size=10, target_line_ids={1})

    assert firebird.calls == [None]


@pytest.mark.asyncio
async def test_date_in_override_discharged_then_do1_once():
    container = ContainerInfo(id=1, container_number="C1", line_id=1, current_dates={})
    firebird = DummyFirebirdManager([])
    mapping = MagicMock()
    mapping.entity_column = "DATE_IN"
    mapping.column_datatype = "TIMESTAMP"
    firebird.operation_matcher.find_best_mapping.return_value = mapping
    cache = DummyCache()
    config = Config(database=FirebirdDatabaseConfig(database="test.fdb", password="pass"))
    engine = ContainerTrackingEngine(config, cache, firebird)

    tr1 = TrackingResult(container_number="C1", last_event=ContainerEvent(date="2025-08-01 10:00", operation="Прием с моря"))
    await engine._write_results_to_firebird([(container, tr1)])
    assert firebird.updated == [(1, None)]
    assert container.current_dates["DATE_IN"] == "2025-08-01 10:00"

    tr2 = TrackingResult(container_number="C1", last_event=ContainerEvent(date="2025-08-02 12:00", operation="Регистрация ДО1"))
    await engine._write_results_to_firebird([(container, tr2)])
    assert firebird.updated == [(1, None), (1, None)]
    assert container.current_dates["DATE_IN"] == "2025-08-02 12:00"
    assert container.processing_flags["date_in_do1_overridden"] is True


@pytest.mark.asyncio
async def test_date_in_no_override_after_do1():
    container = ContainerInfo(
        id=1,
        container_number="C1",
        line_id=1,
        current_dates={"DATE_IN": "2025-08-02 12:00"},
        processing_flags={"date_in_do1_overridden": True},
    )
    firebird = DummyFirebirdManager([])
    mapping = MagicMock()
    mapping.entity_column = "DATE_IN"
    mapping.column_datatype = "TIMESTAMP"
    firebird.operation_matcher.find_best_mapping.return_value = mapping
    cache = DummyCache()
    config = Config(database=FirebirdDatabaseConfig(database="test.fdb", password="pass"))
    engine = ContainerTrackingEngine(config, cache, firebird)

    tr = TrackingResult(container_number="C1", last_event=ContainerEvent(date="2025-08-03 15:00", operation="Регистрация ДО1"))
    await engine._write_results_to_firebird([(container, tr)])
    assert firebird.updated == []
    assert container.current_dates["DATE_IN"] == "2025-08-02 12:00"


@pytest.mark.asyncio
async def test_remaining_distance_updates_only_on_change_and_is_latest():
    container = ContainerInfo(id=1, container_number="C1", line_id=1, current_dates={}, remaining_distance=5)
    firebird = DummyFirebirdManager([])
    firebird.operation_matcher.find_best_mapping.return_value = None
    cache = DummyCache()
    config = Config(database=FirebirdDatabaseConfig(database="test.fdb", password="pass"))
    engine = ContainerTrackingEngine(config, cache, firebird)

    tr1 = TrackingResult(container_number="C1", last_event=ContainerEvent(date="2024-01-01", operation="Op", remainingDistance="5"))
    await engine._write_results_to_firebird([(container, tr1)])
    assert firebird.updated == []

    tr2 = TrackingResult(container_number="C1", last_event=ContainerEvent(date="2024-01-02", operation="Op", remainingDistance="3"))
    await engine._write_results_to_firebird([(container, tr2)])
    assert firebird.updated == [(1, '3')]
    assert container.remaining_distance == 3

    tr3 = TrackingResult(container_number="C1", last_event=ContainerEvent(date="2024-01-03", operation="Op", remainingDistance="3"))
    await engine._write_results_to_firebird([(container, tr3)])
    assert firebird.updated == [(1, '3')]

    tr4 = TrackingResult(container_number="C1", last_event=ContainerEvent(date="2024-01-04", operation="Op", remainingDistance="6"))
    await engine._write_results_to_firebird([(container, tr4)])
    assert firebird.updated == [(1, '3')]
    assert container.remaining_distance == 3


@pytest.mark.asyncio
async def test_remaining_distance_zero_cached_and_written():
    container = ContainerInfo(id=1, container_number="C1", line_id=1, current_dates={}, remaining_distance=5)
    firebird = DummyFirebirdManager([container])
    firebird.operation_matcher.find_best_mapping.return_value = None
    cache = DummyCache()
    config = Config(database=FirebirdDatabaseConfig(database="test.fdb", password="pass"))
    engine = ContainerTrackingEngine(config, cache, firebird)

    engine.api_client.find_order_by_container = AsyncMock(return_value="ORD1")
    engine.api_client.get_order_tracking = AsyncMock(
        return_value={
            "data": [
                {
                    "containers": [
                        {
                            "containerNumber": "C1",
                            "lastEvent": {
                                "date": "2024-01-01",
                                "text": "Op",
                                "location": "Loc",
                                "remainingDistance": 0,
                            },
                        }
                    ]
                }
            ]
        }
    )

    async def dummy_process_single_container(self, session, cont, order_id, order_data):
        return TrackingResult(
            container_number=cont.container_number,
            last_event=ContainerEvent(
                date="2024-01-01",
                operation="Op",
                location="Loc",
                remainingDistance=0,
            ),
        )

    engine._process_single_container = types.MethodType(dummy_process_single_container, engine)

    await engine.run_full_workflow(batch_size=10)

    assert firebird.updated == [(1, 0)]
    assert container.remaining_distance == 0
    cached = await cache.get("order_last_check:ORD1")
    assert cached["C1"]["remainingDistance"] == "0"


@pytest.mark.asyncio
async def test_remaining_distance_zero_without_other_fields():
    container = ContainerInfo(id=1, container_number="C1", line_id=1, current_dates={}, remaining_distance=5)
    firebird = DummyFirebirdManager([container])
    firebird.operation_matcher.find_best_mapping.return_value = None
    cache = DummyCache()
    config = Config(database=FirebirdDatabaseConfig(database="test.fdb", password="pass"))
    engine = ContainerTrackingEngine(config, cache, firebird)

    engine.api_client.find_order_by_container = AsyncMock(return_value="ORD1")
    engine.api_client.get_order_tracking = AsyncMock(
        return_value={
            "data": [
                {
                    "containers": [
                        {"containerNumber": "C1", "lastEvent": {}}
                    ]
                }
            ]
        }
    )
    engine.api_client.get_container_tracking = AsyncMock(
        return_value={"data": [{"remainingDistance": 0}]}
    )

    await engine.run_full_workflow(batch_size=10)

    assert firebird.updated == [(1, 0)]
    cached = await cache.get("order_last_check:ORD1")
    assert cached["C1"]["remainingDistance"] == "0"


@pytest.mark.asyncio
async def test_date_generic_earliest_wins_across_multiple_events():
    container = ContainerInfo(id=1, container_number="C1", line_id=1, current_dates={})
    firebird = DummyFirebirdManager([])
    mapping = MagicMock()
    mapping.entity_column = "DATE_ETA"
    mapping.column_datatype = "DATE"
    firebird.operation_matcher.find_best_mapping.return_value = mapping
    cache = DummyCache()
    config = Config(database=FirebirdDatabaseConfig(database="test.fdb", password="pass"))
    engine = ContainerTrackingEngine(config, cache, firebird)

    events = [
        ContainerEvent(date="2024-03-01", operation="Op"),
        ContainerEvent(date="2024-02-01", operation="Op"),
    ]
    tr = TrackingResult(container_number="C1", last_event=events[0], events=events)
    await engine._write_results_to_firebird([(container, tr)])

    assert container.current_dates["DATE_ETA"] == "2024-02-01"
    assert firebird.updated == [(1, None)]


@pytest.mark.asyncio
async def test_date_generic_no_update_if_min_date_later_than_existing():
    container = ContainerInfo(
        id=1,
        container_number="C1",
        line_id=1,
        current_dates={"DATE_ETA": "2024-01-01"},
    )
    firebird = DummyFirebirdManager([])
    mapping = MagicMock()
    mapping.entity_column = "DATE_ETA"
    mapping.column_datatype = "DATE"
    firebird.operation_matcher.find_best_mapping.return_value = mapping
    cache = DummyCache()
    config = Config(database=FirebirdDatabaseConfig(database="test.fdb", password="pass"))
    engine = ContainerTrackingEngine(config, cache, firebird)

    events = [
        ContainerEvent(date="2024-01-02", operation="Op"),
        ContainerEvent(date="2024-01-03", operation="Op"),
    ]
    tr = TrackingResult(container_number="C1", last_event=events[0], events=events)
    await engine._write_results_to_firebird([(container, tr)])

    assert container.current_dates["DATE_ETA"] == "2024-01-01"
    assert firebird.updated == []


@pytest.mark.asyncio
async def test_date_railway_loading_min_date_selected():
    container = ContainerInfo(id=1, container_number="C1", line_id=1, current_dates={})
    firebird = DummyFirebirdManager([])
    mapping = MagicMock()
    mapping.entity_column = "DATE_RAILWAY_LOADING"
    mapping.column_datatype = "DATE"
    firebird.operation_matcher.find_best_mapping.return_value = mapping
    cache = DummyCache()
    config = Config(database=FirebirdDatabaseConfig(database="test.fdb", password="pass"))
    engine = ContainerTrackingEngine(config, cache, firebird)

    events = [
        ContainerEvent(date="2024-02-10", operation="Op"),
        ContainerEvent(date="2024-01-05", operation="Op"),
        ContainerEvent(date="2024-01-20", operation="Op"),
    ]
    tr = TrackingResult(container_number="C1", last_event=events[0], events=events)
    await engine._write_results_to_firebird([(container, tr)])

    assert container.current_dates["DATE_RAILWAY_LOADING"] == "2024-01-05"
    assert firebird.updated == [(1, None)]


@pytest.mark.asyncio
async def test_date_in_remains_two_step_override():
    container = ContainerInfo(id=1, container_number="C1", line_id=1, current_dates={})
    firebird = DummyFirebirdManager([])
    mapping = MagicMock()
    mapping.entity_column = "DATE_IN"
    mapping.column_datatype = "TIMESTAMP"
    firebird.operation_matcher.find_best_mapping.return_value = mapping
    cache = DummyCache()
    config = Config(database=FirebirdDatabaseConfig(database="test.fdb", password="pass"))
    engine = ContainerTrackingEngine(config, cache, firebird)

    events = [
        ContainerEvent(date="2025-08-02 12:00", operation="Регистрация ДО1"),
        ContainerEvent(date="2025-08-01 10:00", operation="Прием с моря"),
    ]
    tr = TrackingResult(container_number="C1", last_event=events[0], events=events)
    await engine._write_results_to_firebird([(container, tr)])

    assert container.current_dates["DATE_IN"] == "2025-08-02 12:00"
    assert firebird.updated == [(1, None), (1, None)]
    assert container.processing_flags["date_in_do1_overridden"] is True


@pytest.mark.asyncio
async def test_tracing_days_unchanged_behavior():
    container = ContainerInfo(id=1, container_number="C1", line_id=1, current_dates={}, remaining_distance=5)
    firebird = DummyFirebirdManager([])
    firebird.operation_matcher.find_best_mapping.return_value = None
    cache = DummyCache()
    config = Config(database=FirebirdDatabaseConfig(database="test.fdb", password="pass"))
    engine = ContainerTrackingEngine(config, cache, firebird)

    tr1 = TrackingResult(container_number="C1", last_event=ContainerEvent(date="2024-01-01", operation="Op", remainingDistance="5"))
    await engine._write_results_to_firebird([(container, tr1)])
    assert firebird.updated == []

    tr2 = TrackingResult(container_number="C1", last_event=ContainerEvent(date="2024-01-02", operation="Op", remainingDistance="3"))
    await engine._write_results_to_firebird([(container, tr2)])
    assert firebird.updated == [(1, '3')]
    assert container.remaining_distance == 3


@pytest.mark.asyncio
async def test_idempotency_multiple_runs():
    container = ContainerInfo(id=1, container_number="C1", line_id=1, current_dates={})
    firebird = DummyFirebirdManager([])
    mapping = MagicMock()
    mapping.entity_column = "DATE_ETA"
    mapping.column_datatype = "DATE"
    firebird.operation_matcher.find_best_mapping.return_value = mapping
    cache = DummyCache()
    config = Config(database=FirebirdDatabaseConfig(database="test.fdb", password="pass"))
    engine = ContainerTrackingEngine(config, cache, firebird)

    events = [ContainerEvent(date="2024-02-01", operation="Op"), ContainerEvent(date="2024-03-01", operation="Op")]
    tr = TrackingResult(container_number="C1", last_event=events[0], events=events)
    await engine._write_results_to_firebird([(container, tr)])
    assert firebird.updated == [(1, None)]

    await engine._write_results_to_firebird([(container, tr)])
    assert firebird.updated == [(1, None)]


@pytest.mark.asyncio
async def test_timezone_normalization_correct_comparison():
    container = ContainerInfo(
        id=1,
        container_number="C1",
        line_id=1,
        current_dates={"DATE_ETA": "2024-01-01T00:00:00+03:00"},
    )
    firebird = DummyFirebirdManager([])
    mapping = MagicMock()
    mapping.entity_column = "DATE_ETA"
    mapping.column_datatype = "TIMESTAMP"
    firebird.operation_matcher.find_best_mapping.return_value = mapping
    cache = DummyCache()
    config = Config(database=FirebirdDatabaseConfig(database="test.fdb", password="pass"))
    engine = ContainerTrackingEngine(config, cache, firebird)

    events = [ContainerEvent(date="2023-12-31T21:00:00+00:00", operation="Op")]
    tr = TrackingResult(container_number="C1", last_event=events[0], events=events)
    await engine._write_results_to_firebird([(container, tr)])

    assert container.current_dates["DATE_ETA"] == "2024-01-01T00:00:00+03:00"
    assert firebird.updated == []


def test_extract_order_summary_normalizes_and_handles_zero():
    firebird = DummyFirebirdManager([])
    cache = DummyCache()
    config = Config(database=FirebirdDatabaseConfig(database="test.fdb", password="pass"))
    engine = ContainerTrackingEngine(config, cache, firebird)

    order_data = {
        "data": [
            {
                "orderNumber": "ORD1",
                "containers": [
                    {
                        "containerNumber": "co nt1",
                        "lastEvent": {
                            "date": "2024-01-01",
                            "text": "Load",
                            "location": "Loc",
                            "remainingDistance": 0,
                        },
                    }
                ],
            }
        ]
    }

    summary = engine._extract_order_summary(order_data, ["CONT1"])
    assert summary == {
        "CONT1": {
            "date": "2024-01-01",
            "operation": "Load",
            "location": "Loc",
            "remainingDistance": "0",
        }
    }

    cached = {"CONT1": summary["CONT1"].copy()}
    assert engine._data_unchanged(cached, summary)
