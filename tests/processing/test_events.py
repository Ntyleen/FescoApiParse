import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
import pytest

from processing.events import EventProcessor
from models.container_event import ContainerEvent


@pytest.fixture
def processor():
    return EventProcessor()


@pytest.fixture
def sample_order_data():
    return {
        "data": [
            {
                "orderNumber": "ORD123",
                "containers": [
                    {
                        "containerNumber": "CONT1",
                        "lastEvent": {
                            "date": "2024-01-01 10:00:00",
                            "location": "Vladivostok",
                            "text": "Прибыл",
                            "remainingDistance": "1000",
                        },
                    }
                ],
            }
        ]
    }


@pytest.fixture
def sample_container_data():
    return {
        "data": [
            {
                "date": "2024-01-01 10:00:00",
                "type": "ARRIVED",
                "location": "Vladivostok",
                "operation": "Прибыл",
                "transport": "Ship",
                "remainingDistance": "1000",
            }
        ]
    }


@pytest.fixture
def different_container_data():
    return {
        "data": [
            {
                "date": "2024-01-02 12:00:00",
                "type": "LOADED",
                "location": "Vladivostok",
                "operation": "Погружен",
                "transport": "Ship",
                "remainingDistance": "500",
            }
        ]
    }


def test_extract_order_events(processor, sample_order_data):
    events = processor.extract_order_events(sample_order_data, "ORD123", "CONT1")
    assert len(events) == 1
    event = events[0]
    assert event.date == "2024-01-01 10:00:00"
    assert event.location == "Vladivostok"
    assert event.operation == "Прибыл"


def test_extract_container_events(processor, sample_container_data):
    events = processor.extract_container_events(sample_container_data)
    assert len(events) == 1
    event = events[0]
    assert event.type == "ARRIVED"
    assert event.transport == "Ship"


def test_merge_only_order_events(processor, sample_order_data):
    order_events = processor.extract_order_events(sample_order_data, "ORD123", "CONT1")
    merged, dedup, source = processor.merge_and_deduplicate(order_events, [])
    assert source == "order"
    assert dedup is False
    assert len(merged) == 1
    assert isinstance(merged[0], ContainerEvent)


def test_merge_only_container_events(processor, sample_container_data):
    container_events = processor.extract_container_events(sample_container_data)
    merged, dedup, source = processor.merge_and_deduplicate([], container_events)
    assert source == "container"
    assert dedup is False
    assert len(merged) == 1
    assert isinstance(merged[0], ContainerEvent)


def test_merge_duplicate_events(processor, sample_order_data, sample_container_data):
    order_events = processor.extract_order_events(sample_order_data, "ORD123", "CONT1")
    container_events = processor.extract_container_events(sample_container_data)
    merged, dedup, source = processor.merge_and_deduplicate(order_events, container_events)
    assert source == "merged"
    assert dedup is True
    assert len(merged) == 1
    assert merged[0].transport == "Ship"


def test_merge_different_events_prefers_container(processor, sample_order_data, different_container_data):
    order_events = processor.extract_order_events(sample_order_data, "ORD123", "CONT1")
    container_events = processor.extract_container_events(different_container_data)
    merged, dedup, source = processor.merge_and_deduplicate(order_events, container_events)
    assert source == "merged"
    assert dedup is False
    assert len(merged) == 2
    assert merged[-1].operation == "Погружен"


def test_get_best_event_with_malformed_dates(processor):
    events = [
        ContainerEvent(date="2024-01-01 10:00:00", operation="old"),
        ContainerEvent(date="invalid-date", operation="bad"),
        ContainerEvent(date="2024-05-10 15:30:00", operation="new"),
        ContainerEvent(date=None, operation="missing"),
    ]
    best = processor._get_best_event(events, prefer_earliest=False)
    assert best.operation == "new"


def test_get_best_event_prefers_earliest_when_flag(processor):
    events = [
        ContainerEvent(date="2024-01-02 12:00:00", operation="late"),
        ContainerEvent(date="2024-01-01 09:00:00", operation="early"),
    ]
    best = processor._get_best_event(events, prefer_earliest=True)
    assert best.operation == "early"


def test_choose_better_event_more_fields(processor):
    less = ContainerEvent(date="2024-01-01", location="Loc")
    more = ContainerEvent(date="2024-01-01", location="Loc", operation="Op", transport="Ship")
    chosen = processor._choose_better_event(less, more)
    assert chosen is more


def test_choose_better_event_prefers_transport_on_tie(processor):
    no_transport = ContainerEvent(date="2024-01-01", location="Loc", operation="Op", remainingDistance="10")
    with_transport = ContainerEvent(date="2024-01-01", location="Loc", operation="Op", transport="Train")
    chosen = processor._choose_better_event(no_transport, with_transport)
    assert chosen is with_transport