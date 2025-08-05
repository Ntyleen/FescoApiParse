import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from models.container_event import ContainerEvent, TrackingResult


def test_is_empty_default():
    event = ContainerEvent()
    assert event.is_empty()


def test_is_empty_with_fields():
    event = ContainerEvent(date="2024-01-01", location="Port", operation="Loaded")
    assert not event.is_empty()


def test_matches_identical():
    e1 = ContainerEvent(date="2024-01-01", location="Port", operation="Loaded")
    e2 = ContainerEvent(date="2024-01-01", location="Port", operation="Loaded")
    assert e1.matches(e2)


def test_matches_different():
    e1 = ContainerEvent(date="2024-01-01", location="Port", operation="Loaded")
    e2 = ContainerEvent(date="2024-02-02", location="Port", operation="Loaded")
    assert not e1.matches(e2)


def test_tracking_result_success():
    non_empty_event = ContainerEvent(date="2024-01-01", location="Port", operation="Loaded")

    result_ok = TrackingResult(container_number="CNT", last_event=non_empty_event)
    assert result_ok.success

    result_no_event = TrackingResult(container_number="CNT")
    assert not result_no_event.success

    result_error = TrackingResult(container_number="CNT", last_event=non_empty_event, error_message="failed")
    assert not result_error.success

    result_error_only = TrackingResult(container_number="CNT", error_message="failed")
    assert not result_error_only.success
