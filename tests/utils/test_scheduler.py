import sys
import os

import pytest

# Ensure project package is importable via FescoApiParse prefix
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
PARENT_DIR = os.path.dirname(ROOT_DIR)
if PARENT_DIR not in sys.path:
    sys.path.insert(0, PARENT_DIR)

import utils.scheduler as scheduler_module
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger


class FakeScheduler:
    def __init__(self):
        self.calls = []

    def add_job(self, func, trigger, args=None, kwargs=None):
        self.calls.append({
            'func': func,
            'trigger': trigger,
            'args': args,
            'kwargs': kwargs,
        })


@pytest.fixture
def fake_scheduler(monkeypatch):
    fake = FakeScheduler()
    monkeypatch.setattr(scheduler_module, 'AsyncIOScheduler', lambda: fake)
    return fake, scheduler_module.FescoScheduler()


def dummy_job():
    """Sample job used for scheduling tests."""
    return None


def test_add_job_uses_cron_trigger(fake_scheduler):
    fake, scheduler = fake_scheduler
    scheduler.add_job(dummy_job, cron='*/5 * * * *')
    assert len(fake.calls) == 1
    assert isinstance(fake.calls[0]['trigger'], CronTrigger)


def test_add_job_uses_interval_trigger(fake_scheduler):
    fake, scheduler = fake_scheduler
    scheduler.add_job(dummy_job, interval_seconds=10)
    assert len(fake.calls) == 1
    assert isinstance(fake.calls[0]['trigger'], IntervalTrigger)


def test_add_job_without_triggers_raises(fake_scheduler):
    _, scheduler = fake_scheduler
    with pytest.raises(ValueError):
        scheduler.add_job(dummy_job)
