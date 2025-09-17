import asyncio
import subprocess
import sys

import pytest

import cli


@pytest.fixture
def dummy_tracker(monkeypatch):
    calls = {
        "initialized": False,
        "test": None,
        "file": None,
        "db": None,
        "monitor": False,
        "schedule": None,
    }

    class DummyTracker:
        def __init__(self, environment="development"):
            self.environment = environment
            self.config = type(
                "Cfg",
                (),
                {
                    "scheduler": type("S", (), {"cron": "0 * * * *"})(),
                    "processing": type("P", (), {"batch_size": 10})(),
                },
            )()

        async def initialize(self):
            calls["initialized"] = True

        async def run_test_mode(self, count):
            calls["test"] = count

        async def run_file_mode(self, file_path):
            calls["file"] = file_path

        async def run_db_mode(self, batch_size):
            calls["db"] = batch_size

        async def run_monitor_mode(self):
            calls["monitor"] = True

        async def schedule_db_mode(self, cron, batch_size):
            calls["schedule"] = (cron, batch_size)

    monkeypatch.setattr(cli, "FescoTracker", DummyTracker)
    return calls


def run_cli(args):
    sys.argv = ["cli"] + args
    return asyncio.run(cli.run_cli())


def test_help_subprocess():
    result = subprocess.run(
        [sys.executable, "-m", "cli", "--help"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert "FESCO Container Tracking System" in result.stdout


def test_test_mode_invokes(dummy_tracker):
    code = run_cli(["test", "--count", "3"])
    assert code == 0
    assert dummy_tracker["initialized"]
    assert dummy_tracker["test"] == 3


def test_file_mode_invokes(tmp_path, dummy_tracker):
    f = tmp_path / "containers.txt"
    f.write_text("CNT1\nCNT2")
    code = run_cli(["file", str(f)])
    assert code == 0
    assert dummy_tracker["initialized"]
    assert dummy_tracker["file"] == str(f)


def test_db_mode_invokes(dummy_tracker):
    code = run_cli(["db", "--batch-size", "42"])
    assert code == 0
    assert dummy_tracker["initialized"]
    assert dummy_tracker["db"] == 42


def test_monitor_mode_invokes(dummy_tracker):
    code = run_cli(["monitor"])
    assert code == 0
    assert dummy_tracker["initialized"]
    assert dummy_tracker["monitor"]


def test_schedule_mode_invokes(dummy_tracker):
    code = run_cli(["schedule", "--cron", "*/5 * * * *", "--batch-size", "20"])
    assert code == 0
    assert dummy_tracker["initialized"]
    assert dummy_tracker["schedule"] == ("*/5 * * * *", 20)
