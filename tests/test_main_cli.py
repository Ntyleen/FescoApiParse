import sys
import subprocess
import asyncio

import pytest

import main


@pytest.fixture
def dummy_tracker(monkeypatch):
    calls = {
        "initialized": False,
        "test": None,
        "file": None,
        "db": None,
        "monitor": False,
    }

    class DummyTracker:
        def __init__(self, environment="development"):
            self.environment = environment

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

    monkeypatch.setattr(main, "FescoTracker", DummyTracker)
    return calls


def run_main(args):
    """Run the CLI with provided arguments using the patched ``main`` module."""
    sys.argv = ["main"] + args
    asyncio.run(main.main())


def test_help_subprocess():
    result = subprocess.run([
        sys.executable,
        "-m",
        "main",
        "--help",
    ], capture_output=True, text=True, check=True)

    assert "FESCO Container Tracking System" in result.stdout


def test_test_mode_invokes(dummy_tracker):
    run_main(["test", "--count", "3"])
    assert dummy_tracker["initialized"]
    assert dummy_tracker["test"] == 3


def test_file_mode_invokes(tmp_path, dummy_tracker):
    f = tmp_path / "containers.txt"
    f.write_text("CNT1\nCNT2")
    run_main(["file", str(f)])
    assert dummy_tracker["initialized"]
    assert dummy_tracker["file"] == str(f)


def test_db_mode_invokes(dummy_tracker):
    run_main(["db", "--batch-size", "42"])
    assert dummy_tracker["initialized"]
    assert dummy_tracker["db"] == 42


def test_monitor_mode_invokes(dummy_tracker):
    run_main(["monitor"])
    assert dummy_tracker["initialized"]
    assert dummy_tracker["monitor"]
