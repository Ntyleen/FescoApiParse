import asyncio
import logging
import re
import sys
from pathlib import Path
import importlib

import pytest

ROOT_DIR = Path(__file__).resolve().parents[2]
PARENT_DIR = ROOT_DIR.parent
if str(PARENT_DIR) not in sys.path:
    sys.path.insert(0, str(PARENT_DIR))

logging_module = importlib.import_module("FescoApiParse.utils.logging")

log_execution_time = logging_module.log_execution_time
create_container_logger = logging_module.create_container_logger
create_api_logger = logging_module.create_api_logger


def test_log_execution_time_sync(caplog):
    logger = logging.getLogger("sync_test")
    caplog.set_level(logging.DEBUG, logger="sync_test")

    @log_execution_time(logger)
    def dummy_sync():
        return "done"

    dummy_sync()

    messages = [record.getMessage() for record in caplog.records if record.name == "sync_test"]
    assert any("🚀 Запуск dummy_sync" in msg for msg in messages)
    assert any(re.search(r"✅ dummy_sync завершен за \d+\.\d+s", msg) for msg in messages)


def test_log_execution_time_async(caplog):
    logger = logging.getLogger("async_test")
    caplog.set_level(logging.DEBUG, logger="async_test")

    @log_execution_time(logger)
    async def dummy_async():
        await asyncio.sleep(0.01)
        return "done"

    asyncio.run(dummy_async())

    messages = [record.getMessage() for record in caplog.records if record.name == "async_test"]
    assert any("🚀 Запуск dummy_async" in msg for msg in messages)
    assert any(re.search(r"✅ dummy_async завершен за \d+\.\d+s", msg) for msg in messages)


def test_create_loggers():
    container_logger = create_container_logger("123")
    api_logger = create_api_logger("/ping")

    assert isinstance(container_logger, logging.LoggerAdapter)
    assert isinstance(api_logger, logging.LoggerAdapter)
    assert container_logger.extra.get("container") == "123"
    assert api_logger.extra.get("endpoint") == "/ping"

