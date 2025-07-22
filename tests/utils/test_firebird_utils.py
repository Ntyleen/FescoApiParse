import sys
import os
from datetime import datetime, date

import pytest

# Ensure project package is importable via FescoApiParse prefix
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
PARENT_DIR = os.path.dirname(ROOT_DIR)
if PARENT_DIR not in sys.path:
    sys.path.insert(0, PARENT_DIR)

# Ensure 'utils' package points to project package to avoid clash with tests/utils

def _import_firebird_modules():
    import importlib
    sys.modules["utils"] = importlib.import_module("FescoApiParse.utils")
    sys.modules["models"] = importlib.import_module("FescoApiParse.models")
    fm = importlib.import_module("FescoApiParse.utils.db.firebird_manager")
    di = importlib.import_module("FescoApiParse.utils.db.database_init")
    return fm, di


def test_entity_column_mapping_matches_operation():
    fm, _ = _import_firebird_modules()
    EntityColumnMapping = fm.EntityColumnMapping

    mapping = EntityColumnMapping(
        entity_column="DATE",
        fesco_field="date",
        operation_patterns=("Loaded",),
        priority=0,
    )
    assert mapping.matches_operation("Loaded") == 1.0

    mapping_prio = EntityColumnMapping(
        entity_column="DATE",
        fesco_field="date",
        operation_patterns=("load",),
        priority=50,
    )
    score = mapping_prio.matches_operation("load container")
    assert 0.45 < score < 0.6

    mapping_contains = EntityColumnMapping(
        entity_column="DATE",
        fesco_field="date",
        operation_patterns=("loading container",),
    )
    score = mapping_contains.matches_operation("loading")
    assert pytest.approx(0.26, rel=0.05) == score

    mapping_none = EntityColumnMapping(
        entity_column="DATE",
        fesco_field="date",
        operation_patterns=("discharge",),
    )
    assert mapping_none.matches_operation("load") == 0.0


def test_firebird_date_transformer_transform_value():
    fm, _ = _import_firebird_modules()
    FirebirdDateTransformer = fm.FirebirdDateTransformer
    transformer = FirebirdDateTransformer()

    ts = transformer.transform_value("2024-01-02 03:04:05", "TIMESTAMP")
    assert ts == datetime(2024, 1, 2, 3, 4, 5)

    d = transformer.transform_value("2024-01-02", "DATE")
    assert d == date(2024, 1, 2)

    i = transformer.transform_value("Remaining 123 km", "INTEGER")
    assert i == 123


def test_detect_database_type():
    _, di = _import_firebird_modules()
    detect_database_type = di.detect_database_type

    fb_config = {"database": "data.fdb", "port": 3050, "user": "SYSDBA"}
    mysql_config = {"port": 3306}
    pg_config = {"port": 5432}

    assert detect_database_type(fb_config) == "firebird"
    assert detect_database_type(mysql_config) == "mysql"
    assert detect_database_type(pg_config) == "postgresql"


@pytest.mark.asyncio
async def test_validate_firebird_config():
    fm, _ = _import_firebird_modules()
    validate_firebird_config = fm.validate_firebird_config
    valid = {
        "host": "localhost",
        "database": "/tmp/test.fdb",
        "user": "SYSDBA",
        "password": "pass",
    }
    result = await validate_firebird_config(valid)
    assert result["valid"]
    assert result["errors"] == []
    assert result["warnings"] == []

    missing = {
        "host": "localhost",
        "database": "/tmp/test.fdb",
        "user": "SYSDBA",
    }
    result = await validate_firebird_config(missing)
    assert not result["valid"]
    assert "password" in result["errors"][0]

    warn_conf = {
        "host": "localhost",
        "database": "/tmp/db.txt",
        "user": "admin",
        "password": "pass",
    }
    result = await validate_firebird_config(warn_conf)
    assert result["valid"]
    assert ".fdb" in result["warnings"][0]
    assert "SYSDBA" in result["warnings"][1]
