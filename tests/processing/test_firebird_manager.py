import os
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from utils.db.firebird_manager import FirebirdEntityManager, EntityTableConfig


def _normalize(sql: str) -> str:
    return " ".join(sql.split())


def test_build_contractor_selection_query_sql_and_params():
    config = EntityTableConfig(
        railway_carrier_column="RAILWAY_CARRIER_ID",
        excluded_status_ids=set(),
    )
    manager = FirebirdEntityManager.__new__(FirebirdEntityManager)
    manager.entity_config = config

    query, params = manager._build_contractor_selection_query({100, 200}, {1})

    expected_sql = (
        "SELECT ID, NAME, SP_ENTITY_STATUS_ID, LEGAL_PERSON_LINE_ID, "
        "DATE_ETA, DATE_ETD, DATE_IN, DATE_RAILWAY_LOADING, "
        "DATE_RAILWAY_DELIVERY, TRACING_DAYS, RAILWAY_CARRIER_ID "
        "FROM ENTITY WHERE 1=1 AND RAILWAY_CARRIER_ID IN (?,?) "
        "AND ID NOT IN (?) ORDER BY ID"
    )

    assert _normalize(query) == expected_sql
    assert params == [100, 200, 1]


def test_build_contractor_selection_query_no_exclusions():
    config = EntityTableConfig(
        railway_carrier_column="RAILWAY_CARRIER_ID",
        excluded_status_ids=set(),
    )
    manager = FirebirdEntityManager.__new__(FirebirdEntityManager)
    manager.entity_config = config

    query, params = manager._build_contractor_selection_query({5}, None)

    expected_sql = (
        "SELECT ID, NAME, SP_ENTITY_STATUS_ID, LEGAL_PERSON_LINE_ID, DATE_ETA, DATE_ETD, "
        "DATE_IN, DATE_RAILWAY_LOADING, DATE_RAILWAY_DELIVERY, TRACING_DAYS, RAILWAY_CARRIER_ID "
        "FROM ENTITY WHERE 1=1 AND RAILWAY_CARRIER_ID IN (?) ORDER BY ID"
    )

    assert _normalize(query) == expected_sql
    assert params == [5]
