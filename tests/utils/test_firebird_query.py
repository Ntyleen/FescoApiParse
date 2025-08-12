import os
import sys
from unittest.mock import MagicMock

# Ensure project package importable
dIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
PARENT_DIR = os.path.dirname(dIR)
if PARENT_DIR not in sys.path:
    sys.path.insert(0, PARENT_DIR)

import importlib

fm = importlib.import_module('FescoApiParse.utils.db.firebird_manager')
EntityTableConfig = fm.EntityTableConfig
FirebirdEntityManager = fm.FirebirdEntityManager


def test_build_query_line_or_carrier():
    mgr = object.__new__(FirebirdEntityManager)
    mgr.entity_config = EntityTableConfig()
    mgr.logger = MagicMock()
    query, params = FirebirdEntityManager._build_selection_query(
        mgr, {1}, {2}, 0
    )
    assert 'LEGAL_PERSON_LINE_ID' in query
    assert 'LEGAL_PERSON_RAILWAY_CARRIER_ID' in query
    assert ' OR ' in query
    assert 1 in params and 2 in params
