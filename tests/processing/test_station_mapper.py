import pytest
from unittest.mock import MagicMock

from processing.station_mapper import (
    normalize_station_name,
    StationMapper,
    EntityStationUpdater,
)


def test_normalize_station_name():
    assert normalize_station_name(' ст. Москва - Каланчёвская ') == 'МОСКВА КАЛАНЧЁВСКАЯ'
    assert normalize_station_name('г. Владивосток') == 'ВЛАДИВОСТОК'


def test_station_mapper_match_with_alias_and_partial():
    mapper = StationMapper()
    mapper.stations = {'МОСКВА КАЛАНЧЁВСКАЯ': 1, 'МОСКВА ТОВАРНАЯ': 3}
    mapper.aliases = {'МОСКВА КАЛАНЧЕВСКАЯ': 1}

    assert mapper.find_station_id('Москва-Каланчёвская') == 1
    assert mapper.find_station_id('Москва-Кал') == 1
    assert mapper.find_station_id('Москва') is None  # ambiguous


def test_entity_station_updater_updates_only_on_change():
    connection = MagicMock()
    cursor = MagicMock()
    connection.cursor.return_value = cursor
    connection.__enter__.return_value = connection
    cursor.fetchone.return_value = (2,)

    conn_manager = MagicMock()
    conn_manager.get_connection.return_value = connection

    mapper = MagicMock()
    mapper.find_station_id.return_value = 1

    updater = EntityStationUpdater(conn_manager, mapper)
    assert updater.update_entity(5, 'Москва') is True
    cursor.execute.assert_any_call(
        'SELECT SP_RAILWAY_CURRENT_STATION_ID FROM ENTITY WHERE ID = ?', (5,)
    )
    # second execute corresponds to UPDATE query
    update_call = cursor.execute.call_args_list[1]
    assert update_call[0][1] == (1, 5)
    assert 'UPDATE ENTITY' in update_call[0][0]

    cursor.reset_mock()
    cursor.fetchone.return_value = (1,)
    assert updater.update_entity(5, 'Москва') is False
    cursor.execute.assert_called_once_with(
        'SELECT SP_RAILWAY_CURRENT_STATION_ID FROM ENTITY WHERE ID = ?', (5,)
    )
