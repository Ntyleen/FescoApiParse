"""Utilities for mapping location names to railway station IDs and updating entity records.

This module implements the logic required by task 1:
    * normalisation of location names
    * mapping to station identifiers from ``SP_RAILWAY_STATION``
    * updating ``ENTITY.SP_RAILWAY_CURRENT_STATION_ID`` only when the value changes
    * logging all performed updates

The implementation relies only on the existing Firebird utilities and logging
modules.  The database schema is intentionally simple – only the fields used in
this task are queried.

The main entry points are :class:`StationMapper` and
:class:`EntityStationUpdater`.
"""
from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Dict, Optional, Iterable

from utils.logging import get_logger

try:
    # Firebird utilities are optional during tests – they are not available on
    # CI.  The code is structured so that the mapper can be used without an
    # actual connection manager (for unit tests).
    from utils.db.firebird_manager import FirebirdConnectionManager
except Exception:  # pragma: no cover - fallback for tests
    FirebirdConnectionManager = None  # type: ignore

logger = get_logger("station_mapper")


_STATION_PREFIXES = [
    r"^СТ\.?\s*",  # «ст.» or станция
    r"^СТАНЦИЯ\s*",
    r"^Г\.?\s*",
]
_SUFFIX_PATTERN = re.compile(r"[\.,]\s*$")


def normalize_station_name(name: str) -> str:
    """Return a normalised representation of *name*.

    The function performs the following transformations:

    * trim leading/trailing whitespace
    * remove common prefixes like ``ст.``, ``станция`` or ``г.``
    * collapse multiple spaces and hyphens
    * convert to upper case

    Parameters
    ----------
    name:
        Raw location name from the API/cache.
    """
    if not name:
        return ""

    result = name.strip().upper()

    for pattern in _STATION_PREFIXES:
        result = re.sub(pattern, "", result)

    result = re.sub(r"[\s\-]+", " ", result)
    result = _SUFFIX_PATTERN.sub("", result)
    return result.strip()


@dataclass
class StationMapper:
    """Map normalised station names to station IDs.

    Parameters
    ----------
    connection_manager:
        Optional :class:`FirebirdConnectionManager`.  When omitted the mapper can
        still be used by manually populating ``stations`` and ``aliases`` – this
        is convenient for unit tests.
    """

    connection_manager: Optional[FirebirdConnectionManager] = None
    stations: Dict[str, int] | None = None
    aliases: Dict[str, int] | None = None

    def _load(self) -> None:
        """Load station names and aliases from the database if not cached."""
        if self.stations is not None and self.aliases is not None:
            return

        self.stations = {}
        self.aliases = {}
        if not self.connection_manager:
            logger.debug("No connection manager supplied; using empty station map")
            return

        with self.connection_manager.get_connection() as connection:
            cursor = connection.cursor()
            cursor.execute("SELECT ID, NAME FROM SP_RAILWAY_STATION")
            for station_id, name in cursor.fetchall():
                self.stations[normalize_station_name(name)] = station_id

            # Aliases are optional – some installations may not have a separate
            # table.  We try to load it but ignore errors.
            try:
                cursor.execute("SELECT STATION_ID, ALIAS FROM SP_RAILWAY_STATION_ALIAS")
                for station_id, alias in cursor.fetchall():
                    self.aliases[normalize_station_name(alias)] = station_id
            except Exception:  # pragma: no cover - optional table
                pass

    def find_station_id(self, location: str) -> Optional[int]:
        """Return station id for *location* or ``None`` if not resolvable."""
        self._load()
        if not location:
            return None

        assert self.stations is not None and self.aliases is not None

        norm = normalize_station_name(location)
        if norm in self.stations:
            return self.stations[norm]
        if norm in self.aliases:
            return self.aliases[norm]

        # Partial match – collect all station IDs whose names start with the
        # normalised string.  Only a single unique match is accepted.
        matches = {sid for name, sid in self.stations.items() if name.startswith(norm)}
        if len(matches) == 1:
            return matches.pop()
        return None


class EntityStationUpdater:
    """Update ``ENTITY.SP_RAILWAY_CURRENT_STATION_ID`` based on cache data."""

    def __init__(self, connection_manager: FirebirdConnectionManager, mapper: StationMapper):
        if connection_manager is None:
            raise ValueError("connection_manager is required")
        self.connection_manager = connection_manager
        self.mapper = mapper
        self.logger = get_logger("station_updater")

    def update_entity(self, entity_id: int, location: str, source: str = "cache") -> bool:
        """Update the station id for *entity_id*.

        Parameters
        ----------
        entity_id:
            Identifier of the ENTITY record.
        location:
            Raw location string from cache.
        source:
            Source description for logging purposes.

        Returns
        -------
        bool
            ``True`` if the value was updated, ``False`` otherwise.
        """
        station_id = self.mapper.find_station_id(location)
        if station_id is None:
            self.logger.info(
                "❔ Unable to resolve station for entity %s: %s", entity_id, location
            )
            return False

        with self.connection_manager.get_connection() as connection:
            cursor = connection.cursor()
            cursor.execute(
                "SELECT SP_RAILWAY_CURRENT_STATION_ID FROM ENTITY WHERE ID = ?",
                (entity_id,),
            )
            row = cursor.fetchone()
            current_id = row[0] if row else None

            if current_id == station_id:
                self.logger.debug(
                    "Entity %s already has station %s", entity_id, station_id
                )
                return False

            cursor.execute(
                """
                UPDATE ENTITY
                SET SP_RAILWAY_CURRENT_STATION_ID = ?,
                    UPDATED_AT = CURRENT_TIMESTAMP
                WHERE ID = ?
                """,
                (station_id, entity_id),
            )
            connection.commit()

        self.logger.info(
            "✅ Entity %s station updated: %s → %s (source: %s)",
            entity_id,
            current_id,
            station_id,
            source,
        )
        return True

    def process_records(self, records: Iterable[Dict[str, str]]) -> Dict[str, int]:
        """Process multiple cache *records*.

        Each record must contain ``entity_id`` and ``location`` keys.  The method
        returns a summary dictionary with counts of updated and skipped records.
        """
        updated = 0
        skipped = 0
        for rec in records:
            entity_id = int(rec["entity_id"])
            location = rec.get("location") or ""
            if self.update_entity(entity_id, location):
                updated += 1
            else:
                skipped += 1
        return {"updated": updated, "skipped": skipped}
