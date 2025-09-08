"""Utilities for mapping API location to railway station IDs and updating the ENTITY table."""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, List, Any, Optional

from utils.logging import get_logger
from utils.db.firebird_manager import FirebirdEntityManager


@dataclass
class CacheRecord:
    entity_id: int
    location: str
    sp_station: Optional[str] = None


class StationUpdater:
    """Resolve station IDs from API location strings and store them in Firebird."""

    def __init__(self, firebird: FirebirdEntityManager):
        self.firebird = firebird
        self.logger = get_logger("station_updater")

    # ------------------------------------------------------------------
    # Normalisation helpers
    # ------------------------------------------------------------------
    @staticmethod
    def normalize(name: str) -> str:
        if not name:
            return ""
        name = name.strip().upper()
        # remove common prefixes like "СТ.", "СТАНЦИЯ", "Г." etc
        name = re.sub(r"\bСТ\.?\b", " ", name)
        name = name.replace("СТАНЦИЯ", "")
        name = name.replace("ГОРОД", "")
        name = name.replace("Г.", "")
        name = re.sub(r"[\.,]", " ", name)
        name = re.sub(r"\s+", " ", name)
        return name.strip()

    # ------------------------------------------------------------------
    async def resolve_station_id(self, location: str, sp_station: Optional[str] = None) -> Optional[int]:
        """Determine station ID using explicit field or by name lookup."""
        if sp_station:
            sp_station = sp_station.strip()
            if sp_station.isdigit():
                return int(sp_station)

        normalized = self.normalize(location)
        if not normalized:
            return None

        matches = await self.firebird.find_station_by_name(normalized)
        if len(matches) == 1:
            return int(matches[0][0])
        if len(matches) == 0:
            self.logger.warning(f"Не найдено станции для '{location}'")
        else:
            self.logger.warning(
                f"Найдено несколько станций для '{location}': {[m[0] for m in matches]}"
            )
        return None

    async def update_entity(self, entity_id: int, location: str, sp_station: Optional[str] = None) -> bool:
        station_id = await self.resolve_station_id(location, sp_station)
        if station_id is None:
            return False

        current_id = await self.firebird.get_entity_station_id(entity_id)
        if current_id == station_id:
            return False

        success = await self.firebird.update_entity_station_id(entity_id, station_id)
        if success:
            self.logger.info(
                f"ENTITY {entity_id}: station {current_id} -> {station_id}"
            )
        return success

    async def process_records(self, records: List[Dict[str, Any]]) -> Dict[str, int]:
        """Process a batch of cache records."""
        stats = {"updated": 0, "skipped": 0, "conflict": 0}
        for rec in records:
            entity_id = rec.get("entity_id")
            location = rec.get("location", "")
            sp_station = rec.get("sp_station")
            updated = await self.update_entity(entity_id, location, sp_station)
            if updated:
                stats["updated"] += 1
            else:
                stats["skipped"] += 1
        return stats
