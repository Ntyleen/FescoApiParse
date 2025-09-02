"""
Google Sheets sync module for updating distance to destination station and current
railway station based on Firebird DB and cached API data.

Key behaviors:
- Match rows by the sheet column "Контейнер" (container number).
- Write "Расстояние до станции назначения" from DB column TRACING_DAYS.
- Update "Дата обновления слежения" only if the distance value changed.
- Take `location` from cached API data, map it to SP_RAILWAY_STATION, and:
  - Write the station ID to ENTITY.SP_RAILWAY_CURRENT_STATION_ID in Firebird.
  - Write the station NAME to the Google Sheet column "Станция местоположения".

This module does not modify existing Google Sheets or Firebird helper modules.
It composes them and can operate with an injected worksheet for testing.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple
from datetime import datetime
import logging

from zoneinfo import ZoneInfo

from config.settings import Config, load_config
from utils.container_utils import normalize_container_number
from utils.logging import get_logger
from utils.db.firebird_manager import FirebirdConnectionManager
from cache import create_cache
from processing.container_bindings import ContainerBindingManager


# Default sheet header names (keep as plain Unicode literals)
DEFAULT_COL_CONTAINER = "Контейнер"
DEFAULT_COL_DISTANCE = "Расстояние до станции назначения"
DEFAULT_COL_TRACKING_DATE = "Дата обновления слежения"
# Sheet column with station NAME (DB keeps ID separately in SP_RAILWAY_CURRENT_STATION_ID)
DEFAULT_COL_STATION_DISPLAY = "Станция местоположения"


def _now_vvo() -> datetime:
    return datetime.now(ZoneInfo("Asia/Vladivostok"))


@dataclass
class SheetColumns:
    container: str = DEFAULT_COL_CONTAINER
    distance: str = DEFAULT_COL_DISTANCE
    tracking_date: str = DEFAULT_COL_TRACKING_DATE
    station_display: str = DEFAULT_COL_STATION_DISPLAY


class GoogleSheetsSyncer:
    """Synchronizes Google Sheet with Firebird DB and cached API data.

    Usage example:
        config = load_config()
        syncer = GoogleSheetsSyncer(config)
        syncer.sync(sheet_id="...", worksheet_title=None)
    """

    def __init__(
        self,
        config: Config,
        *,
        sheet_columns: SheetColumns = SheetColumns(),
        logger: Optional[logging.Logger] = None,
    ):
        self.config = config
        self.sheet_cols = sheet_columns
        self.logger = logger or get_logger("sheets.sync")

        # Compose existing building blocks
        cache_backend = create_cache(
            cache_type=(config.cache.type or "file").lower().replace("redis", "redis"),
            cache_dir=config.cache.dir,
            redis_url=config.cache.redis.url,
            prefix=(config.cache.cache_prefix or "cache_fesco:"),
            ttl_hours=int(config.cache.ttl_hours),
        )
        self.binding = ContainerBindingManager(cache_backend)
        self.fb = FirebirdConnectionManager(config.database.to_firebird_config())

    # -------------------------------
    # Public API
    # -------------------------------
    def sync(
        self,
        *,
        sheet_id: str,
        worksheet_title: Optional[str] = None,
        sheet: Any = None,
        now_func=_now_vvo,
        batch_size: int = 100,
    ) -> None:
        """Run synchronization for the specified Google Sheet.

        Args:
            sheet_id: Google Sheet ID.
            worksheet_title: Optional worksheet title. If None, uses the first worksheet.
            sheet: Optional gspread worksheet-like object for dependency injection/testing.
            now_func: Clock function to generate timestamps.
            batch_size: Sheet read chunk size for scalability.
        """
        ws = sheet or self._open_gsheet(sheet_id, worksheet_title)
        if ws is None:
            self.logger.error("Cannot open Google Sheet; aborting sync.")
            return

        # Resolve header -> column index
        header = ws.row_values(1)
        header_map = {name: idx for idx, name in enumerate(header, start=1)}
        required = [
            self.sheet_cols.container,
            self.sheet_cols.distance,
            self.sheet_cols.tracking_date,
            self.sheet_cols.station_display,
        ]
        for name in required:
            if name not in header_map:
                raise ValueError(f"Required sheet column not found: {name}")

        col_container = header_map[self.sheet_cols.container]
        col_distance = header_map[self.sheet_cols.distance]
        col_trackdate = header_map[self.sheet_cols.tracking_date]
        col_station = header_map[self.sheet_cols.station_display]

        # Map container -> row index
        container_values = ws.col_values(col_container)[1:]  # skip header
        container_rows: Dict[str, int] = {}
        for i, raw in enumerate(container_values, start=2):
            norm = normalize_container_number(raw)
            if norm:
                container_rows[norm] = i

        if not container_rows:
            self.logger.info("No containers found in the Sheet")
            return

        # Load entity details and station index from Firebird
        containers = list(container_rows.keys())
        entity_info = self._fetch_entity_info(containers)
        station_index = self._fetch_station_index()

        # Build updates per row
        for norm_cn, row in container_rows.items():
            info = entity_info.get(norm_cn)
            if not info:
                self.logger.debug(f"Skip; container not in DB: {norm_cn}")
                continue

            entity_id, tracing_days_db, current_station_id_db = info

            # Current values in sheet
            sheet_distance = ws.cell(row, col_distance).value or ""
            sheet_station_name = ws.cell(row, col_station).value or ""

            # Compute new distance string
            new_distance = "" if tracing_days_db in (None, "") else str(tracing_days_db)

            # Resolve location from cache -> station (ID, NAME)
            location = self._get_location_from_cache(norm_cn)
            matched_station_id: Optional[int] = None
            matched_station_name: str = ""
            if location:
                matched_station_id, matched_station_name = self._map_location_to_station(location, station_index)

            # Update ENTITY.SP_RAILWAY_CURRENT_STATION_ID when needed
            if matched_station_id is not None and matched_station_id != current_station_id_db:
                try:
                    self._update_entity_station(entity_id, matched_station_id)
                    current_station_id_db = matched_station_id
                except Exception as e:
                    self.logger.error(f"Failed DB update for {norm_cn} -> station_id={matched_station_id}: {e}")

            # For the sheet, we write the station NAME (not ID)
            new_station_name = matched_station_name if matched_station_name else sheet_station_name

            # Determine if distance changed; tracking date updates only on distance change
            distance_changed = (sheet_distance or "") != (new_distance or "")
            station_changed = (sheet_station_name or "") != (new_station_name or "")

            # Apply sheet updates (minimal writes)
            if distance_changed:
                ws.update_cell(row, col_distance, new_distance)
                ws.update_cell(row, col_trackdate, now_func().strftime("%Y-%m-%d %H:%M:%S"))
            if station_changed:
                ws.update_cell(row, col_station, new_station_name)

            if distance_changed or station_changed:
                self.logger.info(
                    f"Row {row} {norm_cn}: updated"
                    f"{' distance' if distance_changed else ''}"
                    f"{' station' if station_changed else ''}"
                )

    # -------------------------------
    # Helpers: Google Sheets
    # -------------------------------
    def _open_gsheet(self, sheet_id: str, worksheet_title: Optional[str]) -> Any:
        try:
            import gspread  # Optional dependency
        except Exception as exc:
            self.logger.error(f"gspread is required to open Google Sheets: {exc}")
            return None

        try:
            client = gspread.service_account()
            sh = client.open_by_key(sheet_id)
            if worksheet_title:
                return sh.worksheet(worksheet_title)
            return sh.sheet1
        except Exception as exc:
            self.logger.error(f"Failed to open Google Sheet: {exc}")
            return None

    # -------------------------------
    # Helpers: Firebird
    # -------------------------------
    def _fetch_entity_info(self, container_numbers: List[str]) -> Dict[str, Tuple[int, Optional[int], Optional[int]]]:
        """Fetch entity ID, TRACING_DAYS and current station ID by container numbers.

        Returns a mapping: normalized_container -> (entity_id, tracing_days, station_id)
        """
        if not container_numbers:
            return {}

        placeholders = ", ".join(["?"] * len(container_numbers))
        query = f"""
            SELECT
                {self._q(self.config.database.primary_key)} AS id,
                {self._q(self.config.database.container_column)} AS name,
                {self._q(self.config.database.remaining_distance)} AS tracing_days,
                {self._q(self.config.database.railway_current_station)} AS station_id
            FROM {self._q(self.config.database.table_name)}
            WHERE UPPER(REPLACE({self._q(self.config.database.container_column)}, ' ', '')) IN ({placeholders})
        """

        normed = [normalize_container_number(c) for c in container_numbers]
        name_map: Dict[str, Tuple[int, Optional[int], Optional[int]]] = {}

        with self.fb.get_connection() as conn:
            cur = conn.cursor()
            cur.execute(query, normed)
            for row in cur.fetchall():
                entity_id = int(row[0])
                name = normalize_container_number(row[1])
                tracing_days = row[2]
                station_id = row[3]
                name_map[name] = (entity_id, tracing_days, station_id)
        return name_map

    def _fetch_station_index(self) -> Dict[str, Tuple[int, str]]:
        """Load station name -> (ID, NAME) index from SP_RAILWAY_STATION."""
        index: Dict[str, Tuple[int, str]] = {}
        with self.fb.get_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT ID, NAME FROM SP_RAILWAY_STATION")
            for row in cur.fetchall():
                sid = int(row[0])
                name = str(row[1] or "").strip()
                if not name:
                    continue
                index[self._norm_station(name)] = (sid, name)
        return index

    def _update_entity_station(self, entity_id: int, station_id: int) -> None:
        with self.fb.get_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                f"UPDATE {self._q(self.config.database.table_name)} "
                f"SET {self._q(self.config.database.railway_current_station)} = ? "
                f"WHERE {self._q(self.config.database.primary_key)} = ?",
                [station_id, entity_id],
            )
            # Commit transaction depending on driver support
            if hasattr(conn, "commit"):
                conn.commit()

    # -------------------------------
    # Helpers: Cache -> location -> station mapping
    # -------------------------------
    def _get_location_from_cache(self, norm_container: str) -> Optional[str]:
        """Resolve last known 'location' from cache via container->order binding.

        Tries order-level cache first (order_track:{order_id}), then
        container-level cache (container_track:{order_id}:{container}).
        """
        try:
            # 1) Find order by container (from bindings cache)
            order_id = self._await_sync(self.binding.get_container_order, norm_container)
            if not order_id:
                return None

            # 2) Try order-level cache
            cache = self.binding.cache  # type: ignore[attr-defined]
            order_key = f"order_track:{order_id}"
            order_data = self._await_sync(cache.get, order_key) or {}
            loc = self._extract_location_from_order_cache(order_data, norm_container)
            if loc:
                return loc

            # 3) Fallback to container-level cache
            container_key = f"container_track:{order_id}:{norm_container}"
            container_data = self._await_sync(cache.get, container_key) or {}
            loc = self._extract_location_from_container_cache(container_data)
            return loc

        except Exception as e:
            self.logger.debug(f"Cache lookup failed for {norm_container}: {e}")
            return None

    @staticmethod
    def _extract_location_from_order_cache(order_data: Mapping[str, Any], norm_container: str) -> Optional[str]:
        try:
            for order_item in order_data.get("data", []):
                for container in order_item.get("containers", []):
                    cn = normalize_container_number(container.get("containerNumber", ""))
                    if cn == norm_container:
                        last_event = container.get("lastEvent", {})
                        loc = last_event.get("location")
                        return None if loc in (None, "") else str(loc).strip()
        except Exception:
            pass
        return None

    @staticmethod
    def _extract_location_from_container_cache(container_data: Mapping[str, Any]) -> Optional[str]:
        try:
            items = container_data.get("data", [])
            if not items:
                return None
            # Heuristic: newest is first; prefer lastEvent-like structure
            item0 = items[0]
            loc = item0.get("location") or item0.get("lastEvent", {}).get("location")
            return None if loc in (None, "") else str(loc).strip()
        except Exception:
            return None

    # -------------------------------
    # Utilities
    # -------------------------------
    @staticmethod
    def _norm_station(name: str) -> str:
        return " ".join(name.upper().split())

    def _map_location_to_station(
        self,
        location: str,
        station_index: Mapping[str, Tuple[int, str]],
    ) -> Tuple[Optional[int], str]:
        """Map free-form location to station (ID, NAME) via SP_RAILWAY_STATION.

        Strategy:
        - Exact normalized match.
        - Prefix match (station startswith location) or vice versa.
        - Contains match as a last resort.
        """
        if not location:
            return None, ""

        norm = self._norm_station(location)
        # 1) Exact
        if norm in station_index:
            sid, name = station_index[norm]
            return sid, name

        # 2) Prefix/Contains heuristics
        for key, (sid, name) in station_index.items():
            if key.startswith(norm) or norm.startswith(key):
                return sid, name
        for key, (sid, name) in station_index.items():
            if norm in key or key in norm:
                return sid, name

        return None, ""

    @staticmethod
    def _await_sync(func, *args, **kwargs):
        """Run an async function synchronously; fallback if already a value."""
        try:
            import asyncio
            result = func(*args, **kwargs)
            if asyncio.iscoroutine(result):
                return asyncio.get_event_loop().run_until_complete(result)
            return result
        except RuntimeError:
            # No running loop
            import asyncio
            return asyncio.run(func(*args, **kwargs))

    @staticmethod
    def _q(identifier: str) -> str:
        # Quote Firebird identifiers in a conservative way
        # Avoid quoting when not necessary to keep indexes usable
        ident = (identifier or "").strip()
        if not ident:
            return ident
        # Basic safety: allow alnum and underscore; else wrap in quotes
        if all(ch.isalnum() or ch == "_" for ch in ident):
            return ident
        return f'"{ident}"'


# Convenience top-level function
def sync_sheet_with_db_and_cache(
    sheet_id: str,
    worksheet_title: Optional[str] = None,
    *,
    config: Optional[Config] = None,
    sheet: Any = None,
    columns: SheetColumns = SheetColumns(),
) -> None:
    cfg = config or load_config()
    syncer = GoogleSheetsSyncer(cfg, sheet_columns=columns)
    syncer.sync(sheet_id=sheet_id, worksheet_title=worksheet_title, sheet=sheet)
