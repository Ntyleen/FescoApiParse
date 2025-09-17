"""Service layer exposing high level tracker operations."""
from __future__ import annotations

import asyncio
import json
import time
from dataclasses import asdict, is_dataclass
from datetime import datetime
from pathlib import Path
from typing import List

from cache import create_cache
from config import load_config
from config.settings import Config
from models.processing_stats import ProcessingStats
from processing import ContainerTracker, create_tracking_engine
from utils.logging import get_logger, setup_logging_from_config
from utils.messages import msg
from utils.scheduler import FescoScheduler


class FescoTracker:
    """High level orchestrator used by the CLI."""

    def __init__(self, environment: str = "production") -> None:
        self.environment = environment
        self.config: Config | None = None
        self.logger = get_logger("fesco_tracker.main")
        self.stats = ProcessingStats()

    # ------------------------------------------------------------------
    async def initialize(self) -> None:
        print(msg("cli.initialising", environment=self.environment))
        self.config = load_config(environment=self.environment)
        setup_logging_from_config(self.config.logging)
        self.logger = get_logger("fesco_tracker.main")
        self.logger.info(msg("cli.config.loaded", environment=self.environment))
        await self._check_components()

    # ------------------------------------------------------------------
    async def _check_components(self) -> None:
        self.logger.info(msg("cli.component.check"))
        try:
            from utils.db.firebird_manager import FIREBIRD_AVAILABLE

            if FIREBIRD_AVAILABLE:
                self.logger.info(msg("cli.firebird.available"))
            else:
                self.logger.warning(msg("cli.firebird.unavailable"))
        except ImportError:
            self.logger.warning(msg("cli.firebird.unavailable"))

        try:
            from utils.redis_backend import REDIS_AVAILABLE

            if REDIS_AVAILABLE:
                self.logger.info(msg("cli.redis.available"))
            else:
                self.logger.warning(msg("cli.redis.unavailable"))
        except ImportError:
            self.logger.warning(msg("cli.redis.unavailable"))

    # ------------------------------------------------------------------
    async def run_test_mode(self, num_containers: int = 5) -> None:
        assert self.logger is not None
        self.logger.info(msg("cli.mode.test", count=num_containers))
        containers = [
            "TDSU6005411",
            "FESU5384983",
            "TEMU1234567",
            "SKLU1575022",
            "SKLU3511665",
            "CCLU2903390",
            "SKHU8930645",
            "FESU2278740",
        ][:num_containers]
        await self._run_simple_tracking(containers)

    # ------------------------------------------------------------------
    async def run_file_mode(self, file_path: str) -> None:
        assert self.logger is not None
        self.logger.info(msg("cli.mode.file", path=file_path))
        containers = self._load_containers_from_file(file_path)
        await self._run_simple_tracking(containers)

    # ------------------------------------------------------------------
    async def run_db_mode(self, batch_size: int = 100) -> None:
        assert self.logger is not None and self.config is not None
        self.logger.info(msg("cli.mode.db", batch=batch_size))
        engine = await create_tracking_engine(self.config, cache_type="auto")
        stats = await engine.run_full_workflow(
            batch_size=batch_size,
            target_line_ids=set(self.config.database.target_line_ids),
        )
        self._print_engine_stats(stats)

    # ------------------------------------------------------------------
    async def run_monitor_mode(self) -> None:
        assert self.logger is not None
        self.logger.info(msg("cli.monitor"))
        assert self.config is not None

        stats = {
            "timestamp": datetime.now().isoformat(),
            "environment": self.environment,
            "components": {},
        }

        try:
            from utils.db.firebird_manager import create_firebird_entity_manager

            manager = create_firebird_entity_manager(
                host=self.config.database.host,
                database=self.config.database.database,
                user=self.config.database.user,
                password=self.config.database.password,
            )
            if await manager.test_connection():
                db_stats = await manager.get_entity_statistics()
                stats["components"]["database"] = {
                    "status": "connected",
                    "stats": db_stats,
                }
            else:
                stats["components"]["database"] = {"status": "disconnected"}
            await manager.close()
        except Exception as exc:  # pragma: no cover - defensive logging
            stats["components"]["database"] = {
                "status": "error",
                "error": str(exc),
            }

        try:
            if self.config.cache.type == "redis":
                from utils.redis_backend import create_redis_manager

                redis_manager = create_redis_manager(self.config.cache.redis.url)
                if redis_manager:
                    redis_stats = await redis_manager.get_stats()
                    stats["components"]["redis"] = {
                        "status": "connected",
                        "stats": redis_stats,
                    }
                    await redis_manager.close()
                else:
                    stats["components"]["redis"] = {"status": "unavailable"}
        except Exception as exc:  # pragma: no cover - defensive logging
            stats["components"]["redis"] = {
                "status": "error",
                "error": str(exc),
            }

        print("\n" + "=" * 60)
        print("📊 СТАТУС СИСТЕМЫ")
        print("=" * 60)
        print(json.dumps(stats, indent=2, ensure_ascii=False))

    # ------------------------------------------------------------------
    async def schedule_db_mode(self, cron: str, batch_size: int) -> None:
        scheduler = FescoScheduler()
        scheduler.add_job(self.run_db_mode, cron=cron, args=[batch_size])
        scheduler.start()
        await asyncio.Event().wait()

    # ------------------------------------------------------------------
    async def _run_simple_tracking(self, containers: List[str]) -> None:
        assert self.config is not None and self.logger is not None
        cache = create_cache(
            cache_type=self.config.cache.type,
            cache_dir=self.config.cache.dir,
            redis_url=(
                self.config.cache.redis.url
                if self.config.cache.type == "redis"
                else None
            ),
        )
        tracker = ContainerTracker(self.config, cache)
        results = []
        start_time = time.perf_counter()
        async for result in tracker.track_containers(containers):
            results.append(result)
            if len(results) % 10 == 0:
                self.logger.info(
                    f"Обработано: {len(results)}/{len(containers)}"
                )
        duration = time.perf_counter() - start_time
        successful = sum(1 for r in results if r.success)
        failed = len(results) - successful
        output_path = Path(self.config.output.dir) / f"test_results_{int(time.time())}.json"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as handle:
            json.dump(
                [self._result_to_dict(r) for r in results],
                handle,
                ensure_ascii=False,
                indent=2,
            )
        print("\n" + "=" * 60)
        print("📊 РЕЗУЛЬТАТЫ ТРЕКИНГА")
        print("=" * 60)
        print(f"📦 Всего контейнеров:     {len(results)}")
        print(f"✅ Успешно:              {successful}")
        print(f"❌ Ошибки:               {failed}")
        print(f"⏱️ Время выполнения:     {duration:.2f} сек")
        if duration > 0:
            print(f"⚡ Скорость:             {len(results)/duration:.1f} конт/сек")
        print(f"💾 Результаты сохранены: {output_path}")
        print("=" * 60)

    # ------------------------------------------------------------------
    def _load_containers_from_file(self, file_path: str) -> List[str]:
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Файл не найден: {file_path}")

        if path.suffix.lower() == ".json":
            with open(path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
            if isinstance(data, list):
                return data
            if isinstance(data, dict) and "containers" in data:
                return list(data["containers"])
            raise ValueError("Неверный формат JSON файла")

        if path.suffix.lower() in {".txt", ".csv"}:
            containers: List[str] = []
            with open(path, "r", encoding="utf-8") as handle:
                for line in handle:
                    container = line.strip()
                    if container and not container.startswith("#"):
                        containers.append(container)
            return containers

        raise ValueError(f"Неподдерживаемый формат файла: {path.suffix}")

    # ------------------------------------------------------------------
    def _result_to_dict(self, result: object) -> dict:
        if is_dataclass(result):
            return asdict(result)
        return {
            "container_number": getattr(result, "container_number", None),
            "success": getattr(result, "success", False),
            "error_message": getattr(result, "error_message", None),
            "last_event": getattr(result, "last_event", None),
        }

    # ------------------------------------------------------------------
    def _print_engine_stats(self, stats: object) -> None:
        print("\n" + "=" * 60)
        print("📊 СТАТИСТИКА ОБРАБОТКИ")
        print("=" * 60)
        for key, value in vars(stats).items():
            print(f"{key}: {value}")
        print("=" * 60)


__all__ = ["FescoTracker"]
