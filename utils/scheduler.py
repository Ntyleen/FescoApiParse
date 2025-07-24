from __future__ import annotations

from typing import Callable, Any, Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger


class FescoScheduler:
    """Wrapper around :class:`AsyncIOScheduler` for FESCO tracker."""

    def __init__(self) -> None:
        self._scheduler = AsyncIOScheduler()

    def add_job(
        self,
        func: Callable[..., Any],
        *,
        cron: Optional[str] = None,
        interval_seconds: Optional[int] = None,
        args: Optional[list[Any]] = None,
        kwargs: Optional[dict[str, Any]] = None,
    ) -> None:
        """Add a recurring job using a cron expression or interval."""
        if cron:
            trigger = CronTrigger.from_crontab(cron)
        elif interval_seconds is not None:
            trigger = IntervalTrigger(seconds=interval_seconds)
        else:
            raise ValueError("Either cron or interval_seconds must be provided")

        self._scheduler.add_job(func, trigger, args=args or [], kwargs=kwargs or {})

    def start(self) -> None:
        """Start the underlying scheduler."""
        self._scheduler.start()

    def shutdown(self) -> None:
        """Shutdown the scheduler."""
        self._scheduler.shutdown()
