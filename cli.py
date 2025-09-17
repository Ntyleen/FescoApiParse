"""Command line interface for the FESCO tracker."""
from __future__ import annotations

import argparse
import asyncio
import sys
import traceback
from typing import Sequence

from services.tracker import FescoTracker
from utils.metrics import start_metrics_server


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="FESCO Container Tracking System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Примеры использования:

  # Тестовый запуск с 5 контейнерами
  python main.py test

  # Обработка контейнеров из файла
  python main.py file containers.txt

  # Полная обработка из БД
  python main.py db --batch-size 100

  # Мониторинг системы
  python main.py monitor
        """,
    )
    parser.add_argument(
        "--env",
        "--environment",
        choices=["development", "production", "test"],
        default="production",
        help="Окружение для запуска",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Подробный вывод",
    )
    parser.add_argument(
        "--metrics-port",
        type=int,
        default=None,
        help="Запустить Prometheus metrics endpoint на указанном порту",
    )
    subparsers = parser.add_subparsers(dest="mode", help="Режим работы")

    test_parser = subparsers.add_parser("test", help="Тестовый режим")
    test_parser.add_argument(
        "--count",
        "-n",
        type=int,
        default=5,
        help="Количество тестовых контейнеров",
    )

    file_parser = subparsers.add_parser("file", help="Обработка из файла")
    file_parser.add_argument("file_path", help="Путь к файлу с контейнерами")

    db_parser = subparsers.add_parser("db", help="Обработка из БД")
    db_parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Размер батча для обработки",
    )

    subparsers.add_parser("monitor", help="Мониторинг системы")

    sched_parser = subparsers.add_parser("schedule", help="Запуск по расписанию")
    sched_parser.add_argument("--cron", help="CRON выражение", default=None)
    sched_parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help="Размер батча для обработки",
    )
    return parser


async def run_cli(argv: Sequence[str] | None = None) -> int:
    parser = create_parser()
    args = parser.parse_args(argv)

    if not args.mode:
        parser.print_help()
        return 1

    tracker = FescoTracker(environment=args.env)
    try:
        await tracker.initialize()

        if args.metrics_port:
            start_metrics_server(port=args.metrics_port)

        if args.mode == "test":
            await tracker.run_test_mode(args.count)
        elif args.mode == "file":
            await tracker.run_file_mode(args.file_path)
        elif args.mode == "db":
            await tracker.run_db_mode(args.batch_size)
        elif args.mode == "monitor":
            await tracker.run_monitor_mode()
        elif args.mode == "schedule":
            cron = args.cron or tracker.config.scheduler.cron  # type: ignore[union-attr]
            batch_size = args.batch_size or tracker.config.processing.batch_size  # type: ignore[union-attr]
            await tracker.schedule_db_mode(cron, batch_size)
        else:  # pragma: no cover - defensive fallback
            parser.print_help()
            return 1

    except KeyboardInterrupt:
        print("\n⚠️ Прервано пользователем")
        return 130
    except Exception as exc:  # pragma: no cover - CLI safety net
        print(f"\n❌ Ошибка: {exc}")
        if getattr(args, "verbose", False):
            traceback.print_exc()
        return 1
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())  # type: ignore[attr-defined]
    return asyncio.run(run_cli(argv))


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
