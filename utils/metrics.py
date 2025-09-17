"""Utilities for Prometheus metrics exposure."""
from __future__ import annotations

from prometheus_client import Counter, Histogram, start_http_server

from utils.logging import get_logger
from utils.messages import msg

LOGGER = get_logger("fesco_tracker.metrics")

API_REQUESTS = Counter(
    "fesco_api_requests_total",
    "Количество запросов к FESCO API",
    labelnames=("status",),
)

API_RETRIES = Counter(
    "fesco_api_request_retries_total",
    "Количество повторных попыток API",
)

CONTAINERS_PROCESSED = Counter(
    "fesco_containers_processed_total",
    "Количество обработанных контейнеров",
)

PROCESSING_ERRORS = Counter(
    "fesco_processing_errors_total",
    "Количество ошибок обработки",
)

PROCESSING_DURATION = Histogram(
    "fesco_order_processing_seconds",
    "Время обработки одной заявки",
)

CACHE_HITS = Counter(
    "fesco_cache_hits_total",
    "Количество попаданий в кэш",
)


def start_metrics_server(host: str = "0.0.0.0", port: int = 8000) -> None:
    """Start Prometheus HTTP exporter on *host*:*port*."""

    start_http_server(port, addr=host)
    LOGGER.info(msg("metrics.server.start", host=host, port=port))
