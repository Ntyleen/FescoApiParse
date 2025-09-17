"""Centralised user-facing and logging messages.

All user-visible strings are concentrated in a single location so they can be
translated or updated without touching the business logic modules.  The module
provides a :func:`msg` helper returning formatted strings for the supplied
message *key*.
"""
from __future__ import annotations

from typing import Any, Dict

MESSAGES: Dict[str, str] = {
    "api.init": "🌐 FescoApiClient инициализирован",
    "api.base_url": "🔧 Base URL: {base_url}",
    "api.timeout": "⏱️ Timeout: {timeout}s",
    "api.max_parallel": "🔗 Max parallel: {max_parallel}",
    "api.request.attempt": "🌐 Попытка #{attempt} {method} {endpoint}",
    "api.request.success": "✅ Ответ {status} от {endpoint}",
    "api.request.retry": "🔁 Повтор запроса через {delay:.1f}с из-за: {reason}",
    "api.request.error": "❌ Ошибка запроса {endpoint}: {error}",
    "api.cache.hit": "💾 Cache HIT: {key}",
    "api.cache.miss": "📦 Cache MISS: {key}",
    "api.cache.unchanged": "💾 Cache unchanged, skip update: {key}",
    "api.cache.negative_hit": "💾 NO_ORDER HIT: {container}",
    "api.cache.negative_store": "💾 NO_ORDER STORE: {container}",
    "engine.batch.start": "📦 Обработка батча: {count} контейнеров",
    "engine.order.changed": "🔄 Данные заявки {order_id} изменились, обрабатываем детально",
    "engine.order.skipped": "💾 Данные заявки {order_id} не изменились",
    "engine.container.error": "❌ Ошибка обработки {container}: {error}",
    "engine.container.unexpected_type": "❌ Неожиданный тип результата для {container}: {type}",
    "engine.google.sync.start": "📝 Google Sheets синхронизация активирована",
    "engine.google.sync.skip": "📝 Google Sheets синхронизация отключена",
    "cli.initialising": "🚀 Инициализация FESCO Tracker ({environment})...",
    "cli.config.loaded": "✅ Конфигурация загружена для {environment}",
    "cli.component.check": "🔍 Проверка компонентов...",
    "cli.firebird.available": "✅ Firebird драйвер доступен",
    "cli.firebird.unavailable": "⚠️ Firebird драйвер недоступен",
    "cli.redis.available": "✅ Redis клиент доступен",
    "cli.redis.unavailable": "⚠️ Redis клиент недоступен",
    "cli.mode.test": "🧪 Запуск в тестовом режиме ({count} контейнеров)",
    "cli.mode.file": "📄 Загрузка контейнеров из файла: {path}",
    "cli.mode.db": "🗄️ Запуск обработки из БД (batch_size={batch})",
    "cli.monitor": "📊 Режим мониторинга",
    "metrics.server.start": "📈 Экспорт метрик запущен на {host}:{port}",
}


def msg(message_key: str, **params: Any) -> str:
    """Return formatted message for *key*.

    When an unknown key is requested the function simply returns the key itself
    so that missing entries do not break the application.  Parameters are
    formatted using ``str.format``.
    """

    template = MESSAGES.get(message_key, message_key)
    return template.format(**params)
