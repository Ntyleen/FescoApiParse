import re
from datetime import datetime, date
from typing import Any, Optional

from utils.logging import get_logger


class FirebirdDateTransformer:
    """Преобразование дат и чисел в совместимые с Firebird типы."""

    def __init__(self) -> None:
        self.logger = get_logger("firebird.transformer")
        self._number_pattern = re.compile(r"\d+")
        self._timestamp_formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
            "%d.%m.%Y %H:%M:%S",
            "%d.%m.%Y %H:%M",
        ]
        self._date_formats = [
            "%Y-%m-%d",
            "%d.%m.%Y",
            "%Y/%m/%d",
            "%d/%m/%Y",
        ]

    def transform_value(self, value: Any, target_type: str) -> Optional[Any]:
        if value is None:
            return None
        try:
            if target_type == "TIMESTAMP":
                return self._transform_to_timestamp(value)
            if target_type == "DATE":
                return self._transform_to_date(value)
            if target_type == "INTEGER":
                return self._transform_to_integer(value)
            self.logger.warning(f"Неизвестный тип: {target_type}")
            return str(value)
        except Exception as e:  # pragma: no cover - log only
            self.logger.warning(
                f"Ошибка трансформации '{value}' в {target_type}: {e}"
            )
            return None

    def _transform_to_timestamp(self, date_str: str) -> Optional[datetime]:
        if not date_str or not str(date_str).strip():
            return None
        cleaned = str(date_str).strip()
        for fmt in self._timestamp_formats:
            try:
                return datetime.strptime(cleaned, fmt)
            except ValueError:
                continue
        for fmt in self._date_formats:
            try:
                date_only = datetime.strptime(cleaned, fmt)
                return date_only
            except ValueError:
                continue
        return None

    def _transform_to_date(self, date_str: str) -> Optional[date]:
        timestamp = self._transform_to_timestamp(date_str)
        return timestamp.date() if timestamp else None

    def _transform_to_integer(self, value_str: str) -> Optional[int]:
        if not value_str:
            return None
        numbers = self._number_pattern.findall(str(value_str))
        if numbers:
            try:
                return int(numbers[0])
            except ValueError:
                pass
        return None
