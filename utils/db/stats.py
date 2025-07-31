from typing import Any, Dict, List, Tuple

from utils.logging import get_logger


class FirebirdStatisticsCollector:
    """Сбор статистики операций с Firebird."""

    def __init__(self) -> None:
        self.stats = {
            'containers_loaded': 0,
            'containers_filtered': 0,
            'batches_processed': 0,
            'records_updated': 0,
            'records_failed': 0,
            'date_columns_updated': {},
            'operations_processed': {},
            'connections_created': 0,
            'transactions_committed': 0,
            'transactions_rollbacked': 0,
            'operation_times': [],
        }
        self.logger = get_logger("firebird.stats")

    def record_container_loaded(self, count: int = 1) -> None:
        self.stats['containers_loaded'] += count

    def record_container_filtered(self, count: int = 1) -> None:
        self.stats['containers_filtered'] += count

    def record_batch_processed(self) -> None:
        self.stats['batches_processed'] += 1

    def record_update_success(self, column_name: str, operation: str) -> None:
        self.stats['records_updated'] += 1
        if column_name not in self.stats['date_columns_updated']:
            self.stats['date_columns_updated'][column_name] = 0
        self.stats['date_columns_updated'][column_name] += 1
        operation_key = operation[:50] if operation else "EMPTY_OPERATION"
        if operation_key not in self.stats['operations_processed']:
            self.stats['operations_processed'][operation_key] = 0
        self.stats['operations_processed'][operation_key] += 1

    def record_update_failure(self) -> None:
        self.stats['records_failed'] += 1

    def record_operation_time(self, time_ms: float) -> None:
        self.stats['operation_times'].append(time_ms)
        if len(self.stats['operation_times']) > 1000:
            self.stats['operation_times'] = self.stats['operation_times'][-1000:]

    def get_summary(self) -> Dict[str, Any]:
        operation_times = self.stats['operation_times']
        summary = {
            'totals': {
                'containers_loaded': self.stats['containers_loaded'],
                'containers_filtered': self.stats['containers_filtered'],
                'batches_processed': self.stats['batches_processed'],
                'records_updated': self.stats['records_updated'],
                'records_failed': self.stats['records_failed'],
            },
            'success_rate': self._calculate_success_rate(),
            'performance': {
                'avg_operation_time_ms': sum(operation_times) / len(operation_times) if operation_times else 0,
                'min_operation_time_ms': min(operation_times) if operation_times else 0,
                'max_operation_time_ms': max(operation_times) if operation_times else 0,
            },
            'top_columns': self._get_top_columns(),
            'top_operations': self._get_top_operations(),
        }
        return summary

    def _calculate_success_rate(self) -> float:
        total = self.stats['records_updated'] + self.stats['records_failed']
        return (self.stats['records_updated'] / total * 100) if total > 0 else 0.0

    def _get_top_columns(self, limit: int = 5) -> List[Tuple[str, int]]:
        items = list(self.stats['date_columns_updated'].items())
        items.sort(key=lambda x: x[1], reverse=True)
        return items[:limit]

    def _get_top_operations(self, limit: int = 5) -> List[Tuple[str, int]]:
        items = list(self.stats['operations_processed'].items())
        items.sort(key=lambda x: x[1], reverse=True)
        return items[:limit]
