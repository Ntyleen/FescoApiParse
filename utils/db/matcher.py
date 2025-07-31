from typing import Dict, List, Optional, Tuple

from utils.logging import get_logger
from .models import EntityTableConfig, EntityColumnMapping


class FirebirdOperationMatcher:
    """Логика сопоставления операций FESCO с колонками entity."""

    def __init__(self, entity_config: EntityTableConfig):
        self.entity_config = entity_config
        self.logger = get_logger("firebird.matcher")

    def find_best_mapping(self, operation: str) -> Optional[EntityColumnMapping]:
        if not operation or not operation.strip():
            return None
        operation_clean = operation.strip()
        self.logger.debug(f"🎯 Анализируем операцию: '{operation_clean}'")
        scored_mappings = []
        for column_name, mapping in self.entity_config.date_mappings.items():
            score = mapping.matches_operation(operation_clean)
            if score > 0:
                scored_mappings.append((mapping, score))
                self.logger.debug(f"  📊 {column_name}: score={score:.3f}")
        if not scored_mappings:
            self.logger.debug("  🤷 Подходящих маппингов не найдено")
            return None
        scored_mappings.sort(key=lambda x: x[1], reverse=True)
        best_mapping, best_score = scored_mappings[0]
        if best_score < 0.3:
            self.logger.debug(
                f"🚫 Лучшая оценка {best_score:.3f} ниже порога 0.3"
            )
            return None
        self.logger.debug(
            f"🎯 Выбран маппинг: {best_mapping.entity_column} (score: {best_score:.3f})"
        )
        return best_mapping

    def suggest_new_mappings(self, unmapped_operations: List[str]) -> Dict[str, List[str]]:
        suggestions: Dict[str, List[str]] = {}
        for operation in unmapped_operations:
            for column_name, mapping in self.entity_config.date_mappings.items():
                similarity = self._calculate_similarity(operation, mapping.operation_patterns)
                if 0.1 < similarity < 0.3:
                    if column_name not in suggestions:
                        suggestions[column_name] = []
                    suggestions[column_name].append(
                        f"Рассмотреть добавление паттерна для '{operation}' "
                        f"(схожесть: {similarity:.2f})"
                    )
        return suggestions

    def _calculate_similarity(self, operation: str, patterns: Tuple[str, ...]) -> float:
        if not patterns:
            return 0.0
        operation_words = set(operation.lower().split())
        max_similarity = 0.0
        for pattern in patterns:
            pattern_words = set(pattern.lower().split())
            if operation_words and pattern_words:
                intersection = operation_words.intersection(pattern_words)
                union = operation_words.union(pattern_words)
                if union:
                    similarity = len(intersection) / len(union)
                    max_similarity = max(max_similarity, similarity)
        return max_similarity
