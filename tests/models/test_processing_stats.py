import sys
import pathlib
import pytest

ROOT_DIR = pathlib.Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from models.processing_stats import ProcessingStats


def test_processing_stats_duration_and_str():
    stats = ProcessingStats(
        total_containers=5,
        successful_tracks=3,
        cached_requests=1,
        deduplicated_events=2,
    )
    stats.start_time = 1.0
    stats.end_time = 3.5

    assert stats.duration == pytest.approx(2.5)
    assert str(stats) == (
        "📊 Обработано: 3/5 (60.0%), кэш: 1, дедупликация: 2, время: 2.50s"
    )

