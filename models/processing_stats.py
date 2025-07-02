from dataclasses import dataclass


@dataclass
class ProcessingStats:
    """Статистика обработки"""
    total_containers: int = 0
    successful_tracks: int = 0
    failed_tracks: int = 0
    cached_requests: int = 0
    orders_resolved: int = 0
    deduplicated_events: int = 0
    
    start_time: float = 0
    end_time: float = 0
    
    @property
    def duration(self) -> float:
        return self.end_time - self.start_time if self.end_time > 0 else 0
    
    def __str__(self) -> str:
        success_rate = (self.successful_tracks / self.total_containers * 100) if self.total_containers > 0 else 0
        return (f"📊 Обработано: {self.successful_tracks}/{self.total_containers} ({success_rate:.1f}%), "
                f"кэш: {self.cached_requests}, дедупликация: {self.deduplicated_events}, "
                f"время: {self.duration:.2f}s")