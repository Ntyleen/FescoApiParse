# main.py
"""
FESCO Container Tracking - Refactored Version
Модульная архитектура с разделением ответственности
"""

import asyncio
import json
import logging
from dataclasses import asdict
from pathlib import Path
from typing import List

from config.settings import Config
from cache import create_cache
from models.container_event import TrackingResult
from processing.tracker import ContainerTracker
from utils.logging import setup_logging

config = Config.from_yaml()

async def track_containers_from_list(
    container_numbers: List[str],
    output_file: str = "./output/tracking_results.json",
    cache_type: str = "file",
) -> None:
    """
    Основная функция для трекинга контейнеров
    
    Args:
        container_numbers: Список номеров контейнеров для трекинга
        output_file: Путь к файлу для сохранения результатов
        cache_type: Тип кэша ("file" или "redis")
        log_level: Уровень логирования
    """

    # Настройка логирования
    setup_logging(config.logging.level, config.logging.file, config.logging.format, config.logging.date_format)
    
    try:
        # Загрузка конфигурации
        config.cache.dir
        # Создание кэша
        cache = create_cache(
            cache_type=cache_type,
            cache_dir=config.cache.dir,
            redis_url=config.cache.redis.url,
            prefix=config.cache.redis.prefix,
            ttl_hours=config.cache.ttl_hours
        )
        
        # Создание трекера
        tracker = ContainerTracker(config, cache)
        
        # Сбор результатов
        results: List[TrackingResult] = []
        
        logging.info(f"🚀 Начинаем трекинг {len(container_numbers)} контейнеров")
        
        # Обработка контейнеров
        async for result in tracker.track_containers(container_numbers):
            results.append(result)
        
        # Сортировка результатов по номеру контейнера
        results.sort(key=lambda x: x.container_number)
        
        # Сохранение результатов
        await save_results(results, output_file)
        
        # Финальная статистика
        print_final_stats(results, output_file)
        
    except Exception as e:
        logging.error(f"❌ Критическая ошибка: {e}")
        raise


async def save_results(results: List[TrackingResult], output_file: str) -> None:
    """
    Сохранение результатов в JSON файл
    
    Args:
        results: Список результатов трекинга
        output_file: Путь к выходному файлу
    """
    
    # Создание директории если не существует
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Конвертация в словари для JSON
    results_dict = [asdict(result) for result in results]
    
    # Сохранение с красивым форматированием
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(results_dict, f, ensure_ascii=False, indent=2)
    
    logging.info(f"💾 Результаты сохранены: {output_file}")


def print_final_stats(results: List[TrackingResult], output_file: str) -> None:
    """
    Вывод финальной статистики
    
    Args:
        results: Список результатов трекинга
        output_file: Путь к файлу результатов
    """
    
    total = len(results)
    successful = sum(1 for r in results if r.success)
    failed = total - successful
    duplicates = sum(1 for r in results if r.has_duplicates)
    
    # Статистика по источникам данных
    sources_stats = {}
    for result in results:
        if result.success:
            source = result.events_source
            sources_stats[source] = sources_stats.get(source, 0) + 1
    
    success_rate = (successful / total * 100) if total > 0 else 0
    
    print("\n" + "="*60)
    print("📊 ФИНАЛЬНАЯ СТАТИСТИКА")
    print("="*60)
    print(f"📦 Всего контейнеров:    {total}")
    print(f"✅ Успешно обработано:   {successful} ({success_rate:.1f}%)")
    print(f"❌ Ошибок:               {failed}")
    print(f"🔄 С дедупликацией:      {duplicates}")
    print("\n📈 Источники данных:")
    
    for source, count in sources_stats.items():
        percentage = (count / successful * 100) if successful > 0 else 0
        print(f"   {source:12} {count:3} ({percentage:4.1f}%)")
    
    print(f"\n💾 Результаты: {output_file}")
    print("="*60)


async def example_usage():
    """Тестовая функция запуска"""

    # Список контейнеров для трекинга
    containers = [
        "TDSU6005411",
        "FESU5384983", 
        "TEMU1234567",
        "SKLU1575022",
        "SKLU3511665",
        "CCLU2903390",
        "SKHU8930645",
        "FESU2278740",
        "SKHU9806290",
        "BMOU2588739",
        "SUDU5969459"
    ]

    # Запуск трекинга
    await track_containers_from_list(
        container_numbers=containers,
        output_file=f"{config.output.dir}{config.output.filename}",
        cache_type="file",  # или "redis"
        # log_level="INFO"
    )


if __name__ == "__main__":
    # Запуск тестовый
    asyncio.run(example_usage())