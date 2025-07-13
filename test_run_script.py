#!/usr/bin/env python3
"""
Скрипт для быстрого тестирования FESCO Tracker
==============================================

Запускает тесты производительности и проверяет работу всех компонентов.
"""

import asyncio
import time
from psutil import Process, virtual_memory
import os
from datetime import datetime

# Для мониторинга ресурсов
def get_system_resources():
    """Получить текущее использование ресурсов"""
    process = Process(os.getpid())
    
    return {
        'cpu_percent': process.cpu_percent(interval=0.1),
        'memory_mb': process.memory_info().rss / 1024 / 1024,
        'threads': process.num_threads(),
        'open_files': len(process.open_files()),
        'connections': len(process.connections())
    }


async def test_minimal():
    """Минимальный тест - 3 контейнера"""
    print("\n🧪 ТЕСТ 1: Минимальный (3 контейнера)")
    print("-" * 50)
    
    # Импортируем напрямую из модулей проекта
    from config import load_config
    from cache import create_cache
    from processing import ContainerTracker
    
    containers = ["TDSU6005411", "FESU5384983", "TEMU1234567"]
    
    # Создаем компоненты для трекинга
    config = load_config(environment="development")
    cache = create_cache(cache_type="file", cache_dir="./test_cache")
    tracker = ContainerTracker(config, cache)
    
    start_resources = get_system_resources()
    start_time = time.time()
    
    # Собираем результаты
    results = []
    async for result in tracker.track_containers(containers):
        results.append(result)
    
    end_time = time.time()
    end_resources = get_system_resources()
    
    # Статистика
    duration = end_time - start_time
    successful = sum(1 for r in results if r.success)
    
    # Очистка
    await cache.close()
    
    print(f"✅ Обработано: {len(results)} контейнеров")
    print(f"✅ Успешно: {successful}")
    print(f"⏱️ Время: {duration:.2f} сек")
    print(f"💾 Память: {start_resources['memory_mb']:.1f} → {end_resources['memory_mb']:.1f} МБ")
    print(f"🔥 CPU: {end_resources['cpu_percent']:.1f}%")
    
    return results


async def test_performance():
    """Тест производительности - 50 контейнеров"""
    print("\n🚀 ТЕСТ 2: Производительность (50 контейнеров)")
    print("-" * 50)
    
    from config import load_config
    from cache import create_cache
    from processing import ContainerTracker
    
    # Генерируем тестовые контейнеры
    containers = [
        f"TEST{str(i).zfill(7)}" for i in range(50)
    ]
    
    config = load_config(environment="development")
    cache = create_cache(cache_type="file", cache_dir="./test_cache")
    tracker = ContainerTracker(config, cache)
    
    start_resources = get_system_resources()
    start_time = time.time()
    
    # Обработка с отслеживанием прогресса
    results = []
    checkpoints = []
    
    async for i, result in enumerate(tracker.track_containers(containers)):
        results.append(result)
        
        # Записываем метрики каждые 10 контейнеров
        if (i + 1) % 10 == 0:
            checkpoint_time = time.time() - start_time
            checkpoint_resources = get_system_resources()
            checkpoints.append({
                'containers': i + 1,
                'time': checkpoint_time,
                'memory_mb': checkpoint_resources['memory_mb'],
                'rate': (i + 1) / checkpoint_time
            })
    
    end_time = time.time()
    end_resources = get_system_resources()
    
    # Анализ результатов
    duration = end_time - start_time
    avg_rate = len(results) / duration
    
    print(f"\n📊 Результаты:")
    print(f"✅ Обработано: {len(results)} контейнеров")
    print(f"⏱️ Общее время: {duration:.2f} сек")
    print(f"⚡ Средняя скорость: {avg_rate:.1f} конт/сек")
    print(f"\n💾 Использование памяти:")
    print(f"   Начало: {start_resources['memory_mb']:.1f} МБ")
    print(f"   Конец: {end_resources['memory_mb']:.1f} МБ")
    print(f"   Прирост: {end_resources['memory_mb'] - start_resources['memory_mb']:.1f} МБ")
    
    print(f"\n📈 Прогресс по времени:")
    for cp in checkpoints:
        print(f"   {cp['containers']:3d} конт: {cp['time']:5.1f}с ({cp['rate']:.1f} конт/сек, {cp['memory_mb']:.1f} МБ)")
    
    # Очистка
    await cache.clear()
    await cache.close()
    
    return results


async def test_cache_efficiency():
    """Тест эффективности кэширования"""
    print("\n💾 ТЕСТ 3: Эффективность кэширования")
    print("-" * 50)
    
    from config import load_config
    from cache import create_cache
    from models.processing_stats import ProcessingStats
    from api import FescoApiClient
    import aiohttp
    
    config = load_config(environment="development")
    cache = create_cache(cache_type="file", cache_dir="./test_cache")
    stats = ProcessingStats()
    
    api_client = FescoApiClient(config, cache, stats)
    
    test_container = "TDSU6005411"
    
    async with aiohttp.ClientSession() as session:
        # Первый запрос (без кэша)
        start1 = time.time()
        result1 = await api_client.find_order_by_container(session, test_container)
        time1 = time.time() - start1
        
        # Второй запрос (с кэшем)
        start2 = time.time()
        result2 = await api_client.find_order_by_container(session, test_container)
        time2 = time.time() - start2
    
    print(f"🌐 Первый запрос (без кэша): {time1:.3f} сек")
    print(f"💾 Второй запрос (с кэшем): {time2:.3f} сек")
    print(f"⚡ Ускорение: {time1/time2:.1f}x")
    print(f"📊 Статистика кэша: {stats.cached_requests} попаданий")
    
    # Очистка
    await cache.clear()
    await cache.close()


async def test_error_handling():
    """Тест обработки ошибок"""
    print("\n❌ ТЕСТ 4: Обработка ошибок")
    print("-" * 50)
    
    from config import load_config
    from cache import create_cache
    from processing import ContainerTracker
    
    config = load_config(environment="development")
    cache = create_cache(cache_type="file", cache_dir="./test_cache")
    tracker = ContainerTracker(config, cache)
    
    # Тестируем с невалидными контейнерами
    invalid_containers = [
        "",                    # Пустая строка
        "INVALID",            # Короткий номер
        "XXXX9999999",       # Несуществующий
    ]
    
    errors = 0
    for container in invalid_containers:
        try:
            results = []
            async for result in tracker.track_containers([container]):
                results.append(result)
            
            if results and not results[0].success:
                errors += 1
                print(f"✅ Корректно обработана ошибка для: '{container}'")
        except Exception as e:
            print(f"❌ Необработанное исключение для '{container}': {e}")
    
    print(f"\n📊 Обработано ошибок: {errors}")
    
    # Очистка
    await cache.close()


async def test_database_connection():
    """Тест подключения к БД"""
    print("\n🗄️ ТЕСТ 5: Подключение к базе данных")
    print("-" * 50)
    
    try:
        from utils.db.firebird_manager import FIREBIRD_AVAILABLE
        
        if not FIREBIRD_AVAILABLE:
            print("⚠️ Firebird драйвер не установлен")
            return
        
        from config import load_config
        from utils.db.firebird_manager import create_firebird_entity_manager
        
        config = load_config()
        
        manager = create_firebird_entity_manager(
            host=config.database.host,
            database=config.database.database,
            user=config.database.user,
            password=config.database.password
        )
        
        # Тест подключения
        connected = await manager.test_connection()
        print(f"🔗 Подключение: {'✅ Успешно' if connected else '❌ Ошибка'}")
        
        if connected:
            # Получаем статистику
            stats = await manager.get_entity_statistics()
            if stats:
                print(f"📊 Всего записей: {stats.get('total_records', 0)}")
                print(f"📦 Доступно для обработки: {stats.get('available_for_processing', 0)}")
        
        await manager.close()
        
    except Exception as e:
        print(f"❌ Ошибка теста БД: {e}")


async def run_all_tests():
    """Запуск всех тестов"""
    print("="*60)
    print("🧪 FESCO TRACKER - КОМПЛЕКСНОЕ ТЕСТИРОВАНИЕ")
    print("="*60)
    print(f"📅 Дата: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🖥️ Система: {os.name} ({os.cpu_count()} CPU)")
    print(f"💾 RAM: {virtual_memory().total / (1024**3):.1f} GB")
    
    # Проверка компонентов
    print("\n📋 Статус компонентов:")
    
    # Проверяем Firebird
    try:
        from utils.db.firebird_manager import FIREBIRD_AVAILABLE
        firebird_status = "✅ Доступен" if FIREBIRD_AVAILABLE else "⚠️ Не установлен"
        print(f"  Firebird: {firebird_status}")
    except ImportError:
        print("  Firebird: ❌ Модуль не найден")
    
    # Проверяем Redis
    try:
        from utils.redis_backend import REDIS_AVAILABLE
        redis_status = "✅ Доступен" if REDIS_AVAILABLE else "⚠️ Не установлен"
        print(f"  Redis: {redis_status}")
    except ImportError:
        print("  Redis: ❌ Модуль не найден")
    
    # Запуск тестов
    tests = [
        test_minimal,
        test_performance,
        test_cache_efficiency,
        test_error_handling,
        test_database_connection
    ]
    
    for test in tests:
        try:
            await test()
        except Exception as e:
            print(f"\n❌ Ошибка в тесте {test.__name__}: {e}")
            import traceback
            traceback.print_exc()
    
    print("\n" + "="*60)
    print("✅ ТЕСТИРОВАНИЕ ЗАВЕРШЕНО")
    print("="*60)


if __name__ == "__main__":
    # Запускаем все тесты
    asyncio.run(run_all_tests())
