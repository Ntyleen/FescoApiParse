#!/usr/bin/env python3
"""
FESCO Container Tracking System - Main Entry Point
==================================================

Режимы работы:
1. test     - Тестовый запуск с небольшим набором контейнеров
2. file     - Обработка контейнеров из файла
3. db       - Полная обработка из Firebird БД
4. monitor  - Режим мониторинга (показывает статистику)
"""

import asyncio
import argparse
import sys
import json
import time
from pathlib import Path
from typing import List, Optional
from datetime import datetime

# Добавляем корневую директорию в PYTHONPATH
sys.path.insert(0, str(Path(__file__).parent))

from config import load_config
from utils.logging import setup_logging_from_config, get_logger
from cache import create_cache
from models.processing_stats import ProcessingStats


class FescoTracker:
    """Главный класс приложения"""
    
    def __init__(self, environment: str = "development"):
        self.environment = environment
        self.config = None
        self.logger = None
        self.stats = ProcessingStats()
        
    async def initialize(self):
        """Инициализация приложения"""
        print(f"🚀 Инициализация FESCO Tracker ({self.environment})...")
        
        # Загрузка конфигурации
        self.config = load_config(environment=self.environment)
        
        # Настройка логирования
        setup_logging_from_config(self.config.logging)
        self.logger = get_logger("fesco_tracker.main")
        
        self.logger.info(f"✅ Конфигурация загружена для {self.environment}")
        
        # Проверка доступности компонентов
        await self._check_components()
    
    async def _check_components(self):
        """Проверка доступности компонентов"""
        self.logger.info("🔍 Проверка компонентов...") # type: ignore
        
        # Проверка Firebird
        try:
            from utils.db.firebird_manager import FIREBIRD_AVAILABLE
            if FIREBIRD_AVAILABLE:
                self.logger.info("✅ Firebird драйвер доступен") # type: ignore
            else:
                self.logger.warning("⚠️ Firebird драйвер недоступен") # type: ignore
        except ImportError:
            self.logger.warning("⚠️ Модуль Firebird не найден") # type: ignore
        
        # Проверка Redis
        try:
            from utils.redis_backend import REDIS_AVAILABLE
            if REDIS_AVAILABLE:
                self.logger.info("✅ Redis клиент доступен") # type: ignore
            else:
                self.logger.warning("⚠️ Redis клиент недоступен") # type: ignore
        except ImportError:
            self.logger.warning("⚠️ Модуль Redis не найден") # type: ignore
    
    async def run_test_mode(self, num_containers: int = 5):
        """Тестовый режим с ограниченным набором контейнеров"""
        self.logger.info(f"🧪 Запуск в тестовом режиме ({num_containers} контейнеров)") # type: ignore
        
        # Тестовые контейнеры
        test_containers = [
            "TDSU6005411",
            "FESU5384983",
            "TEMU1234567",
            "SKLU1575022",
            "SKLU3511665",
            "CCLU2903390",
            "SKHU8930645",
            "FESU2278740",
        ][:num_containers]
        
        # Используем простой трекер для теста
        await self._run_simple_tracking(test_containers)
    
    async def run_file_mode(self, file_path: str):
        """Режим обработки контейнеров из файла"""
        self.logger.info(f"📄 Загрузка контейнеров из файла: {file_path}") # type: ignore
        
        try:
            containers = self._load_containers_from_file(file_path)
            self.logger.info(f"📦 Загружено {len(containers)} контейнеров") # type: ignore
            
            await self._run_simple_tracking(containers)
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка загрузки файла: {e}") # type: ignore
            raise
    
    async def run_db_mode(self, batch_size: int = 100):
        """Режим полной обработки из БД"""
        self.logger.info(f"🗄️ Запуск обработки из БД (batch_size={batch_size})") # type: ignore
        
        try:
            # Пробуем использовать новый Engine
            from processing import create_tracking_engine
            
            engine = await create_tracking_engine(
                self.config,
                cache_type="auto"
            )
            
            self.logger.info("🎼 Используем ContainerTrackingEngine") # type: ignore
            
            stats = await engine.run_full_workflow(
                batch_size=batch_size,
                target_line_ids=set(self.config.processing.get('target_lines', [])) # type: ignore
            )
            
            self._print_engine_stats(stats)
            
        except ImportError:
            self.logger.warning("⚠️ ContainerTrackingEngine недоступен, используем legacy режим")
            await self._run_legacy_db_mode(batch_size)
    
    async def run_monitor_mode(self):
        """Режим мониторинга системы"""
        self.logger.info("📊 Режим мониторинга")
        
        # Собираем статистику
        stats = {
            'timestamp': datetime.now().isoformat(),
            'environment': self.environment,
            'components': {}
        }
        
        # Проверка БД
        try:
            from utils.db.firebird_manager import create_firebird_entity_manager
            
            manager = create_firebird_entity_manager(
                host=self.config.database.host,
                database=self.config.database.database,
                user=self.config.database.user,
                password=self.config.database.password
            )
            
            if await manager.test_connection():
                db_stats = await manager.get_entity_statistics()
                stats['components']['database'] = {
                    'status': 'connected',
                    'stats': db_stats
                }
            else:
                stats['components']['database'] = {'status': 'disconnected'}
                
            await manager.close()
            
        except Exception as e:
            stats['components']['database'] = {
                'status': 'error',
                'error': str(e)
            }
        
        # Проверка Redis
        try:
            if self.config.cache.type == "redis":
                from utils.redis_backend import create_redis_manager
                
                redis_manager = create_redis_manager(self.config.cache.redis.url)
                if redis_manager:
                    redis_stats = await redis_manager.get_stats()
                    stats['components']['redis'] = {
                        'status': 'connected',
                        'stats': redis_stats
                    }
                    await redis_manager.close()
                else:
                    stats['components']['redis'] = {'status': 'unavailable'}
        except Exception as e:
            stats['components']['redis'] = {
                'status': 'error',
                'error': str(e)
            }
        
        # Вывод статистики
        print("\n" + "="*60)
        print("📊 СТАТУС СИСТЕМЫ")
        print("="*60)
        print(json.dumps(stats, indent=2, ensure_ascii=False))
    
    async def _run_simple_tracking(self, containers: List[str]):
        """Простой трекинг для тестирования"""
        from processing import ContainerTracker
        
        # Создаем кэш
        cache = create_cache(
            cache_type=self.config.cache.type, # type: ignore
            cache_dir=self.config.cache.dir,
            redis_url=self.config.cache.redis.url if self.config.cache.type == "redis" else None # type: ignore
        )
        
        # Создаем трекер
        tracker = ContainerTracker(self.config, cache)
        
        # Счетчики для статистики
        results = []
        start_time = time.perf_counter()
        
        # Прогресс
        self.logger.info(f"🚀 Начинаем трекинг {len(containers)} контейнеров...")
        
        # Обработка
        async for result in tracker.track_containers(containers):
            results.append(result)
            
            # Показываем прогресс каждые 10 контейнеров
            if len(results) % 10 == 0:
                self.logger.info(f"Обработано: {len(results)}/{len(containers)}")
        
        # Статистика
        end_time = time.perf_counter()
        duration = end_time - start_time
        
        successful = sum(1 for r in results if r.success)
        failed = len(results) - successful
        
        # Сохранение результатов
        output_path = Path(self.config.output.dir) / f"test_results_{int(time.time())}.json"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(
                [self._result_to_dict(r) for r in results],
                f,
                ensure_ascii=False,
                indent=2
            )
        
        # Вывод статистики
        print("\n" + "="*60)
        print("📊 РЕЗУЛЬТАТЫ ТРЕКИНГА")
        print("="*60)
        print(f"📦 Всего контейнеров:     {len(results)}")
        print(f"✅ Успешно:              {successful}")
        print(f"❌ Ошибки:               {failed}")
        print(f"⏱️ Время выполнения:     {duration:.2f} сек")
        print(f"⚡ Скорость:             {len(results)/duration:.1f} конт/сек")
        print(f"💾 Результаты сохранены: {output_path}")
        print("="*60)
    
    async def _run_legacy_db_mode(self, batch_size: int):
        """Legacy режим работы с БД"""
        self.logger.warning("Legacy режим пока не реализован")
        # TODO: Реализовать когда понадобится
    
    def _load_containers_from_file(self, file_path: str) -> List[str]:
        """Загрузка контейнеров из файла"""
        path = Path(file_path)
        
        if not path.exists():
            raise FileNotFoundError(f"Файл не найден: {file_path}")
        
        containers = []
        
        # Поддержка разных форматов
        if path.suffix.lower() == '.json':
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, list):
                    containers = data
                elif isinstance(data, dict) and 'containers' in data:
                    containers = data['containers']
        
        elif path.suffix.lower() in ['.txt', '.csv']:
            with open(path, 'r', encoding='utf-8') as f:
                for line in f:
                    container = line.strip()
                    if container and not container.startswith('#'):
                        containers.append(container)
        
        else:
            raise ValueError(f"Неподдерживаемый формат файла: {path.suffix}")
        
        return containers
    
    def _result_to_dict(self, result) -> dict:
        """Конвертация результата в словарь"""
        from dataclasses import asdict, is_dataclass
        
        if is_dataclass(result):
            return asdict(result)
        else:
            # Fallback для не-dataclass объектов
            return {
                'container_number': getattr(result, 'container_number', None),
                'success': getattr(result, 'success', False),
                'error_message': getattr(result, 'error_message', None),
                'last_event': getattr(result, 'last_event', None)
            }
    
    def _print_engine_stats(self, stats):
        """Вывод статистики Engine"""
        print("\n" + "="*60)
        print("📊 СТАТИСТИКА ОБРАБОТКИ")
        print("="*60)
        
        for key, value in vars(stats).items():
            print(f"{key}: {value}")
        
        print("="*60)


def create_parser():
    """Создание парсера аргументов командной строки"""
    parser = argparse.ArgumentParser(
        description="FESCO Container Tracking System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  
  # Тестовый запуск с 5 контейнерами
  python main.py test
  
  # Обработка контейнеров из файла
  python main.py file containers.txt
  
  # Полная обработка из БД
  python main.py db --batch-size 100
  
  # Мониторинг системы
  python main.py monitor
        """
    )
    
    # Общие аргументы
    parser.add_argument(
        '--env', '--environment',
        choices=['development', 'production', 'test'],
        default='development',
        help='Окружение для запуска'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Подробный вывод'
    )
    
    # Подкоманды
    subparsers = parser.add_subparsers(dest='mode', help='Режим работы')
    
    # Test mode
    test_parser = subparsers.add_parser('test', help='Тестовый режим')
    test_parser.add_argument(
        '--count', '-n',
        type=int,
        default=5,
        help='Количество тестовых контейнеров'
    )
    
    # File mode
    file_parser = subparsers.add_parser('file', help='Обработка из файла')
    file_parser.add_argument(
        'file_path',
        help='Путь к файлу с контейнерами'
    )
    
    # DB mode
    db_parser = subparsers.add_parser('db', help='Обработка из БД')
    db_parser.add_argument(
        '--batch-size',
        type=int,
        default=100,
        help='Размер батча для обработки'
    )
    
    # Monitor mode
    monitor_parser = subparsers.add_parser('monitor', help='Мониторинг системы')
    
    return parser


async def main():
    """Главная функция"""
    parser = create_parser()
    args = parser.parse_args()
    
    # Если режим не указан, показываем help
    if not args.mode:
        parser.print_help()
        sys.exit(1)
    
    # Создаем и инициализируем приложение
    app = FescoTracker(environment=args.env)
    
    try:
        await app.initialize()
        
        # Запускаем в выбранном режиме
        if args.mode == 'test':
            await app.run_test_mode(args.count)
        
        elif args.mode == 'file':
            await app.run_file_mode(args.file_path)
        
        elif args.mode == 'db':
            await app.run_db_mode(args.batch_size)
        
        elif args.mode == 'monitor':
            await app.run_monitor_mode()
        
        else:
            parser.print_help()
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n⚠️ Прервано пользователем")
        sys.exit(130)
        
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    # Для Windows - исправляем проблемы с asyncio
    if sys.platform.startswith('win'):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    
    asyncio.run(main())
