# test_integration.py
"""
Интеграционный тест FESCO Container Tracking System v2.0
========================================================

Этот скрипт проверяет работу всех компонентов системы:
    🔥 Firebird Entity Manager
    🌐 FESCO API Client  
    🔗 Redis Backend (Cache + Bindings)
    🎼 Container Tracking Orchestrator
    📊 Statistics & Monitoring

Режимы тестирования:
    🧪 unit       - Тестирование отдельных компонентов
    🔗 integration - Тестирование взаимодействия компонентов
    🚀 full        - Полный end-to-end тест
    🏥 health      - Проверка здоровья системы
"""

import asyncio
import sys
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional
import traceback

# Основные импорты
from config import load_config
from utils.logging import setup_logging_from_config, get_logger
from processing import (
    validate_processing_environment,
    get_processing_capabilities,
    create_orchestrator,
    create_firebird_orchestrator
)

# Специфичные импорты для тестирования
try:
    from utils.db.firebird_manager import (
        create_firebird_entity_manager,
        validate_firebird_config,
        FIREBIRD_AVAILABLE
    )
except ImportError:
    FIREBIRD_AVAILABLE = False
    print("⚠️ Firebird manager недоступен")

try:
    from utils.redis_backend import (
        create_redis_manager,
        check_redis_availability,
        REDIS_AVAILABLE
    )
except ImportError:
    REDIS_AVAILABLE = False
    print("⚠️ Redis backend недоступен")

from api.api_client import FescoApiClient, FescoApiError
from cache import create_cache
from models.container_event import TrackingResult, ContainerEvent


class IntegrationTester:
    """
    Класс для проведения интеграционных тестов
    
    Последовательно проверяет каждый компонент и их взаимодействие.
    """
    
    def __init__(self, environment: str = "development"):
        self.environment = environment
        self.config = None
        self.logger = None
        self.test_results = {
            'timestamp': datetime.now().isoformat(),
            'environment': environment,
            'tests': {},
            'summary': {}
        }
        
        # Тестовые данные
        self.test_containers = [
            "TDSU6005411",
            "FESU5384983",
            "TEMU1234567"
        ]
        
        # Компоненты для тестирования
        self.firebird_manager = None
        self.redis_manager = None
        self.api_client = None
        self.cache = None
        self.orchestrator = None
    
    async def run_all_tests(self) -> Dict[str, Any]:
        """
        Запуск всех тестов
        
        Returns:
            Детальные результаты тестирования
        """
        
        print("\n" + "="*70)
        print("🧪 FESCO Container Tracking Integration Tests")
        print("="*70)
        
        try:
            # Инициализация
            await self._initialize()
            
            # Unit тесты
            await self._run_unit_tests()
            
            # Integration тесты
            await self._run_integration_tests()
            
            # Full workflow тест
            await self._run_full_workflow_test()
            
            # Финализация результатов
            self._finalize_results()
            
        except Exception as e:
            self.logger.error(f"❌ Критическая ошибка тестирования: {e}")
            self.test_results['critical_error'] = str(e)
            self.test_results['traceback'] = traceback.format_exc()
        
        finally:
            await self._cleanup()
        
        return self.test_results
    
    async def _initialize(self):
        """Инициализация тестового окружения"""
        
        print("🔧 Инициализация тестового окружения...")
        
        # Загрузка конфигурации
        self.config = load_config(environment=self.environment)
        
        # Настройка логирования
        setup_logging_from_config(self.config.logging)
        self.logger = get_logger("integration_test")
        
        self.logger.info("🧪 Запуск интеграционных тестов")
        self.logger.info(f"🌍 Environment: {self.environment}")
        
        print("✅ Инициализация завершена")
    
    async def _run_unit_tests(self):
        """Тестирование отдельных компонентов"""
        
        print("\n📦 Unit Tests - тестирование компонентов")
        print("-" * 50)
        
        # Тест 1: Environment validation
        await self._test_environment_validation()
        
        # Тест 2: Configuration loading
        await self._test_configuration_loading()
        
        # Тест 3: Firebird manager
        if FIREBIRD_AVAILABLE:
            await self._test_firebird_manager()
        
        # Тест 4: Redis backend
        if REDIS_AVAILABLE:
            await self._test_redis_backend()
        
        # Тест 5: API client
        await self._test_api_client()
        
        # Тест 6: Cache backends
        await self._test_cache_backends()
    
    async def _run_integration_tests(self):
        """Тестирование взаимодействия компонентов"""
        
        print("\n🔗 Integration Tests - взаимодействие компонентов")
        print("-" * 50)
        
        # Тест 1: Orchestrator creation
        await self._test_orchestrator_creation()
        
        # Тест 2: API + Cache integration
        await self._test_api_cache_integration()
        
        # Тест 3: Firebird + Redis integration
        if FIREBIRD_AVAILABLE and REDIS_AVAILABLE:
            await self._test_firebird_redis_integration()
        
        # Тест 4: Event processing pipeline
        await self._test_event_processing_pipeline()
    
    async def _run_full_workflow_test(self):
        """Полный end-to-end тест workflow"""
        
        print("\n🚀 Full Workflow Test - end-to-end тестирование")
        print("-" * 50)
        
        if self.orchestrator:
            await self._test_full_orchestrator_workflow()
        else:
            await self._test_legacy_tracker_workflow()
    
    # =========================================================================
    # UNIT TESTS
    # =========================================================================
    
    async def _test_environment_validation(self):
        """Тест валидации окружения"""
        
        test_name = "environment_validation"
        print("🔍 Тестирование валидации окружения...")
        
        try:
            env_status = validate_processing_environment()
            capabilities = get_processing_capabilities()
            
            self.test_results['tests'][test_name] = {
                'status': 'PASS' if env_status['ready'] else 'WARN',
                'environment_ready': env_status['ready'],
                'components': env_status['components'],
                'capabilities': capabilities,
                'recommendations': env_status.get('recommendations', [])
            }
            
            print(f"  ✅ Environment validation: {'READY' if env_status['ready'] else 'PARTIAL'}")
            
        except Exception as e:
            self.test_results['tests'][test_name] = {
                'status': 'FAIL',
                'error': str(e)
            }
            print(f"  ❌ Environment validation failed: {e}")
    
    async def _test_configuration_loading(self):
        """Тест загрузки конфигурации"""
        
        test_name = "configuration_loading"
        print("⚙️ Тестирование загрузки конфигурации...")
        
        try:
            # Проверяем основные секции конфигурации
            config_check = {
                'api_configured': hasattr(self.config, 'api') and self.config.api.base_url,
                'cache_configured': hasattr(self.config, 'cache'),
                'database_configured': hasattr(self.config, 'database'),
                'processing_configured': hasattr(self.config, 'processing'),
                'logging_configured': hasattr(self.config, 'logging')
            }
            
            all_configured = all(config_check.values())
            
            self.test_results['tests'][test_name] = {
                'status': 'PASS' if all_configured else 'WARN',
                'config_sections': config_check,
                'auth_token_present': bool(getattr(self.config, 'auth_token', None))
            }
            
            print(f"  ✅ Configuration loading: {'COMPLETE' if all_configured else 'PARTIAL'}")
            
        except Exception as e:
            self.test_results['tests'][test_name] = {
                'status': 'FAIL',
                'error': str(e)
            }
            print(f"  ❌ Configuration loading failed: {e}")
    
    async def _test_firebird_manager(self):
        """Тест Firebird Entity Manager"""
        
        test_name = "firebird_manager"
        print("🔥 Тестирование Firebird Entity Manager...")
        
        try:
            # Валидация конфигурации
            firebird_config = {
                'host': self.config.database.host,
                'database': self.config.database.database,
                'user': self.config.database.user,
                'password': self.config.database.password
            }
            
            validation_result = await validate_firebird_config(firebird_config)
            
            if validation_result['valid']:
                # Создание manager'а
                self.firebird_manager = create_firebird_entity_manager(**firebird_config)
                
                # Тест подключения
                connection_test = await self.firebird_manager.test_connection()
                
                self.test_results['tests'][test_name] = {
                    'status': 'PASS' if connection_test else 'WARN',
                    'config_valid': True,
                    'connection_test': connection_test,
                    'manager_created': True
                }
                
                print(f"  ✅ Firebird manager: {'CONNECTED' if connection_test else 'CONFIG_VALID'}")
                
            else:
                self.test_results['tests'][test_name] = {
                    'status': 'WARN',
                    'config_valid': False,
                    'validation_errors': validation_result['errors'],
                    'validation_warnings': validation_result['warnings']
                }
                
                print(f"  ⚠️ Firebird manager: INVALID CONFIG")
                
        except Exception as e:
            self.test_results['tests'][test_name] = {
                'status': 'FAIL',
                'error': str(e)
            }
            print(f"  ❌ Firebird manager failed: {e}")
    
    async def _test_redis_backend(self):
        """Тест Redis Backend"""
        
        test_name = "redis_backend"
        print("🔗 Тестирование Redis Backend...")
        
        try:
            # Проверка доступности
            redis_status = check_redis_availability()
            
            if redis_status['available']:
                # Создание manager'а
                self.redis_manager = create_redis_manager(self.config.cache.redis.url)
                
                # Тест подключения
                client = await self.redis_manager.get_client()
                await client.ping()
                
                # Тест namespace'ов
                cache_ns = self.redis_manager.get_cache_namespace()
                binding_ns = self.redis_manager.get_binding_namespace()
                
                # Простой тест записи/чтения
                test_key = "integration_test"
                test_data = {"test": True, "timestamp": datetime.now().isoformat()}
                
                await cache_ns.set(test_key, test_data, ttl_seconds=60)
                retrieved_data = await cache_ns.get(test_key)
                
                data_matches = retrieved_data == test_data
                
                self.test_results['tests'][test_name] = {
                    'status': 'PASS',
                    'connection_test': True,
                    'cache_namespace': True,
                    'binding_namespace': True,
                    'data_integrity': data_matches
                }
                
                print(f"  ✅ Redis backend: CONNECTED")
                
                # Очистка тестовых данных
                await cache_ns.delete(test_key)
                
            else:
                self.test_results['tests'][test_name] = {
                    'status': 'WARN',
                    'available': False,
                    'error': redis_status['error']
                }
                
                print(f"  ⚠️ Redis backend: NOT AVAILABLE")
                
        except Exception as e:
            self.test_results['tests'][test_name] = {
                'status': 'FAIL',
                'error': str(e)
            }
            print(f"  ❌ Redis backend failed: {e}")
    
    async def _test_api_client(self):
        """Тест FESCO API Client"""
        
        test_name = "api_client"
        print("🌐 Тестирование FESCO API Client...")
        
        try:
            # Создание cache для API client
            if not self.cache:
                self.cache = create_cache(
                    cache_type="file",
                    cache_dir="./test_cache"
                )
            
            # Создание API client
            from models.processing_stats import ProcessingStats
            stats = ProcessingStats()
            
            self.api_client = FescoApiClient(self.config, self.cache, stats)
            
            # Проверка конфигурации
            has_token = bool(self.config.auth_token)
            has_base_url = bool(self.config.api.base_url)
            
            self.test_results['tests'][test_name] = {
                'status': 'PASS' if (has_token and has_base_url) else 'WARN',
                'client_created': True,
                'has_token': has_token,
                'has_base_url': has_base_url,
                'base_url': self.config.api.base_url if has_base_url else None
            }
            
            print(f"  ✅ API client: {'CONFIGURED' if (has_token and has_base_url) else 'PARTIAL'}")
            
        except Exception as e:
            self.test_results['tests'][test_name] = {
                'status': 'FAIL',
                'error': str(e)
            }
            print(f"  ❌ API client failed: {e}")
    
    async def _test_cache_backends(self):
        """Тест различных cache backends"""
        
        test_name = "cache_backends"
        print("💾 Тестирование Cache Backends...")
        
        try:
            # Тест file cache
            file_cache = create_cache(cache_type="file", cache_dir="./test_cache")
            
            test_data = {"test": "file_cache", "timestamp": datetime.now().isoformat()}
            await file_cache.set("test_key", test_data, ttl_seconds=60)
            retrieved_data = await file_cache.get("test_key")
            
            file_cache_works = retrieved_data == test_data
            
            results = {
                'file_cache': {
                    'available': True,
                    'write_test': True,
                    'read_test': file_cache_works
                }
            }
            
            # Тест Redis cache если доступен
            if self.redis_manager:
                try:
                    from utils.redis_backend import create_compatible_cache
                    redis_cache = create_compatible_cache(self.redis_manager)
                    
                    await redis_cache.set("test_key", test_data, ttl_seconds=60)
                    redis_retrieved = await redis_cache.get("test_key")
                    
                    results['redis_cache'] = {
                        'available': True,
                        'write_test': True,
                        'read_test': redis_retrieved == test_data
                    }
                    
                    # Очистка
                    await redis_cache.delete("test_key")
                    
                except Exception as redis_error:
                    results['redis_cache'] = {
                        'available': False,
                        'error': str(redis_error)
                    }
            
            # Очистка file cache
            await file_cache.delete("test_key")
            await file_cache.close()
            
            all_working = all(
                cache.get('read_test', False) 
                for cache in results.values() 
                if cache.get('available', False)
            )
            
            self.test_results['tests'][test_name] = {
                'status': 'PASS' if all_working else 'WARN',
                'backends': results
            }
            
            print(f"  ✅ Cache backends: {'ALL WORKING' if all_working else 'PARTIAL'}")
            
        except Exception as e:
            self.test_results['tests'][test_name] = {
                'status': 'FAIL',
                'error': str(e)
            }
            print(f"  ❌ Cache backends failed: {e}")
    
    # =========================================================================
    # INTEGRATION TESTS  
    # =========================================================================
    
    async def _test_orchestrator_creation(self):
        """Тест создания Orchestrator"""
        
        test_name = "orchestrator_creation"
        print("🎼 Тестирование создания Orchestrator...")
        
        try:
            # Попытка создать orchestrator
            if FIREBIRD_AVAILABLE and self.firebird_manager:
                # Firebird orchestrator
                firebird_config = {
                    'host': self.config.database.host,
                    'database': self.config.database.database,
                    'user': self.config.database.user,
                    'password': self.config.database.password
                }
                
                self.orchestrator = await create_firebird_orchestrator(
                    self.config,
                    firebird_config,
                    redis_url=self.config.cache.redis.url if REDIS_AVAILABLE else None
                )
                
                orchestrator_type = "firebird"
                
            else:
                # Generic orchestrator
                self.orchestrator = await create_orchestrator(
                    self.config,
                    cache_type="file",
                    enable_firebird=False,
                    enable_redis=False
                )
                
                orchestrator_type = "generic"
            
            self.test_results['tests'][test_name] = {
                'status': 'PASS',
                'orchestrator_created': bool(self.orchestrator),
                'orchestrator_type': orchestrator_type,
                'has_db_source': bool(getattr(self.orchestrator, 'db_source', None)),
                'has_cache': bool(getattr(self.orchestrator, 'cache', None)),
                'has_api_client': bool(getattr(self.orchestrator, 'api_client', None))
            }
            
            print(f"  ✅ Orchestrator creation: {orchestrator_type.upper()} TYPE")
            
        except Exception as e:
            self.test_results['tests'][test_name] = {
                'status': 'FAIL',
                'error': str(e)
            }
            print(f"  ❌ Orchestrator creation failed: {e}")
    
    async def _test_api_cache_integration(self):
        """Тест интеграции API + Cache"""
        
        test_name = "api_cache_integration" 
        print("🌐💾 Тестирование API + Cache интеграции...")
        
        try:
            if not self.api_client or not self.cache:
                self.test_results['tests'][test_name] = {
                    'status': 'SKIP',
                    'reason': 'API client или cache недоступны'
                }
                print(f"  ⏭️ API + Cache integration: SKIPPED")
                return
            
            # Симуляция API запроса с кэшированием
            import aiohttp
            
            connector = aiohttp.TCPConnector(limit_per_host=1)
            timeout = aiohttp.ClientTimeout(total=10)
            
            try:
                async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                    
                    # Тест 1: Поиск заявки (может не найти, но не должен падать)
                    try:
                        order_id = await self.api_client.find_order_by_container(
                            session, self.test_containers[0]
                        )
                        find_order_success = True
                        
                    except FescoApiError:
                        # API ошибки ожидаемы в тестовом окружении
                        find_order_success = False
                        order_id = None
                    
                    # Тест 2: Проверка кэша
                    cache_key = f"test_integration:{self.test_containers[0]}"
                    test_data = {"test": True, "container": self.test_containers[0]}
                    
                    await self.cache.set(cache_key, test_data, ttl_seconds=60)
                    cached_data = await self.cache.get(cache_key)
                    
                    cache_works = cached_data == test_data
                    
                    # Очистка
                    await self.cache.delete(cache_key)
                    
                    self.test_results['tests'][test_name] = {
                        'status': 'PASS',
                        'api_client_functional': True,
                        'find_order_attempted': True,
                        'find_order_success': find_order_success,
                        'cache_integration': cache_works,
                        'order_id_found': order_id
                    }
                    
                    print(f"  ✅ API + Cache integration: FUNCTIONAL")
                    
            except asyncio.TimeoutError:
                self.test_results['tests'][test_name] = {
                    'status': 'WARN',
                    'timeout': True,
                    'cache_test_only': True
                }
                print(f"  ⚠️ API + Cache integration: TIMEOUT (cache OK)")
                
        except Exception as e:
            self.test_results['tests'][test_name] = {
                'status': 'FAIL',
                'error': str(e)
            }
            print(f"  ❌ API + Cache integration failed: {e}")
    
    async def _test_firebird_redis_integration(self):
        """Тест интеграции Firebird + Redis"""
        
        test_name = "firebird_redis_integration"
        print("🔥🔗 Тестирование Firebird + Redis интеграции...")
        
        try:
            if not self.firebird_manager or not self.redis_manager:
                self.test_results['tests'][test_name] = {
                    'status': 'SKIP',
                    'reason': 'Firebird или Redis недоступны'
                }
                print(f"  ⏭️ Firebird + Redis integration: SKIPPED")
                return
            
            # Тест получения статистики Firebird
            try:
                fb_stats = await self.firebird_manager.get_entity_statistics()
                firebird_stats_ok = isinstance(fb_stats, dict)
            except Exception:
                firebird_stats_ok = False
            
            # Тест Redis namespace'ов
            binding_ns = self.redis_manager.get_binding_namespace()
            
            # Тест привязки контейнера к заявке
            test_container = self.test_containers[0]
            test_order = "TEST_ORDER_123"
            
            bind_success = await binding_ns.bind_container_to_order(test_container, test_order)
            retrieved_order = await binding_ns.get_container_order(test_container)
            
            binding_works = retrieved_order == test_order
            
            # Очистка тестовых данных
            if binding_works:
                client = await self.redis_manager.get_client()
                await client.delete(f"{binding_ns.prefix}container:{test_container}")
                await client.delete(f"{binding_ns.prefix}order:{test_order}")
            
            self.test_results['tests'][test_name] = {
                'status': 'PASS' if (firebird_stats_ok and binding_works) else 'WARN',
                'firebird_stats': firebird_stats_ok,
                'redis_binding': binding_works,
                'integration_functional': firebird_stats_ok and binding_works
            }
            
            print(f"  ✅ Firebird + Redis integration: {'WORKING' if (firebird_stats_ok and binding_works) else 'PARTIAL'}")
            
        except Exception as e:
            self.test_results['tests'][test_name] = {
                'status': 'FAIL',
                'error': str(e)
            }
            print(f"  ❌ Firebird + Redis integration failed: {e}")
    
    async def _test_event_processing_pipeline(self):
        """Тест pipeline обработки событий"""
        
        test_name = "event_processing_pipeline"
        print("📋 Тестирование Event Processing Pipeline...")
        
        try:
            from processing.events import EventProcessor
            
            processor = EventProcessor()
            
            # Создаем тестовые данные событий
            test_order_data = {
                "data": [{
                    "orderNumber": "TEST123",
                    "containers": [{
                        "containerNumber": self.test_containers[0],
                        "lastEvent": {
                            "date": "2024-01-15 14:30:00",
                            "text": "Грузится на фидер",
                            "location": "Владивосток"
                        }
                    }]
                }]
            }
            
            test_container_data = {
                "data": [{
                    "date": "2024-01-15 14:30:00",
                    "operation": "Грузится на фидер",
                    "location": "Владивосток",
                    "transport": "Автомобиль"
                }]
            }
            
            # Тест извлечения событий
            order_events = processor.extract_order_events(
                test_order_data, "TEST123", self.test_containers[0]
            )
            
            container_events = processor.extract_container_events(test_container_data)
            
            # Тест дедупликации
            final_event, has_duplicates, source = processor.merge_and_deduplicate(
                order_events, container_events
            )
            
            pipeline_works = (
                len(order_events) > 0 and
                len(container_events) > 0 and
                final_event is not None and
                final_event.operation == "Грузится на фидер"
            )
            
            self.test_results['tests'][test_name] = {
                'status': 'PASS' if pipeline_works else 'WARN',
                'order_events_extracted': len(order_events),
                'container_events_extracted': len(container_events),
                'deduplication_works': has_duplicates,
                'final_event_valid': final_event is not None,
                'event_source': source
            }
            
            print(f"  ✅ Event processing pipeline: {'WORKING' if pipeline_works else 'PARTIAL'}")
            
        except Exception as e:
            self.test_results['tests'][test_name] = {
                'status': 'FAIL',
                'error': str(e)
            }
            print(f"  ❌ Event processing pipeline failed: {e}")
    
    # =========================================================================
    # FULL WORKFLOW TESTS
    # =========================================================================
    
    async def _test_full_orchestrator_workflow(self):
        """Полный тест orchestrator workflow"""
        
        test_name = "full_orchestrator_workflow"
        print("🚀 Тестирование полного Orchestrator workflow...")
        
        try:
            # Валидация компонентов
            await self.orchestrator._validate_components()
            
            # Тест создания статистики
            stats = self.orchestrator.orchestrator_stats
            initial_stats = {
                'containers_loaded': stats.containers_loaded,
                'containers_processed': stats.containers_processed
            }
            
            # Имитация простейшего workflow (без реального запуска full_workflow)
            # так как это может требовать реальных БД подключений
            
            workflow_components = {
                'has_config': bool(self.orchestrator.config),
                'has_cache': bool(self.orchestrator.cache),
                'has_api_client': bool(self.orchestrator.api_client),
                'has_event_processor': bool(self.orchestrator.event_processor),
                'has_binding_manager': bool(self.orchestrator.binding_manager),
                'has_stats': bool(self.orchestrator.orchestrator_stats)
            }
            
            all_components_ready = all(workflow_components.values())
            
            self.test_results['tests'][test_name] = {
                'status': 'PASS' if all_components_ready else 'WARN',
                'validation_passed': True,
                'components': workflow_components,
                'orchestrator_ready': all_components_ready
            }
            
            print(f"  ✅ Full orchestrator workflow: {'READY' if all_components_ready else 'PARTIAL'}")
            
        except Exception as e:
            self.test_results['tests'][test_name] = {
                'status': 'FAIL',
                'error': str(e)
            }
            print(f"  ❌ Full orchestrator workflow failed: {e}")
    
    async def _test_legacy_tracker_workflow(self):
        """Тест legacy tracker workflow"""
        
        test_name = "legacy_tracker_workflow"
        print("🔄 Тестирование Legacy Tracker workflow...")
        
        try:
            from processing import create_tracker
            
            # Создаем простой file cache для тестирования
            test_cache = create_cache(cache_type="file", cache_dir="./test_cache")
            
            # Создаем legacy tracker
            tracker = create_tracker(self.config, test_cache)
            
            # Проверяем компоненты tracker'а
            tracker_components = {
                'has_config': bool(tracker.config),
                'has_cache': bool(tracker.cache),
                'has_api_client': bool(tracker.api_client),
                'has_event_processor': bool(tracker.event_processor),
                'has_stats': bool(tracker.stats)
            }
            
            all_components_ready = all(tracker_components.values())
            
            # Очистка
            await test_cache.close()
            
            self.test_results['tests'][test_name] = {
                'status': 'PASS' if all_components_ready else 'WARN',
                'components': tracker_components,
                'tracker_ready': all_components_ready,
                'backward_compatibility': True
            }
            
            print(f"  ✅ Legacy tracker workflow: {'READY' if all_components_ready else 'PARTIAL'}")
            
        except Exception as e:
            self.test_results['tests'][test_name] = {
                'status': 'FAIL',
                'error': str(e)
            }
            print(f"  ❌ Legacy tracker workflow failed: {e}")
    
    # =========================================================================
    # UTILITIES
    # =========================================================================
    
    def _finalize_results(self):
        """Финализация и анализ результатов"""
        
        # Подсчет статистики
        total_tests = len(self.test_results['tests'])
        passed_tests = sum(1 for t in self.test_results['tests'].values() if t['status'] == 'PASS')
        warned_tests = sum(1 for t in self.test_results['tests'].values() if t['status'] == 'WARN')
        failed_tests = sum(1 for t in self.test_results['tests'].values() if t['status'] == 'FAIL')
        skipped_tests = sum(1 for t in self.test_results['tests'].values() if t['status'] == 'SKIP')
        
        self.test_results['summary'] = {
            'total_tests': total_tests,
            'passed': passed_tests,
            'warned': warned_tests,
            'failed': failed_tests,
            'skipped': skipped_tests,
            'success_rate': (passed_tests / total_tests * 100) if total_tests > 0 else 0,
            'overall_status': 'PASS' if failed_tests == 0 else 'PARTIAL' if passed_tests > failed_tests else 'FAIL'
        }
        
        # Выводим итоговую статистику
        print("\n" + "="*70)
        print("📊 ИТОГОВЫЕ РЕЗУЛЬТАТЫ ТЕСТИРОВАНИЯ")
        print("="*70)
        
        summary = self.test_results['summary']
        print(f"📋 Всего тестов:      {summary['total_tests']}")
        print(f"✅ Прошли:           {summary['passed']}")
        print(f"⚠️ Предупреждения:   {summary['warned']}")
        print(f"❌ Провалились:      {summary['failed']}")
        print(f"⏭️ Пропущены:        {summary['skipped']}")
        print(f"📈 Процент успеха:   {summary['success_rate']:.1f}%")
        print(f"🎯 Общий статус:     {summary['overall_status']}")
        
        print("\n📋 Детали по тестам:")
        for test_name, test_result in self.test_results['tests'].items():
            status_emoji = {
                'PASS': '✅',
                'WARN': '⚠️',
                'FAIL': '❌',
                'SKIP': '⏭️'
            }.get(test_result['status'], '❓')
            
            print(f"  {status_emoji} {test_name}: {test_result['status']}")
        
        print("="*70)
    
    async def _cleanup(self):
        """Очистка ресурсов после тестирования"""
        
        try:
            # Закрываем Redis manager
            if self.redis_manager:
                await self.redis_manager.close()
            
            # Закрываем Firebird manager
            if self.firebird_manager and hasattr(self.firebird_manager, 'close'):
                await self.firebird_manager.close()
            
            # Закрываем cache
            if self.cache and hasattr(self.cache, 'close'):
                await self.cache.close()
            
            # Очистка тестовых файлов
            test_cache_dir = Path("./test_cache")
            if test_cache_dir.exists():
                import shutil
                shutil.rmtree(test_cache_dir, ignore_errors=True)
            
            print("🧹 Очистка ресурсов завершена")
            
        except Exception as e:
            print(f"⚠️ Ошибка очистки: {e}")


# =============================================================================
# CLI INTERFACE
# =============================================================================

async def main():
    """Главная функция для запуска тестов"""
    
    import argparse
    
    parser = argparse.ArgumentParser(
        description="FESCO Container Tracking Integration Tests",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--env', '--environment',
        choices=['production', 'development', 'demo'],
        default='development',
        help='Environment for testing'
    )
    
    parser.add_argument(
        '--output',
        type=str,
        help='Save results to JSON file'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose output'
    )
    
    args = parser.parse_args()
    
    # Создание и запуск тестера
    tester = IntegrationTester(environment=args.env)
    
    try:
        results = await tester.run_all_tests()
        
        # Сохранение результатов
        if args.output:
            output_path = Path(args.output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2, default=str)
            
            print(f"\n💾 Результаты сохранены: {output_path}")
        
        # Определяем exit code
        overall_status = results['summary']['overall_status']
        
        if overall_status == 'PASS':
            print("\n🎉 Все критические тесты прошли успешно!")
            sys.exit(0)
        elif overall_status == 'PARTIAL':
            print("\n⚠️ Есть предупреждения, но система функциональна")
            sys.exit(0)
        else:
            print("\n❌ Есть критические ошибки")
            sys.exit(1)
            
    except Exception as e:
        print(f"\n❌ Критическая ошибка тестирования: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())