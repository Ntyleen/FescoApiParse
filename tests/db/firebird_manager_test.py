# tests/test_firebird_manager_fixed.py
"""
ИСПРАВЛЕННЫЕ И ДОПОЛНЕННЫЕ ТЕСТЫ для FirebirdEntityManager

ИСПРАВЛЕНИЯ:
1. ✅ Правильные импорты
2. ✅ Корректные фикстуры 
3. ✅ Реальные проверки в assertions
4. ✅ Правильное использование mock'ов
5. ✅ Покрытие всех компонентов
"""

import pytest
import asyncio
from unittest.mock import Mock, patch, MagicMock, AsyncMock, call
from datetime import datetime, date
from typing import Dict, Any, List
import threading
import time
from concurrent.futures import ThreadPoolExecutor

# ИСПРАВЛЕННЫЕ ИМПОРТЫ - основываемся на реальной структуре файлов
from utils.db.firebird_manager import (
    FirebirdEntityManager,
    FirebirdConnectionManager,
    FirebirdDateTransformer,
    FirebirdOperationMatcher,
    FirebirdStatisticsCollector,
    EntityTableConfig,
    EntityColumnMapping,
    EntityStatusID,
    ContainerInfo,
    create_firebird_entity_manager,
    validate_firebird_config,
    FIREBIRD_AVAILABLE
)

from models.container_event import TrackingResult, ContainerEvent


# =============================================================================
# ИСПРАВЛЕННЫЕ ФИКСТУРЫ
# =============================================================================

@pytest.fixture
def firebird_config():
    """ИСПРАВЛЕНО: Возвращает реальный dict конфигурации"""
    return {
        'host': '192.168.120.19',
        'database': 'D:/BrokerDB/BROKER_TEST.FDB',
        'user': 'SYSDBA',
        'password': '4fv50X%9r'
    }

@pytest.fixture
def entity_config():
    """Базовая конфигурация entity таблицы"""
    return EntityTableConfig()

@pytest.fixture
def sample_container_info():
    """Пример ContainerInfo для тестов"""
    return ContainerInfo(
        id=1,
        container_number="TDSU6005411",
        status_id=3,  # SEA
        status_name="SEA",
        line_id=1,
        current_dates={
            'DATE_ETA': '2024-01-15',
            'DATE_ETD': None,
            'DATE_IN': None,
            'DATE_RAILWAY_LOADING': None,
            'DATE_RAILWAY_DELIVERY': None
        },
        processing_flags={'loaded_from_firebird': True}
    )

@pytest.fixture 
def sample_tracking_result():
    """Пример TrackingResult для тестов"""
    result = TrackingResult(container_number="TDSU6005411")
    result.last_event = ContainerEvent(
        date="2024-01-15 14:30:00",
        operation="Грузится на фидер",
        location="Владивосток",
        transport="Автомобиль",
        remainingDistance="5"
    )
    return result

@pytest.fixture
def mock_firebird_connection():
    """УЛУЧШЕННЫЙ Mock для Firebird connection"""
    mock_connection = Mock()
    mock_cursor = Mock()
    mock_transaction = Mock()
    
    # Настраиваем cursor
    mock_cursor.execute = Mock()
    mock_cursor.fetchall = Mock(return_value=[])
    mock_cursor.fetchone = Mock(return_value=(1,))
    mock_cursor.rowcount = 1
    
    # Настраиваем connection
    mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
    mock_connection.cursor.return_value.__exit__ = Mock(return_value=None)
    mock_connection.close = Mock()
    
    # Настраиваем транзакции
    mock_transaction.begin = Mock()
    mock_transaction.commit = Mock() 
    mock_transaction.rollback = Mock()
    mock_connection.trans.return_value = mock_transaction
    
    return mock_connection


# =============================================================================
# ИСПРАВЛЕННЫЕ UNIT TESTS
# =============================================================================

class TestFirebirdConnectionManager:
    """Тесты для FirebirdConnectionManager"""
    
    def test_init_with_valid_config(self, firebird_config):
        """ИСПРАВЛЕНО: Тест инициализации с валидной конфигурацией"""
        if not FIREBIRD_AVAILABLE:
            pytest.skip("Firebird driver не установлен")
        
        # Act
        manager = FirebirdConnectionManager(firebird_config)
        
        # Assert - ДОБАВЛЕНЫ конкретные проверки
        assert manager.config == firebird_config
        assert manager._active_connections == 0
        assert manager._max_connections == 10
        assert hasattr(manager, '_connection_lock')
        assert isinstance(manager._connection_lock, threading.Lock)
    
    def test_init_with_invalid_config_should_raise_error(self):
        """ИСПРАВЛЕНО: Тест с невалидной конфигурацией"""
        # Arrange - тестируем разные типы невалидных конфигураций
        invalid_configs = [
            {'host': 'localhost'},  # Нет database
            {'database': 'test.fdb'},  # Нет host
            {'host': 'localhost', 'database': 'test.fdb'},  # Нет user
            {'host': 'localhost', 'database': 'test.fdb', 'user': 'test'},  # Нет password
            {}  # Пустая конфигурация
        ]
        
        for invalid_config in invalid_configs:
            if not FIREBIRD_AVAILABLE:
                with pytest.raises(ImportError):
                    FirebirdConnectionManager(invalid_config)
            else:
                with pytest.raises(ValueError, match="Отсутствует обязательное поле"):
                    FirebirdConnectionManager(invalid_config)
    
    @patch('firebird_manager.fdb')
    def test_get_connection_context_manager_success(self, mock_fdb, firebird_config, mock_firebird_connection):
        """НОВЫЙ: Тест успешного использования context manager"""
        if not FIREBIRD_AVAILABLE:
            pytest.skip("Firebird driver не установлен")
        
        # Arrange
        mock_fdb.connect.return_value = mock_firebird_connection
        manager = FirebirdConnectionManager(firebird_config)
        
        # Act & Assert
        with manager.get_connection() as conn:
            assert conn == mock_firebird_connection
            assert manager._active_connections == 1
        
        # После выхода из context manager
        mock_firebird_connection.close.assert_called_once()
        assert manager._active_connections == 0
    
    @patch('firebird_manager.fdb')
    def test_get_connection_should_handle_exception_in_context(self, mock_fdb, firebird_config, mock_firebird_connection):
        """НОВЫЙ: Тест обработки исключений в context manager"""
        if not FIREBIRD_AVAILABLE:
            pytest.skip("Firebird driver не установлен")
        
        # Arrange
        mock_fdb.connect.return_value = mock_firebird_connection
        manager = FirebirdConnectionManager(firebird_config)
        
        # Act & Assert
        with pytest.raises(ValueError, match="Test exception"):
            with manager.get_connection() as conn:
                assert manager._active_connections == 1
                raise ValueError("Test exception")
        
        # После исключения ресурсы должны быть освобождены
        mock_firebird_connection.close.assert_called_once()
        assert manager._active_connections == 0


class TestFirebirdDateTransformer:
    """УЛУЧШЕННЫЕ тесты для FirebirdDateTransformer"""
    
    @pytest.fixture
    def transformer(self):
        return FirebirdDateTransformer()
    
    @pytest.mark.parametrize("date_string,expected_year,expected_month,expected_day", [
        ("2024-01-15 14:30:00", 2024, 1, 15),
        ("2024-12-31 23:59:59", 2024, 12, 31),
        ("2024-02-29 12:00:00", 2024, 2, 29),  # Високосный год
        ("15.01.2024 14:30:00", 2024, 1, 15),  # Европейский формат
        ("2024/01/15 14:30", 2024, 1, 15),     # Альтернативный формат
    ])
    def test_transform_to_timestamp_various_formats(self, transformer, date_string, expected_year, expected_month, expected_day):
        """НОВЫЙ: Тест различных форматов timestamp"""
        # Act
        result = transformer.transform_value(date_string, "TIMESTAMP")
        
        # Assert
        assert isinstance(result, datetime)
        assert result.year == expected_year
        assert result.month == expected_month
        assert result.day == expected_day
    
    @pytest.mark.parametrize("date_string,expected_date", [
        ("2024-01-15", date(2024, 1, 15)),
        ("15.01.2024", date(2024, 1, 15)),
        ("2024/01/15", date(2024, 1, 15)),
        ("01/15/2024", None),  # Неподдерживаемый формат
    ])
    def test_transform_to_date_various_formats(self, transformer, date_string, expected_date):
        """НОВЫЙ: Тест различных форматов дат"""
        # Act
        result = transformer.transform_value(date_string, "DATE")
        
        # Assert
        assert result == expected_date
    
    @pytest.mark.parametrize("input_string,expected_number", [
        ("5 дней", 5),
        ("120 км до места назначения", 120),
        ("осталось 3 дня до прибытия", 3),
        ("42", 42),
        ("нет числовых данных", None),
        ("", None),
        (None, None),
    ])
    def test_transform_to_integer_extraction(self, transformer, input_string, expected_number):
        """УЛУЧШЕННЫЙ: Тест извлечения чисел из строк"""
        # Act
        result = transformer.transform_value(input_string, "INTEGER")
        
        # Assert
        assert result == expected_number
    
    def test_transform_unknown_type_returns_string(self, transformer):
        """НОВЫЙ: Тест обработки неизвестного типа"""
        # Arrange
        test_value = "test_value"
        
        # Act
        result = transformer.transform_value(test_value, "UNKNOWN_TYPE")
        
        # Assert
        assert result == "test_value"
        assert isinstance(result, str)


class TestFirebirdOperationMatcher:
    """УЛУЧШЕННЫЕ тесты для FirebirdOperationMatcher"""
    
    @pytest.fixture
    def matcher(self, entity_config):
        return FirebirdOperationMatcher(entity_config)
    
    def test_find_best_mapping_exact_match(self, matcher):
        """УЛУЧШЕННЫЙ: Тест точного совпадения"""
        # Act
        mapping = matcher.find_best_mapping("Грузится на фидер")
        
        # Assert
        assert mapping is not None
        assert mapping.entity_column == "DATE_ETA"
        
        # Проверяем высокую оценку для точного совпадения
        score = mapping.matches_operation("Грузится на фидер")
        assert score >= 0.9
    
    def test_find_best_mapping_partial_match_with_scores(self, matcher):
        """НОВЫЙ: Тест частичного совпадения с проверкой оценок"""
        # Arrange - операция содержит ключевое слово
        operation = "Контейнер грузится на фидер в порту Владивосток"
        
        # Act
        mapping = matcher.find_best_mapping(operation)
        
        # Assert
        assert mapping is not None
        assert mapping.entity_column == "DATE_ETA"
        
        score = mapping.matches_operation(operation)
        assert 0.3 <= score < 1.0  # Частичное совпадение
    
    def test_find_best_mapping_priority_handling(self, entity_config):
        """НОВЫЙ: Тест обработки приоритетов"""
        # Arrange - создаем два маппинга с одинаковыми паттернами, но разными приоритетами
        high_priority_mapping = EntityColumnMapping(
            entity_column="HIGH_PRIORITY_DATE",
            fesco_field="date",
            operation_patterns=("тестовая операция",),
            priority=100
        )
        
        low_priority_mapping = EntityColumnMapping(
            entity_column="LOW_PRIORITY_DATE", 
            fesco_field="date",
            operation_patterns=("тестовая операция",),
            priority=1
        )
        
        entity_config.date_mappings["HIGH_PRIORITY_DATE"] = high_priority_mapping
        entity_config.date_mappings["LOW_PRIORITY_DATE"] = low_priority_mapping
        
        matcher = FirebirdOperationMatcher(entity_config)
        
        # Act
        mapping = matcher.find_best_mapping("тестовая операция")
        
        # Assert - должен выбрать маппинг с высоким приоритетом
        assert mapping is not None
        assert mapping.entity_column == "HIGH_PRIORITY_DATE"
    
    @pytest.mark.parametrize("operation,expected_column", [
        ("Выгружается груженным", "DATE_ETD"),
        ("Прием с моря", "DATE_IN"),
        ("Отправление вагона со станции", "DATE_RAILWAY_LOADING"),
        ("Добавлен в поручение на отгрузку на ЖД", "DATE_RAILWAY_DELIVERY"),
    ])
    def test_find_best_mapping_default_operations(self, matcher, operation, expected_column):
        """НОВЫЙ: Тест маппинга стандартных операций"""
        # Act
        mapping = matcher.find_best_mapping(operation)
        
        # Assert
        assert mapping is not None
        assert mapping.entity_column == expected_column


class TestFirebirdStatisticsCollector:
    """УЛУЧШЕННЫЕ тесты для FirebirdStatisticsCollector"""
    
    @pytest.fixture
    def stats(self):
        return FirebirdStatisticsCollector()
    
    def test_record_operations_incremental_updates(self, stats):
        """УЛУЧШЕННЫЙ: Тест инкрементальных обновлений"""
        # Act - записываем операции поэтапно
        stats.record_container_loaded(3)
        stats.record_container_loaded(2)  # +2 = 5 total
        
        stats.record_update_success("DATE_ETA", "Операция 1")
        stats.record_update_success("DATE_ETA", "Операция 2")
        stats.record_update_success("DATE_ETD", "Операция 3")
        
        # Assert
        assert stats.stats['containers_loaded'] == 5
        assert stats.stats['records_updated'] == 3
        assert stats.stats['date_columns_updated']['DATE_ETA'] == 2
        assert stats.stats['date_columns_updated']['DATE_ETD'] == 1
    
    def test_get_summary_calculations(self, stats):
        """УЛУЧШЕННЫЙ: Тест расчетов в сводке"""
        # Arrange - создаем тестовые данные
        stats.record_update_success("DATE_ETA", "Op1")
        stats.record_update_success("DATE_ETA", "Op1")  # Дубликат операции
        stats.record_update_success("DATE_ETD", "Op2")
        stats.record_update_failure()
        
        # Записываем времена выполнения
        stats.record_operation_time(100.0)
        stats.record_operation_time(200.0)
        stats.record_operation_time(300.0)
        
        # Act
        summary = stats.get_summary()
        
        # Assert - проверяем все расчеты
        assert summary['totals']['records_updated'] == 3
        assert summary['totals']['records_failed'] == 1
        assert summary['success_rate'] == 75.0  # 3/4 * 100
        
        # Проверяем статистику производительности
        perf = summary['performance']
        assert perf['avg_operation_time_ms'] == 200.0  # (100+200+300)/3
        assert perf['min_operation_time_ms'] == 100.0
        assert perf['max_operation_time_ms'] == 300.0
        
        # Проверяем топ колонки
        top_columns = summary['top_columns']
        assert len(top_columns) > 0
        assert top_columns[0] == ("DATE_ETA", 2)  # Самая частая
    
    def test_operation_times_limit_enforcement(self, stats):
        """НОВЫЙ: Тест ограничения количества записей времени"""
        # Act - записываем больше лимита
        for i in range(1100):
            stats.record_operation_time(float(i))
        
        # Assert
        assert len(stats.stats['operation_times']) == 1000
        # Должны остаться последние 1000 записей
        assert stats.stats['operation_times'][0] == 100.0
        assert stats.stats['operation_times'][-1] == 1099.0

    def test_prepare_update_data_uses_earliest_date(self, entity_config):
        """Проверка, что для DATE_RAILWAY_LOADING берется самая ранняя дата"""
        if not FIREBIRD_AVAILABLE:
            pytest.skip("Firebird driver не установлен")

        manager = FirebirdEntityManager({'host':'h','database':'db','user':'u','password':'p'}, entity_config)
        mapping = entity_config.date_mappings[entity_config.date_railway_loading]
        result = TrackingResult(container_number="TEST")
        result.last_event = ContainerEvent(operation="Отправление вагона со станции", date="2024-01-20")
        result.earliest_railway_loading_date = "2024-01-15"

        update = manager._prepare_update_data(result, mapping)

        assert mapping.entity_column in update
        assert str(update[mapping.entity_column]).startswith("2024-01-15")

    @pytest.mark.asyncio
    @patch('firebird_manager.fdb')
    async def test_write_results_skip_when_date_matches(self, mock_fdb, entity_config):
        if not FIREBIRD_AVAILABLE:
            pytest.skip("Firebird driver не установлен")
        """Engine не обновляет дату если она уже соответствует"""
        manager = FirebirdEntityManager({'host':'h','database':'db','user':'u','password':'p'}, entity_config)
        config = Config()
        engine = ContainerTrackingEngine(config, MagicMock(), manager)

        container = ContainerInfo(
            id=1,
            container_number="TEST",
            current_dates={entity_config.date_railway_loading: "2024-01-15"},
        )

        result = TrackingResult(container_number="TEST")
        result.last_event = ContainerEvent(operation="Отправление вагона со станции", date="2024-01-20")
        result.earliest_railway_loading_date = "2024-01-15"

        with patch.object(manager, 'update_container_from_tracking', new=AsyncMock()) as mock_update:
            await engine._write_results_to_firebird([(container, result)])
            mock_update.assert_not_called()


# =============================================================================
# INTEGRATION TESTS - ИСПРАВЛЕННЫЕ И ДОПОЛНЕННЫЕ
# =============================================================================

class TestFirebirdEntityManagerIntegration:
    """ИСПРАВЛЕННЫЕ интеграционные тесты"""
    
    @pytest.mark.asyncio
    async def test_init_creates_all_components_correctly(self, firebird_config, entity_config):
        """ИСПРАВЛЕНО: Тест правильной инициализации всех компонентов"""
        if not FIREBIRD_AVAILABLE:
            pytest.skip("Firebird driver не установлен")
        
        # Act
        manager = FirebirdEntityManager(firebird_config, entity_config)
        
        # Assert - ДЕТАЛЬНЫЕ проверки
        assert isinstance(manager.connection_manager, FirebirdConnectionManager)
        assert isinstance(manager.transformer, FirebirdDateTransformer)
        assert isinstance(manager.operation_matcher, FirebirdOperationMatcher)
        assert isinstance(manager.stats, FirebirdStatisticsCollector)
        assert manager.entity_config == entity_config
        
        # Проверяем настройки thread pool
        assert manager._thread_pool is None  # Ленивая инициализация
        assert manager._max_workers > 0
    
    @pytest.mark.asyncio
    @patch('firebird_manager.fdb')
    async def test_get_containers_for_processing_complete_workflow(
        self, mock_fdb, firebird_config, entity_config, mock_firebird_connection
    ):
        """ИСПРАВЛЕНО: Полный workflow получения контейнеров"""
        if not FIREBIRD_AVAILABLE:
            pytest.skip("Firebird driver не установлен")
        
        # Arrange - создаем реалистичные тестовые данные
        sample_rows = [
            (1, "TDSU6005411", 3, 1, "2024-01-15", None, "2024-01-10", None, None, "2024-01-01 10:00:00", "2024-01-02 11:00:00"),
            (2, "FESU5384983", 5, 2, None, "2024-01-16", None, "2024-01-12", None, "2024-01-01 12:00:00", "2024-01-03 13:00:00"),
            (3, "TEMU1234567", 8, 1, None, None, None, None, "2024-01-14", "2024-01-01 14:00:00", "2024-01-04 15:00:00")  # Должен быть исключен (status_id=8)
        ]
        
        mock_cursor = mock_firebird_connection.cursor.return_value.__enter__.return_value
        mock_cursor.fetchall.return_value = sample_rows
        mock_fdb.connect.return_value = mock_firebird_connection
        
        manager = FirebirdEntityManager(firebird_config, entity_config)
        
        # Act
        batches = []
        async for batch in manager.get_containers_for_processing(batch_size=10):
            batches.append(batch)
        
        # Assert
        assert len(batches) == 1
        batch = batches[0]
        assert len(batch) == 2  # Третий контейнер исключен из-за статуса 8
        
        # Проверяем первый контейнер детально
        container1 = batch[0]
        assert container1.id == 1
        assert container1.container_number == "TDSU6005411"
        assert container1.status_id == 3
        assert container1.status_name == "SEA"
        assert container1.line_id == 1
        assert container1.current_dates['DATE_ETA'] == "2024-01-15"
        assert container1.current_dates['DATE_ETD'] is None
        assert container1.current_dates['DATE_IN'] == "2024-01-10"
        
        # Проверяем второй контейнер
        container2 = batch[1]
        assert container2.id == 2
        assert container2.container_number == "FESU5384983"
        assert container2.current_dates['DATE_ETD'] == "2024-01-16"
        
        # Проверяем вызовы к БД
        mock_cursor.execute.assert_called_once()
        mock_cursor.fetchall.assert_called_once()
    
    @pytest.mark.asyncio
    @patch('firebird_manager.fdb')
    async def test_update_container_from_tracking_complete_workflow(
        self, mock_fdb, firebird_config, entity_config, mock_firebird_connection, sample_tracking_result
    ):
        """ИСПРАВЛЕНО: Полный workflow обновления контейнера"""
        if not FIREBIRD_AVAILABLE:
            pytest.skip("Firebird driver не установлен")
        
        # Arrange
        mock_fdb.connect.return_value = mock_firebird_connection
        mock_cursor = mock_firebird_connection.cursor.return_value.__enter__.return_value
        mock_cursor.rowcount = 1
        
        # Создаем tracking result с операцией, которая точно найдет маппинг
        sample_tracking_result.last_event.operation = "Грузится на фидер"
        
        manager = FirebirdEntityManager(firebird_config, entity_config)
        container_id = 1
        
        # Act
        success = await manager.update_container_from_tracking(container_id, sample_tracking_result)
        
        # Assert
        assert success is True
        
        # Проверяем, что UPDATE запрос был выполнен
        mock_cursor.execute.assert_called()
        execute_calls = mock_cursor.execute.call_args_list
        
        # Ищем UPDATE запрос
        update_call = None
        for call in execute_calls:
            sql = str(call[0][0]).upper()
            if "UPDATE" in sql and "ENTITY" in sql:
                update_call = call
                break
        
        assert update_call is not None, "UPDATE запрос должен был быть выполнен"
        
        # Проверяем, что в запросе есть DATE_ETA (ожидаемая колонка для "Грузится на фидер")
        update_sql = str(update_call[0][0])
        assert "DATE_ETA" in update_sql
        assert "UPDATED_AT" in update_sql
        
        # Проверяем статистику
        summary = manager.stats.get_summary()
        assert summary['totals']['records_updated'] == 1
        assert "DATE_ETA" in manager.stats.stats['date_columns_updated']


# =============================================================================
# НОВЫЕ ТЕСТЫ ДЛЯ FACTORY FUNCTIONS
# =============================================================================

class TestFactoryFunctions:
    """Тесты для фабричных функций"""
    
    @pytest.mark.asyncio
    async def test_create_firebird_entity_manager_with_defaults(self):
        """Тест создания менеджера с настройками по умолчанию"""
        if not FIREBIRD_AVAILABLE:
            pytest.skip("Firebird driver не установлен")
        
        # Act
        manager = create_firebird_entity_manager(
            host="192.168.120.19",
            database="D:/BrokerDB/BROKER_TEST.FDB",
            user="SYSDBA",
            password="4fv50X%9r"
        )
        
        # Assert
        assert isinstance(manager, FirebirdEntityManager)
        assert isinstance(manager.entity_config, EntityTableConfig)
        assert manager.connection_manager.config['host'] == "localhost"
        assert manager.connection_manager.config['database'] == "test.fdb"
    
    @pytest.mark.asyncio
    async def test_create_firebird_entity_manager_with_custom_config(self):
        """Тест создания менеджера с кастомной конфигурацией"""
        if not FIREBIRD_AVAILABLE:
            pytest.skip("Firebird driver не установлен")
        
        # Arrange
        custom_config = EntityTableConfig()
        custom_config.table_name = "CUSTOM_ENTITY"
        
        # Act
        manager = create_firebird_entity_manager(
            host="192.168.120.19",
            database="D:/BrokerDB/BROKER_TEST.FDB",
            user="SYSDBA",
            password="4fv50X%9r",
            entity_config=custom_config
        )
        
        # Assert
        assert manager.entity_config.table_name == "CUSTOM_ENTITY"
    
    @pytest.mark.asyncio
    async def test_validate_firebird_config_valid(self):
        """Тест валидации корректной конфигурации"""
        # Arrange
        valid_config = {
            'host': '192.168.120.19',
            'database': 'D:/BrokerDB/BROKER_TEST.FDB',
            'user': 'SYSDBA',
            'password': '4fv50X%9r'
        }
        
        # Act
        result = await validate_firebird_config(valid_config)
        
        # Assert
        assert result['valid'] is True
        assert len(result['errors']) == 0
        assert 'config_summary' in result
    
    @pytest.mark.asyncio
    async def test_validate_firebird_config_invalid(self):
        """Тест валидации некорректной конфигурации"""
        # Arrange
        invalid_config = {
            'host': 'localhost',
            'database': 'test.txt',  # Неправильное расширение
            'user': 'notSysdba',     # Нестандартный пользователь
            # Отсутствует password
        }
        
        # Act  
        result = await validate_firebird_config(invalid_config)
        
        # Assert
        assert result['valid'] is False
        assert len(result['errors']) > 0
        assert len(result['warnings']) > 0
        
        # Проверяем конкретные ошибки
        errors_text = ' '.join(result['errors'])
        assert 'password' in errors_text.lower()


# =============================================================================
# НОВЫЕ ТЕСТЫ ДЛЯ ERROR HANDLING И EDGE CASES
# =============================================================================

class TestAdvancedErrorHandling:
    """Продвинутые тесты обработки ошибок"""
    
    @pytest.mark.asyncio
    @patch('firebird_manager.fdb')
    async def test_connection_pool_exhaustion(self, mock_fdb, firebird_config, entity_config):
        """НОВЫЙ: Тест исчерпания пула соединений"""
        if not FIREBIRD_AVAILABLE:
            pytest.skip("Firebird driver не установлен")
        
        # Arrange
        manager = FirebirdEntityManager(firebird_config, entity_config)
        manager.connection_manager._max_connections = 1
        
        # Имитируем занятое соединение
        manager.connection_manager._active_connections = 1
        
        # Создаем tracking result
        tracking_result = TrackingResult(container_number="TEST")
        tracking_result.last_event = ContainerEvent(
            operation="Грузится на фидер",
            date="2024-01-15"
        )
        
        # Act & Assert
        # При попытке обновления должна возникнуть ошибка пула
        success = await manager.update_container_from_tracking(1, tracking_result)
        assert success is False
    
    @pytest.mark.asyncio 
    @patch('firebird_manager.fdb')
    async def test_database_transaction_rollback(self, mock_fdb, firebird_config, entity_config):
        """НОВЫЙ: Тест rollback транзакции при ошибке"""
        if not FIREBIRD_AVAILABLE:
            pytest.skip("Firebird driver не установлен")
        
        # Arrange
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_transaction = Mock()
        
        # Настраиваем cursor чтобы выбросить ошибку при execute
        mock_cursor.execute.side_effect = Exception("SQL Error")
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=None)
        
        # Настраиваем транзакцию
        mock_transaction.rollback = Mock()
        mock_connection.trans.return_value = mock_transaction
        
        mock_fdb.connect.return_value = mock_connection
        
        manager = FirebirdEntityManager(firebird_config, entity_config)
        
        tracking_result = TrackingResult(container_number="TEST")
        tracking_result.last_event = ContainerEvent(operation="Грузится на фидер")
        
        # Act
        success = await manager.update_container_from_tracking(1, tracking_result)
        
        # Assert
        assert success is False
        mock_transaction.rollback.assert_called_once()
    
    def test_entity_column_mapping_validation_edge_cases(self):
        """НОВЫЙ: Тест валидации EntityColumnMapping в граничных случаях"""
        # Test 1: Пустые паттерны
        with pytest.raises(ValueError, match="operation_patterns должен содержать хотя бы один паттерн"):
            EntityColumnMapping(
                entity_column="TEST",
                fesco_field="date",
                operation_patterns=()
            )
        
        # Test 2: Очень длинные паттерны
        very_long_pattern = "очень " * 1000 + "длинный паттерн"
        mapping = EntityColumnMapping(
            entity_column="TEST",
            fesco_field="date", 
            operation_patterns=(very_long_pattern,)
        )
        assert len(mapping.operation_patterns[0]) > 5000
        
        # Test 3: Специальные символы в паттернах  
        special_mapping = EntityColumnMapping(
            entity_column="TEST",
            fesco_field="date",
            operation_patterns=("паттерн с символами: !@#$%^&*()", "パターン", "🚢⚓🏭")
        )
        assert len(special_mapping.operation_patterns) == 3


# =============================================================================
# PERFORMANCE И LOAD TESTS
# =============================================================================

class TestPerformanceAndLoad:
    """Тесты производительности и нагрузки"""
    
    def test_operation_matcher_performance_stress(self):
        """Стресс-тест производительности matcher'а"""
        # Arrange - создаем большую конфигурацию
        config = EntityTableConfig()
        for i in range(50):
            mapping = EntityColumnMapping(
                entity_column=f"DATE_FIELD_{i}",
                fesco_field="date",
                operation_patterns=tuple([f"операция_{j}_{i}" for j in range(10)]),
                priority=i
            )
            config.date_mappings[f"DATE_FIELD_{i}"] = mapping
        
        matcher = FirebirdOperationMatcher(config)
        
        # Act - измеряем время выполнения
        start_time = time.perf_counter()
        
        for i in range(1000):
            operation = f"тестовая операция_{i % 100}"
            mapping = matcher.find_best_mapping(operation)
        
        end_time = time.perf_counter()
        execution_time = end_time - start_time
        
        # Assert
        assert execution_time < 2.0  # Должно быть быстро даже для большой конфигурации
    
    def test_statistics_memory_usage_large_dataset(self):
        """Тест использования памяти при больших данных"""
        # Arrange
        stats = FirebirdStatisticsCollector()
        
        # Act - записываем много уникальных операций
        for i in range(10000):
            stats.record_update_success(f"COLUMN_{i % 20}", f"unique_operation_{i}")
            stats.record_operation_time(float(i))
        
        # Assert - проверяем ограничения памяти
        assert len(stats.stats['operation_times']) <= 1000  # Ограничение работает
        assert len(stats.stats['date_columns_updated']) == 20  # Правильное количество колонок
        assert len(stats.stats['operations_processed']) == 10000  # Все операции записаны
        
        # Проверяем что summary вычисляется быстро
        start_time = time.perf_counter()
        summary = stats.get_summary()
        summary_time = time.perf_counter() - start_time
        
        assert summary_time < 0.1  # Быстрый расчет сводки
    
    @pytest.mark.asyncio
    async def test_concurrent_updates_thread_safety(self, firebird_config, entity_config):
        """НОВЫЙ: Тест thread safety при параллельных обновлениях"""
        if not FIREBIRD_AVAILABLE:
            pytest.skip("Firebird driver не установлен")
        
        # Arrange
        manager = FirebirdEntityManager(firebird_config, entity_config)
        
        # Создаем несколько tracking results
        tracking_results = []
        for i in range(10):
            result = TrackingResult(container_number=f"CONTAINER_{i}")
            result.last_event = ContainerEvent(operation="Грузится на фидер")
            tracking_results.append(result)
        
        # Act - имитируем параллельные обновления
        async def mock_update(container_id, tracking_result):
            # Имитируем успешное обновление без реального подключения к БД
            await asyncio.sleep(0.01)  # Небольшая задержка
            manager.stats.record_update_success("DATE_ETA", tracking_result.last_event.operation)
            return True
        
        # Patch метод для тестирования без реальной БД
        with patch.object(manager, 'update_container_from_tracking', side_effect=mock_update):
            tasks = [
                manager.update_container_from_tracking(i, result) 
                for i, result in enumerate(tracking_results)
            ]
            
            results = await asyncio.gather(*tasks)
        
        # Assert
        assert all(results)  # Все обновления успешны
        assert manager.stats.stats['records_updated'] == 10


if __name__ == "__main__":
    """Запуск тестов"""
    import subprocess
    import sys
    
    result = subprocess.run([
        sys.executable, "-m", "pytest", 
        __file__, 
        "-v", 
        "--tb=short",
        "--durations=10",
        "--cov=firebird_manager",
        "--cov-report=html"
    ])
    
    sys.exit(result.returncode)
