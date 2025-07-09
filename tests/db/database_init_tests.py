# tests/test_database_init.py
"""
Comprehensive Test Suite для database/__init__.py

ФОКУС ТЕСТИРОВАНИЯ:
===================

1. Factory Functions Testing - тестируем фабричные функции
2. Auto-detection Logic - тестируем автоопределение типа БД
3. Configuration Validation - тестируем валидацию конфигураций
4. Integration Points - тестируем точки интеграции
5. Error Handling - тестируем обработку ошибок

АРХИТЕКТУРНЫЕ ПРИНЦИПЫ ТЕСТИРОВАНИЯ:
====================================
✅ Factory Pattern Testing: каждая фабрика тестируется с разными входными данными
✅ Detection Algorithm Testing: все ветки логики автоопределения покрыты
✅ Configuration Matrix Testing: тестируем все комбинации конфигураций
✅ Boundary Value Testing: граничные случаи для всех параметров
✅ Error Recovery Testing: что происходит при неожиданных ошибках
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, Any

# Импортируем наш рефакторенный модуль
from utils.db.database_init import (
    # Factory functions
    create_database_source,
    create_database_writer,
    create_unified_config,
    
    # Utility functions
    detect_database_type,
    get_database_capabilities,
    validate_database_config,
    test_database_connections,
    
    # Firebird components
    create_firebird_entity_manager,
    FirebirdEntityManager,
    EntityTableConfig,
    
    # Generic components
    DatabaseContainerSource,
    ExternalDatabaseWriter,
    
    # Predefined configs
    FIREBIRD_CONFIGS,
    MYSQL_CONFIGS,
    POSTGRESQL_CONFIGS
)


# =============================================================================
# ФИКСТУРЫ ДЛЯ ТЕСТИРОВАНИЯ РАЗНЫХ ТИПОВ БД
# =============================================================================

@pytest.fixture
def firebird_config():
    """Firebird конфигурация для тестов"""
    return {
        'host': 'localhost',
        'database': 'C:/test_shipping.fdb',
        'user': 'SYSDBA',
        'password': 'testpass'
    }

@pytest.fixture
def mysql_config():
    """MySQL конфигурация для тестов"""
    return {
        'host': 'localhost',
        'port': 3306,
        'database': 'test_shipping',
        'user': 'mysql_user',
        'password': 'mysql_pass'
    }

@pytest.fixture
def postgresql_config():
    """PostgreSQL конфигурация для тестов"""
    return {
        'host': 'localhost',
        'port': 5432,
        'database': 'test_shipping',
        'user': 'postgres',
        'password': 'postgres_pass'
    }

@pytest.fixture
def ambiguous_config():
    """Конфигурация без явных признаков типа БД"""
    return {
        'host': 'db.company.com',
        'database': 'shipping',
        'user': 'app_user',
        'password': 'secret123'
    }


# =============================================================================
# ТЕСТЫ АВТООПРЕДЕЛЕНИЯ ТИПА БД
# =============================================================================

class TestDatabaseTypeDetection:
    """
    Тесты алгоритма автоопределения типа БД
    
    Критически важно: от правильности определения зависит выбор компонента!
    """
    
    def test_detect_firebird_by_file_extension(self):
        """Тест: должен определить Firebird по расширению .fdb"""
        # Arrange
        config = {'database': 'C:/shipping.fdb'}
        
        # Act
        db_type = detect_database_type(config)
        
        # Assert
        assert db_type == 'firebird'
    
    def test_detect_firebird_by_port(self):
        """Тест: должен определить Firebird по порту 3050"""
        # Arrange
        config = {'port': 3050, 'database': 'shipping'}
        
        # Act
        db_type = detect_database_type(config)
        
        # Assert
        assert db_type == 'firebird'
    
    def test_detect_firebird_by_user(self):
        """Тест: должен определить Firebird по пользователю SYSDBA"""
        # Arrange
        config = {'user': 'SYSDBA', 'database': 'shipping'}
        
        # Act
        db_type = detect_database_type(config)
        
        # Assert
        assert db_type == 'firebird'
    
    def test_detect_mysql_by_port(self):
        """Тест: должен определить MySQL по порту 3306"""
        # Arrange
        config = {'port': 3306, 'database': 'shipping'}
        
        # Act
        db_type = detect_database_type(config)
        
        # Assert
        assert db_type == 'mysql'
    
    def test_detect_postgresql_by_port(self):
        """Тест: должен определить PostgreSQL по порту 5432"""
        # Arrange
        config = {'port': 5432, 'database': 'shipping'}
        
        # Act
        db_type = detect_database_type(config)
        
        # Assert
        assert db_type == 'postgresql'
    
    def test_detect_explicit_type_should_override_heuristics(self):
        """Тест: явно указанный тип должен переопределять эвристики"""
        # Arrange
        config = {
            'type': 'mysql',
            'database': 'shipping.fdb',  # .fdb намекает на Firebird
            'port': 3050,                # порт Firebird
            'user': 'SYSDBA'            # пользователь Firebird
        }
        
        # Act
        db_type = detect_database_type(config)
        
        # Assert
        assert db_type == 'mysql'  # Явное указание побеждает
    
    def test_detect_fallback_to_mysql_for_ambiguous_config(self, ambiguous_config):
        """Тест: должен fallback на MySQL для неопределенных конфигураций"""
        # Act
        db_type = detect_database_type(ambiguous_config)
        
        # Assert
        assert db_type == 'mysql'  # Fallback по умолчанию
    
    def test_detect_case_insensitive_file_extension(self):
        """Тест: определение расширения должно быть регистронезависимым"""
        # Arrange
        test_cases = [
            'C:/shipping.FDB',
            'C:/shipping.fdb',
            'shipping.Fdb',
            'SHIPPING.FDB'
        ]
        
        # Act & Assert
        for database_path in test_cases:
            config = {'database': database_path}
            db_type = detect_database_type(config)
            assert db_type == 'firebird', f"Не удалось определить Firebird для {database_path}"
    
    def test_detect_priority_order_is_correct(self):
        """Тест: порядок приоритета в алгоритме определения"""
        # Arrange - конфигурация с несколькими признаками
        config = {
            'database': 'shipping.fdb',  # Firebird признак (высокий приоритет)
            'port': 3306,                # MySQL признак (низкий приоритет)
            'user': 'mysql_user'         # Нейтральный признак
        }
        
        # Act
        db_type = detect_database_type(config)
        
        # Assert
        assert db_type == 'firebird'  # .fdb должен победить порт


# =============================================================================
# ТЕСТЫ ФАБРИЧНЫХ ФУНКЦИЙ
# =============================================================================

class TestFactoryFunctions:
    """
    Тесты фабричных функций
    
    Фокус: правильное создание объектов, передача параметров, error handling
    """
    
    @patch('database.create_firebird_entity_manager')
    def test_create_database_source_with_firebird_config_should_create_firebird_manager(
        self, mock_create_firebird, firebird_config
    ):
        """Тест: фабрика должна создать Firebird менеджер для Firebird конфигурации"""
        # Arrange
        mock_manager = Mock(spec=FirebirdEntityManager)
        mock_create_firebird.return_value = mock_manager
        
        # Act
        source = create_database_source(**firebird_config)
        
        # Assert
        assert source == mock_manager
        mock_create_firebird.assert_called_once_with(
            host=firebird_config['host'],
            database=firebird_config['database'],
            user=firebird_config['user'],
            password=firebird_config['password'],
            entity_config=None
        )
    
    @patch('database.DatabaseContainerSource')
    def test_create_database_source_with_mysql_config_should_create_generic_source(
        self, mock_generic_source, mysql_config
    ):
        """Тест: фабрика должна создать generic источник для MySQL"""
        # Arrange
        mock_source_instance = Mock(spec=DatabaseContainerSource)
        mock_generic_source.return_value = mock_source_instance
        
        # Act
        source = create_database_source(db_type="mysql", **mysql_config)
        
        # Assert
        assert source == mock_source_instance
        mock_generic_source.assert_called_once()
        
        # Проверяем переданную конфигурацию
        call_args = mock_generic_source.call_args[0][0]
        assert call_args['host'] == mysql_config['host']
        assert call_args['port'] == mysql_config['port']
        assert call_args['database'] == mysql_config['database']
    
    def test_create_database_source_with_auto_detection(self, firebird_config):
        """Тест: автоопределение типа должно работать в фабрике"""
        # Act & Assert - должно автоматически определить Firebird и не выбросить ошибку
        with patch('database.create_firebird_entity_manager') as mock_create:
            mock_create.return_value = Mock()
            source = create_database_source(db_type="auto", **firebird_config)
            assert source is not None
            mock_create.assert_called_once()
    
    def test_create_database_source_with_unsupported_type_should_raise_error(self):
        """Тест: неподдерживаемый тип БД должен выбрасывать ошибку"""
        # Arrange
        config = {'host': 'localhost', 'database': 'test'}
        
        # Act & Assert
        with pytest.raises(ValueError, match="Неподдерживаемый тип БД: unsupported"):
            create_database_source(db_type="unsupported", **config)
    
    @patch('database.ExternalDatabaseWriter')
    def test_create_database_writer_with_mysql_should_create_external_writer(
        self, mock_external_writer, mysql_config
    ):
        """Тест: фабрика writer'а должна создать ExternalDatabaseWriter для MySQL"""
        # Arrange
        mock_writer_instance = Mock(spec=ExternalDatabaseWriter)
        mock_external_writer.return_value = mock_writer_instance
        
        # Act
        writer = create_database_writer(db_type="mysql", **mysql_config)
        
        # Assert
        assert writer == mock_writer_instance
        mock_external_writer.assert_called_once()
    
    @patch('database.create_firebird_entity_manager')
    def test_create_database_writer_with_firebird_should_reuse_entity_manager(
        self, mock_create_firebird, firebird_config
    ):
        """Тест: для Firebird writer должен использовать тот же EntityManager"""
        # Arrange
        mock_manager = Mock(spec=FirebirdEntityManager)
        mock_create_firebird.return_value = mock_manager
        
        # Act
        writer = create_database_writer(db_type="firebird", **firebird_config)
        
        # Assert
        assert writer == mock_manager  # Тот же объект!
        mock_create_firebird.assert_called_once()


class TestUnifiedConfigCreation:
    """
    Тесты создания унифицированных конфигураций
    
    Фокус: объединение конфигураций, валидация структуры, defaults
    """
    
    def test_create_unified_config_with_firebird_source_only(self, firebird_config):
        """Тест: создание конфигурации только с Firebird источником"""
        # Act
        config = create_unified_config(firebird_config)
        
        # Assert
        assert config['version'] == '2.0'
        assert config['source']['type'] == 'firebird'
        assert config['source']['config'] == firebird_config
        assert 'target' not in config  # Нет целевой БД
        assert 'capabilities' in config
        assert config['capabilities']['unified_manager'] is True
    
    def test_create_unified_config_with_source_and_target(self, firebird_config, mysql_config):
        """Тест: создание конфигурации с источником и целью"""
        # Act
        config = create_unified_config(firebird_config, mysql_config, {'batch_size': 100})
        
        # Assert
        assert config['source']['type'] == 'firebird'
        assert config['target']['type'] == 'mysql'
        assert config['processing']['batch_size'] == 100
        assert 'capabilities' in config
    
    def test_create_unified_config_should_detect_types_automatically(self):
        """Тест: автоопределение типов в унифицированной конфигурации"""
        # Arrange
        source_config = {'database': 'shipping.fdb', 'user': 'SYSDBA'}
        target_config = {'port': 3306, 'database': 'external_db'}
        
        # Act
        config = create_unified_config(source_config, target_config)
        
        # Assert
        assert config['source']['type'] == 'firebird'
        assert config['target']['type'] == 'mysql'
    
    def test_create_unified_config_with_explicit_types_should_override_detection(self):
        """Тест: явные типы должны переопределять автоопределение"""
        # Arrange
        source_config = {
            'type': 'mysql',           # Явно указываем MySQL
            'database': 'shipping.fdb'  # Но расширение намекает на Firebird
        }
        
        # Act
        config = create_unified_config(source_config)
        
        # Assert
        assert config['source']['type'] == 'mysql'  # Явный тип побеждает


# =============================================================================
# ТЕСТЫ ВАЛИДАЦИИ КОНФИГУРАЦИЙ
# =============================================================================

class TestConfigurationValidation:
    """
    Тесты валидации конфигураций
    
    Фокус: проверка корректности, выявление ошибок, рекомендации
    """
    
    @pytest.mark.asyncio
    async def test_validate_database_config_with_valid_firebird_config_should_pass(self, firebird_config):
        """Тест: валидная Firebird конфигурация должна проходить проверку"""
        # Act
        result = await validate_database_config(firebird_config)
        
        # Assert
        assert result['valid'] is True
        assert result['detected_type'] == 'firebird'
        assert len(result['errors']) == 0
        assert 'capabilities' in result
        assert result['capabilities']['unified_manager'] is True
    
    @pytest.mark.asyncio
    async def test_validate_database_config_with_missing_fields_should_fail(self):
        """Тест: конфигурация с отсутствующими полями должна не проходить валидацию"""
        # Arrange
        incomplete_config = {'host': 'localhost'}  # Отсутствуют user, password, database
        
        # Act
        result = await validate_database_config(incomplete_config)
        
        # Assert
        assert result['valid'] is False
        assert len(result['errors']) >= 3  # Минимум 3 ошибки (user, password, database)
        
        error_text = ' '.join(result['errors'])
        assert 'user' in error_text
        assert 'password' in error_text
        assert 'database' in error_text
    
    @pytest.mark.asyncio
    async def test_validate_database_config_should_provide_recommendations(self, mysql_config):
        """Тест: валидация должна предоставлять рекомендации"""
        # Act
        result = await validate_database_config(mysql_config)
        
        # Assert
        assert 'recommendations' in result
        assert len(result['recommendations']) > 0
        
        # Для MySQL должны быть рекомендации по производительности
        recommendations_text = ' '.join(result['recommendations'])
        assert any(keyword in recommendations_text.lower() 
                  for keyword in ['connection', 'pool', 'performance', 'monitoring'])
    
    @pytest.mark.asyncio
    async def test_validate_database_config_should_detect_warnings(self):
        """Тест: валидация должна выявлять предупреждения"""
        # Arrange
        config_with_warnings = {
            'host': 'localhost',
            'port': 9999,  # Нестандартный порт
            'user': 'user',
            'password': 'pass',
            'database': 'test'
        }
        
        # Act
        result = await validate_database_config(config_with_warnings)
        
        # Assert
        assert len(result['warnings']) > 0
        warnings_text = ' '.join(result['warnings'])
        assert 'порт' in warnings_text.lower()


class TestConnectionTesting:
    """
    Тесты тестирования подключений
    
    Фокус: проверка доступности БД, error handling, recommendations
    """
    
    @pytest.mark.asyncio
    @patch('database.firebird_manager.FirebirdConnectionManager')
    async def test_test_database_connections_with_firebird_success(
        self, mock_connection_manager_class, firebird_config
    ):
        """Тест: успешное тестирование Firebird подключения"""
        # Arrange
        mock_manager = Mock()
        mock_manager.test_connection = AsyncMock(return_value=True)
        mock_connection_manager_class.return_value = mock_manager
        
        # Act
        result = await test_database_connections(firebird_config)
        
        # Assert
        assert result['detected_type'] == 'firebird'
        assert result['connection_test']['success'] is True
        assert 'enterprise_ready' in str(result['capabilities_test'])
        assert any('enterprise' in rec.lower() for rec in result['recommendations'])
    
    @pytest.mark.asyncio
    @patch('database.firebird_manager.FirebirdConnectionManager')
    async def test_test_database_connections_with_firebird_failure(
        self, mock_connection_manager_class, firebird_config
    ):
        """Тест: неудачное тестирование Firebird подключения"""
        # Arrange
        mock_manager = Mock()
        mock_manager.test_connection = AsyncMock(side_effect=Exception("Connection failed"))
        mock_connection_manager_class.return_value = mock_manager
        
        # Act
        result = await test_database_connections(firebird_config)
        
        # Assert
        assert result['connection_test']['success'] is False
        assert 'Connection failed' in result['connection_test']['error']
        assert any('ошибка' in rec.lower() for rec in result['recommendations'])
    
    @pytest.mark.asyncio
    async def test_test_database_connections_with_unsupported_db_type(self, mysql_config):
        """Тест: тестирование неподдерживаемого типа БД"""
        # Act
        result = await test_database_connections(mysql_config)
        
        # Assert
        assert result['detected_type'] == 'mysql'
        assert result['connection_test']['success'] is False
        assert 'не реализовано' in result['connection_test']['error']
        assert any('реализуйте' in rec.lower() for rec in result['recommendations'])


# =============================================================================
# ТЕСТЫ CAPABILITIES И МЕТАДАННЫХ
# =============================================================================

class TestDatabaseCapabilities:
    """
    Тесты определения возможностей БД
    
    Фокус: корректность метаданных, полнота информации
    """
    
    def test_get_database_capabilities_for_firebird_should_include_enterprise_features(self):
        """Тест: возможности Firebird должны включать enterprise функции"""
        # Act
        capabilities = get_database_capabilities('firebird')
        
        # Assert
        assert capabilities['unified_manager'] is True
        assert capabilities['read_write_same_db'] is True
        assert capabilities['transaction_support'] is True
        assert capabilities['enterprise_features'] is True
        assert 'DATE' in capabilities['date_types']
        assert 'TIMESTAMP' in capabilities['date_types']
        assert 'corporate_integrations' in capabilities['recommended_for']
    
    def test_get_database_capabilities_for_mysql_should_include_web_features(self):
        """Тест: возможности MySQL должны включать веб-функции"""
        # Act
        capabilities = get_database_capabilities('mysql')
        
        # Assert
        assert capabilities['unified_manager'] is False
        assert capabilities['read_write_same_db'] is False
        assert capabilities['concurrent_connections'] == 'high'
        assert 'web_applications' in capabilities['recommended_for']
        assert 'microservices' in capabilities['recommended_for']
    
    def test_get_database_capabilities_for_postgresql_should_include_analytics_features(self):
        """Тест: возможности PostgreSQL должны включать аналитические функции"""
        # Act
        capabilities = get_database_capabilities('postgresql')
        
        # Assert
        assert 'analytics' in capabilities['recommended_for']
        assert 'complex_queries' in capabilities['recommended_for']
        assert 'TIMESTAMPTZ' in capabilities['date_types']
    
    def test_get_database_capabilities_for_unknown_type_should_return_empty_dict(self):
        """Тест: неизвестный тип БД должен возвращать пустой словарь"""
        # Act
        capabilities = get_database_capabilities('unknown_db')
        
        # Assert
        assert capabilities == {}


# =============================================================================
# ТЕСТЫ ПРЕДОПРЕДЕЛЕННЫХ КОНФИГУРАЦИЙ
# =============================================================================

class TestPredefinedConfigurations:
    """
    Тесты предопределенных конфигураций
    
    Фокус: корректность шаблонов, покрытие основных сценариев
    """
    
    def test_firebird_configs_should_contain_development_and_production_templates(self):
        """Тест: Firebird конфигурации должны содержать шаблоны для разработки и продакшена"""
        # Assert
        assert 'local_development' in FIREBIRD_CONFIGS
        assert 'production_template' in FIREBIRD_CONFIGS
        
        # Проверяем development конфигурацию
        dev_config = FIREBIRD_CONFIGS['local_development']
        assert dev_config['host'] == 'localhost'
        assert dev_config['user'] == 'SYSDBA'
        assert dev_config['database'].endswith('.fdb')
        
        # Проверяем production шаблон
        prod_config = FIREBIRD_CONFIGS['production_template']
        assert '${FIREBIRD_HOST}' in prod_config['host']
        assert '${FIREBIRD_USER}' in prod_config['user']
    
    def test_mysql_configs_should_contain_development_template(self):
        """Тест: MySQL конфигурации должны содержать шаблон для разработки"""
        # Assert
        assert 'local_development' in MYSQL_CONFIGS
        
        dev_config = MYSQL_CONFIGS['local_development']
        assert dev_config['port'] == 3306
        assert 'table' in dev_config
        assert 'container_column' in dev_config
    
    def test_postgresql_configs_should_contain_development_template(self):
        """Тест: PostgreSQL конфигурации должны содержать шаблон для разработки"""
        # Assert
        assert 'local_development' in POSTGRESQL_CONFIGS
        
        dev_config = POSTGRESQL_CONFIGS['local_development']
        assert dev_config['port'] == 5432
        assert dev_config['user'] == 'postgres'


# =============================================================================
# ИНТЕГРАЦИОННЫЕ ТЕСТЫ
# =============================================================================

class TestFullIntegrationScenarios:
    """
    Полные интеграционные сценарии
    
    Фокус: работа всей системы от начала до конца
    """
    
    @pytest.mark.asyncio
    @patch('database.create_firebird_entity_manager')
    async def test_full_firebird_workflow_should_work_end_to_end(
        self, mock_create_firebird, firebird_config
    ):
        """Тест: полный workflow с Firebird должен работать от начала до конца"""
        # Arrange
        mock_manager = Mock(spec=FirebirdEntityManager)
        mock_manager.test_connection = AsyncMock(return_value=True)
        mock_create_firebird.return_value = mock_manager
        
        # Act - воспроизводим полный сценарий использования
        
        # 1. Создаем источник данных
        source = create_database_source(db_type="auto", **firebird_config)
        
        # 2. Создаем writer (тот же объект для Firebird)
        writer = create_database_writer(db_type="auto", **firebird_config)
        
        # 3. Создаем унифицированную конфигурацию
        unified_config = create_unified_config(firebird_config)
        
        # 4. Валидируем конфигурацию
        validation_result = await validate_database_config(firebird_config)
        
        # 5. Тестируем подключение
        connection_test = await test_database_connections(firebird_config)
        
        # Assert - проверяем, что все компоненты работают согласованно
        assert source == writer  # Для Firebird это один объект
        assert unified_config['source']['type'] == 'firebird'
        assert validation_result['valid'] is True
        assert connection_test['detected_type'] == 'firebird'
        
        # Проверяем вызовы
        mock_create_firebird.assert_called()
        mock_manager.test_connection.assert_called()
    
    @patch('database.DatabaseContainerSource')
    @patch('database.ExternalDatabaseWriter')
    def test_full_mysql_workflow_should_create_separate_components(
        self, mock_external_writer, mock_container_source, mysql_config
    ):
        """Тест: полный workflow с MySQL должен создавать отдельные компоненты"""
        # Arrange
        mock_source_instance = Mock()
        mock_writer_instance = Mock()
        mock_container_source.return_value = mock_source_instance
        mock_external_writer.return_value = mock_writer_instance
        
        # Act
        source = create_database_source(db_type="mysql", **mysql_config)
        writer = create_database_writer(db_type="mysql", **mysql_config)
        
        # Assert
        assert source != writer  # Для MySQL это разные объекты
        assert source == mock_source_instance
        assert writer == mock_writer_instance
        
        mock_container_source.assert_called_once()
        mock_external_writer.assert_called_once()
    
    def test_configuration_consistency_across_functions(self, firebird_config):
        """Тест: консистентность конфигураций между функциями"""
        # Act - вызываем разные функции с одной конфигурацией
        detected_type_1 = detect_database_type(firebird_config)
        capabilities = get_database_capabilities(detected_type_1)
        unified_config = create_unified_config(firebird_config)
        detected_type_2 = unified_config['source']['type']
        
        # Assert - все должны согласованно определить тип
        assert detected_type_1 == detected_type_2 == 'firebird'
        assert capabilities['unified_manager'] is True
        assert unified_config['capabilities']['unified_manager'] is True


# =============================================================================
# PERFORMANCE И EDGE CASES
# =============================================================================

class TestPerformanceAndEdgeCases:
    """
    Тесты производительности и граничных случаев
    
    Фокус: масштабируемость, нестандартные входные данные
    """
    
    def test_detect_database_type_performance_with_large_config(self):
        """Тест: производительность определения типа с большой конфигурацией"""
        # Arrange - создаем большую конфигурацию
        large_config = {f'param_{i}': f'value_{i}' for i in range(1000)}
        large_config.update({'database': 'shipping.fdb'})
        
        # Act - измеряем время
        import time
        start_time = time.perf_counter()
        
        for _ in range(100):
            db_type = detect_database_type(large_config)
        
        end_time = time.perf_counter()
        execution_time = end_time - start_time
        
        # Assert
        assert db_type == 'firebird'
        assert execution_time < 0.1  # Должно быть быстро
    
    def test_create_unified_config_with_complex_nested_configuration(self):
        """Тест: создание унифицированной конфигурации со сложной структурой"""
        # Arrange
        complex_source = {
            'type': 'firebird',
            'connection': {
                'host': 'localhost',
                'database': 'shipping.fdb'
            },
            'options': {
                'charset': 'UTF8',
                'page_size': 8192
            }
        }
        
        complex_processing = {
            'batch_processing': {
                'size': 100,
                'parallel_workers': 4
            },
            'error_handling': {
                'max_retries': 3,
                'timeout_seconds': 30
            }
        }
        
        # Act
        unified_config = create_unified_config(complex_source, None, complex_processing)
        
        # Assert
        assert unified_config['source']['type'] == 'firebird'
        assert unified_config['processing']['batch_processing']['size'] == 100
        assert 'error_handling' in unified_config['processing']
    
    def test_factory_functions_with_none_values_should_handle_gracefully(self):
        """Тест: фабричные функции должны gracefully обрабатывать None значения"""
        # Arrange
        config_with_nones = {
            'host': 'localhost',
            'database': None,
            'user': 'test',
            'password': None
        }
        
        # Act & Assert - не должно падать, должно возвращать ошибку валидации
        with pytest.raises((ValueError, TypeError)):
            create_database_source(**config_with_nones)
    
    def test_get_database_capabilities_with_mixed_case_db_type(self):
        """Тест: получение capabilities с разным регистром"""
        # Arrange
        test_cases = ['Firebird', 'FIREBIRD', 'firebird', 'FireBird']
        
        # Act & Assert
        for db_type in test_cases:
            capabilities = get_database_capabilities(db_type.lower())
            # Должно работать только с нижним регистром
            if db_type.lower() == 'firebird':
                assert capabilities['unified_manager'] is True
            else:
                # Для остальных вариантов (с неправильным регистром) - пустой результат
                assert capabilities == {}


# =============================================================================
# MOCK SCENARIOS ДЛЯ СЛОЖНЫХ СЛУЧАЕВ
# =============================================================================

class TestComplexMockScenarios:
    """
    Сложные сценарии с моками
    
    Фокус: имитация редких и сложных ситуаций
    """
    
    @patch('database.FIREBIRD_AVAILABLE', False)
    def test_factory_with_firebird_unavailable_should_handle_gracefully(self, firebird_config):
        """Тест: фабрика должна обрабатывать недоступность Firebird драйвера"""
        # Act & Assert
        with pytest.raises(ImportError):
            create_database_source(db_type="firebird", **firebird_config)
    
    @patch('database.detect_database_type')
    def test_create_database_source_should_handle_detection_errors(
        self, mock_detect, firebird_config
    ):
        """Тест: фабрика должна обрабатывать ошибки автоопределения"""
        # Arrange
        mock_detect.side_effect = Exception("Detection failed")
        
        # Act & Assert
        with pytest.raises(Exception, match="Detection failed"):
            create_database_source(db_type="auto", **firebird_config)
    
    def test_predefined_configs_structure_validation(self):
        """Тест: валидация структуры предопределенных конфигураций"""
        # Act & Assert - проверяем, что все предопределенные конфигурации валидны
        
        # Firebird configs
        for config_name, config in FIREBIRD_CONFIGS.items():
            assert 'host' in config, f"Firebird config '{config_name}' missing host"
            assert 'database' in config, f"Firebird config '{config_name}' missing database"
            assert 'user' in config, f"Firebird config '{config_name}' missing user"
            assert 'description' in config, f"Firebird config '{config_name}' missing description"
        
        # MySQL configs
        for config_name, config in MYSQL_CONFIGS.items():
            assert 'port' in config, f"MySQL config '{config_name}' missing port"
            assert config['port'] == 3306, f"MySQL config '{config_name}' wrong default port"
        
        # PostgreSQL configs
        for config_name, config in POSTGRESQL_CONFIGS.items():
            assert 'port' in config, f"PostgreSQL config '{config_name}' missing port"
            assert config['port'] == 5432, f"PostgreSQL config '{config_name}' wrong default port"


# =============================================================================
# RUNNER И UTILITIES
# =============================================================================

def test_module_metadata():
    """Тест: проверка метаданных модуля"""
    import database
    
    assert hasattr(database, '__version__')
    assert hasattr(database, '__description__')
    assert database.__version__ == "2.0.0"
    assert "Firebird-first" in database.__description__


if __name__ == "__main__":
    """
    Запуск тестов для database/__init__.py
    
    Команды для запуска:
    python -m pytest tests/test_database_init.py -v
    python -m pytest tests/test_database_init.py -k "test_factory" -v
    python -m pytest tests/test_database_init.py -m "integration" -v
    python -m pytest tests/test_database_init.py --cov=database
    """
    
    import subprocess
    import sys
    
    result = subprocess.run([
        sys.executable, "-m", "pytest", 
        __file__, 
        "-v", 
        "--tb=short",
        "--durations=10",
        "--cov=database"
    ])
    
    sys.exit(result.returncode)
