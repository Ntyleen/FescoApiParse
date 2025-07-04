# database/container_source.py
"""
Модуль для получения списка контейнеров из базы данных
Реализует первый шаг схемы: "Получаем номер контейнера из базы данных"
"""

import asyncio
import aiomysql
from typing import List, Optional, AsyncGenerator
from dataclasses import dataclass
from utils.logging import get_logger


@dataclass
class ContainerInfo:
    """Информация о контейнере из БД"""
    container_number: str
    id: Optional[int] = None
    status: Optional[str] = None
    priority: int = 0
    created_at: Optional[str] = None


class DatabaseContainerSource:
    """
    Источник контейнеров из базы данных
    
    Основные функции:
    - Подключение к существующей БД приложения
    - Получение списка контейнеров для обработки
    - Пакетная загрузка для оптимизации памяти
    - Фильтрация по статусу/приоритету
    """
    
    def __init__(self, db_config: dict):
        """
        Args:
            db_config: Конфигурация БД вида:
            {
                'host': 'localhost',
                'port': 3306,
                'user': 'username',
                'password': 'password',
                'database': 'your_app_db',
                'table': 'containers_table',
                'column': 'container_number_column'
            }
        """
        self.db_config = db_config
        self.connection_pool = None
        self.logger = get_logger("fesco_tracker.db_source")
        
        # Настройки по умолчанию
        self.table_name = db_config.get('table', 'containers')
        self.container_column = db_config.get('column', 'container_number')
        self.status_column = db_config.get('status_column', 'status')
        self.priority_column = db_config.get('priority_column', 'priority')
        
        self.logger.debug(f"📂 DatabaseContainerSource настроен для таблицы {self.table_name}")
    
    async def connect(self) -> None:
        """Создание пула подключений к БД"""
        try:
            self.connection_pool = await aiomysql.create_pool(
                host=self.db_config['host'],
                port=self.db_config.get('port', 3306),
                user=self.db_config['user'],
                password=self.db_config['password'],
                db=self.db_config['database'],
                charset='utf8mb4',
                autocommit=True,
                minsize=1,
                maxsize=5  # Небольшой пул для чтения
            )
            
            # Проверяем подключение
            async with self.connection_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("SELECT 1")
                    await cursor.fetchone()
            
            self.logger.info(f"🗄️ Подключение к БД установлено: {self.db_config['host']}/{self.db_config['database']}")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка подключения к БД: {e}")
            raise
    
    async def get_containers_batch(
        self, 
        batch_size: int = 100,
        status_filter: Optional[str] = None,
        min_priority: int = 0
    ) -> AsyncGenerator[List[ContainerInfo], None]:
        """
        Получение контейнеров пакетами для оптимизации памяти
        
        Args:
            batch_size: Размер пакета
            status_filter: Фильтр по статусу (например, 'pending', 'new')
            min_priority: Минимальный приоритет
            
        Yields:
            Список объектов ContainerInfo
        """
        
        if not self.connection_pool:
            await self.connect()
        
        # Строим SQL запрос
        base_query = f"""
        SELECT id, {self.container_column}
        {f', {self.status_column}' if self._column_exists(self.status_column) else ', NULL as status'}
        {f', {self.priority_column}' if self._column_exists(self.priority_column) else ', 0 as priority'}
        , created_at
        FROM {self.table_name}
        """
        
        # Добавляем условия фильтрации
        conditions = []
        params = []
        
        if status_filter and self._column_exists(self.status_column):
            conditions.append(f"{self.status_column} = %s")
            params.append(status_filter)
        
        if min_priority > 0 and self._column_exists(self.priority_column):
            conditions.append(f"{self.priority_column} >= %s")
            params.append(min_priority)
        
        # Добавляем условия к запросу
        if conditions:
            base_query += " WHERE " + " AND ".join(conditions)
        
        # Сортировка по приоритету (если колонка существует) и дате
        if self._column_exists(self.priority_column):
            base_query += f" ORDER BY {self.priority_column} DESC, created_at ASC"
        else:
            base_query += " ORDER BY created_at ASC"
        
        self.logger.info(f"📊 Запрос контейнеров: batch_size={batch_size}, status={status_filter}")
        self.logger.debug(f"🔍 SQL: {base_query}")
        
        try:
            async with self.connection_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(base_query, params)
                    
                    batch = []
                    total_loaded = 0
                    
                    while True:
                        rows = await cursor.fetchmany(batch_size)
                        if not rows:
                            # Отдаем последний неполный батч если он есть
                            if batch:
                                self.logger.debug(f"📦 Последний батч: {len(batch)} контейнеров")
                                yield batch
                            break
                        
                        # Преобразуем строки в объекты ContainerInfo
                        for row in rows:
                            container_info = ContainerInfo(
                                id=row[0],
                                container_number=row[1],
                                status=row[2],
                                priority=row[3] or 0,
                                created_at=str(row[4]) if row[4] else None
                            )
                            batch.append(container_info)
                        
                        total_loaded += len(rows)
                        self.logger.debug(f"📦 Загружен батч: {len(rows)} контейнеров (всего: {total_loaded})")
                        
                        # Отдаем полный батч
                        yield batch
                        batch = []
                    
                    self.logger.info(f"✅ Загрузка завершена: {total_loaded} контейнеров")
                    
        except Exception as e:
            self.logger.error(f"❌ Ошибка получения контейнеров: {e}")
            raise
    
    async def get_containers_list(
        self, 
        limit: int = 1000,
        status_filter: Optional[str] = None
    ) -> List[ContainerInfo]:
        """
        Получение списка контейнеров (простой метод для небольших объемов)
        
        Args:
            limit: Максимальное количество контейнеров
            status_filter: Фильтр по статусу
            
        Returns:
            Список объектов ContainerInfo
        """
        
        containers = []
        
        async for batch in self.get_containers_batch(
            batch_size=min(limit, 500), 
            status_filter=status_filter
        ):
            containers.extend(batch)
            if len(containers) >= limit:
                break
        
        # Обрезаем до лимита
        return containers[:limit]
    
    async def get_container_count(self, status_filter: Optional[str] = None) -> int:
        """
        Получение количества контейнеров в БД
        
        Args:
            status_filter: Фильтр по статусу
            
        Returns:
            Количество контейнеров
        """
        
        if not self.connection_pool:
            await self.connect()
        
        query = f"SELECT COUNT(*) FROM {self.table_name}"
        params = []
        
        if status_filter and self._column_exists(self.status_column):
            query += f" WHERE {self.status_column} = %s"
            params.append(status_filter)
        
        try:
            async with self.connection_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(query, params)
                    row = await cursor.fetchone()
                    count = row[0] if row else 0
                    
                    self.logger.debug(f"📊 Количество контейнеров в БД: {count}")
                    return count
                    
        except Exception as e:
            self.logger.error(f"❌ Ошибка подсчета контейнеров: {e}")
            return 0
    
    def _column_exists(self, column_name: str) -> bool:
        """
        Простая проверка существования колонки
        В реальном приложении лучше кэшировать результат
        """
        # Для упрощения считаем, что стандартные колонки существуют
        standard_columns = ['status', 'priority', 'created_at', 'updated_at']
        return column_name in standard_columns
    
    async def close(self) -> None:
        """Закрытие пула подключений"""
        if self.connection_pool:
            self.connection_pool.close()
            await self.connection_pool.wait_closed()
            self.logger.info("🔒 Подключения к БД закрыты")


# Вспомогательная функция для создания источника из конфигурации
def create_database_source(config) -> DatabaseContainerSource:
    """
    Фабричная функция для создания источника контейнеров
    
    Args:
        config: Объект конфигурации с настройками БД
        
    Returns:
        Настроенный DatabaseContainerSource
    """
    
    # Извлекаем настройки БД из конфигурации
    db_config = {
        'host': getattr(config, 'db_host', 'localhost'),
        'port': getattr(config, 'db_port', 3306),
        'user': getattr(config, 'db_user', 'root'),
        'password': getattr(config, 'db_password', ''),
        'database': getattr(config, 'db_name', 'your_app'),
        'table': getattr(config, 'containers_table', 'containers'),
        'column': getattr(config, 'container_column', 'container_number')
    }
    
    return DatabaseContainerSource(db_config)


# Пример использования:
# ```python
# # Создание источника
# db_source = DatabaseContainerSource({
#     'host': 'localhost',
#     'user': 'username', 
#     'password': 'password',
#     'database': 'shipment_db',
#     'table': 'shipment_containers',
#     'column': 'container_no'
# })
# 
# await db_source.connect()
# 
# # Получение пакетами
# async for batch in db_source.get_containers_batch(batch_size=50):
#     for container in batch:
#         print(f"Контейнер: {container.container_number}")
# 
# await db_source.close()
# ```