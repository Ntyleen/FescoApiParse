from contextlib import contextmanager
import threading
from typing import Optional

from utils.logging import get_logger

try:
    import firebird.driver as fdb
    FIREBIRD_AVAILABLE = True
except ImportError:  # pragma: no cover - optional dependency
    FIREBIRD_AVAILABLE = False
    fdb = None


class FirebirdConnectionManager:
    def __init__(self, firebird_config: dict):
        if not FIREBIRD_AVAILABLE:
            raise ImportError(
                "❌ Firebird драйвер не установлен!\n"
                "Установите: pip install fdb\n"
                "или: pip install firebird-driver"
            )

        self.config = firebird_config
        self.logger = get_logger("firebird.connection")
        self._validate_config()
        self._connection_lock = threading.Lock()
        self._active_connections = 0
        self._max_connections = 10

    def _validate_config(self) -> None:
        required_fields = ['host', 'database', 'user', 'password']
        for field in required_fields:
            if not self.config.get(field):
                raise ValueError(f"Отсутствует обязательное поле: {field}")

    @contextmanager
    def get_connection(self):
        connection = None
        with self._connection_lock:
            if self._active_connections >= self._max_connections:
                raise RuntimeError(
                    f"Превышен лимит соединений: {self._max_connections}"
                )
            self._active_connections += 1

        try:
            connection = self._create_connection()
            yield connection
        except Exception as e:
            self.logger.error(f"❌ Ошибка работы с подключением: {e}")
            raise
        finally:
            if connection:
                try:
                    connection.close()
                except Exception as close_error:  # pragma: no cover - log only
                    self.logger.error(f"Ошибка закрытия соединения: {close_error}")

            with self._connection_lock:
                self._active_connections -= 1

    def _create_connection(self):
        try:
            if 'dsn' in self.config:
                dsn = self.config['dsn']
            else:
                host = self.config['host']
                database = self.config['database']
                dsn = f"{host}:{database}"

            connection = fdb.connect(
                database=self.config['database'],
                user=self.config['user'],
                password=self.config['password'],
                charset='UTF8',
            )
            return connection
        except Exception as e:
            self.logger.error(f"❌ Ошибка создания Firebird подключения: {e}")
            raise

    async def test_connection(self) -> bool:
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1 FROM RDB$DATABASE")
                result = cursor.fetchone()
                return result is not None
        except Exception as e:
            self.logger.error(f"❌ Ошибка тестирования подключения: {e}")
            return False
