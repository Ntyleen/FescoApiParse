import json
import hashlib
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any

from cache.cache_base import CacheBackend


class FileCache(CacheBackend):
    """Файловый кэш с TTL"""
    
    def __init__(self, cache_dir: Path, ttl_hours: int = 1):
        self.cache_dir = cache_dir
        self.ttl = timedelta(hours=ttl_hours)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        logging.info(f"📁 FileCache: {cache_dir}, TTL: {ttl_hours}h")
    
    def _get_cache_path(self, key: str) -> Path:
        """Получить путь к файлу кэша"""
        hash_key = hashlib.md5(key.encode()).hexdigest()
        return self.cache_dir / f"{hash_key}.json"
    
    def _is_fresh(self, cache_path: Path) -> bool:
        """Проверить актуальность кэша"""
        if not cache_path.exists():
            return False
            
        try:
            mtime = datetime.fromtimestamp(cache_path.stat().st_mtime)
            return datetime.now() - mtime < self.ttl
        except OSError:
            return False
    
    async def get(self, key: str) -> Dict[str, Any] | None:
        """Получить данные из кэша"""
        cache_path = self._get_cache_path(key)
        
        if not self._is_fresh(cache_path):
            # Удаляем устаревший файл
            cache_path.unlink(missing_ok=True)
            return None
        
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                logging.debug(f"💾 Cache HIT: {key}")
                return data
        except (json.JSONDecodeError, FileNotFoundError, OSError) as e:
            logging.warning(f"Cache read error for {key}: {e}")
            cache_path.unlink(missing_ok=True)
            return None
    
    async def set(self, key: str, data: Dict[str, Any], ttl_seconds: int = 3600) -> None:
        """Сохранить данные в кэш"""
        cache_path = self._get_cache_path(key)
        
        try:
            # Атомарная запись через временный файл
            temp_path = cache_path.with_suffix('.tmp')
            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            temp_path.replace(cache_path)
            logging.debug(f"💾 Cache SET: {key}")
            
        except Exception as e:
            logging.error(f"Cache write error for {key}: {e}")
            # Удаляем поврежденный временный файл
            temp_path.unlink(missing_ok=True)
    
    async def delete(self, key: str) -> bool:
        """Удалить данные из кэша"""
        cache_path = self._get_cache_path(key)
        try:
            cache_path.unlink()
            logging.debug(f"💾 Cache DELETE: {key}")
            return True
        except FileNotFoundError:
            return False
    
    async def clear(self) -> None:
        """Очистить весь кэш"""
        try:
            deleted_count = 0
            for cache_file in self.cache_dir.glob("*.json"):
                cache_file.unlink()
                deleted_count += 1
            
            logging.info(f"💾 Cache cleared: {deleted_count} files")
        except Exception as e:
            logging.error(f"Cache clear error: {e}")
    
    async def exists(self, key: str) -> bool:
        """Проверить существование ключа"""
        cache_path = self._get_cache_path(key)
        return self._is_fresh(cache_path)
    
    async def close(self) -> None:
        """Закрыть кэш (для файлового кэша не требуется)"""
        pass