import os
import tempfile
from pathlib import Path

import pytest

from config import load_config, ConfigError


def create_temp_yaml(content: str) -> str:
    fd, path = tempfile.mkstemp(suffix=".yml")
    os.close(fd)
    Path(path).write_text(content, encoding="utf-8")
    return path


def test_load_config_env_substitution(monkeypatch):
    monkeypatch.setenv("FESCO_TOKEN", "token123")
    monkeypatch.setenv("API_HOST", "api.test.local")
    monkeypatch.setenv("REDIS_HOST", "redis.test.local")

    yaml_content = """
api:
  base_url: "https://${API_HOST}/v1/"
BrokerDatabase:
  host: localhost
  port: 3050
  user: SYSDBA
  password: pass
  database: /tmp/test.fdb
cache:
  type: redis
  redis:
    url: "redis://${REDIS_HOST}:6379"
"""
    path = create_temp_yaml(yaml_content)

    config = load_config(environment="test", config_files=[path])

    assert config.api.base_url == "https://api.test.local/v1/"
    assert config.cache.redis.url == "redis://redis.test.local:6379"


def test_load_config_invalid_timeout(monkeypatch):
    monkeypatch.setenv("FESCO_TOKEN", "token123")
    yaml_content = """
api:
  base_url: "https://example.com/"
  timeout_seconds: 0
BrokerDatabase:
  host: localhost
  port: 3050
  user: SYSDBA
  password: pass
  database: /tmp/test.fdb
"""
    path = create_temp_yaml(yaml_content)

    with pytest.raises(ConfigError, match="timeout_seconds"):
        load_config(environment="test", config_files=[path])


def test_load_config_invalid_port(monkeypatch):
    monkeypatch.setenv("FESCO_TOKEN", "token123")
    yaml_content = """
api:
  base_url: "https://example.com/"
BrokerDatabase:
  host: localhost
  port: 70000
  user: SYSDBA
  password: pass
  database: /tmp/test.fdb
"""
    path = create_temp_yaml(yaml_content)

    with pytest.raises(ConfigError, match="port"):
        load_config(environment="test", config_files=[path])
