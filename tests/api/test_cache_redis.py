from __future__ import annotations

import builtins
import sys
import types
from unittest import mock

import pytest

from aurum.api.config import CacheConfig
from aurum.api import service


def test_maybe_redis_client_standalone(monkeypatch):
    fake_client = mock.Mock()
    fake_client.ping.return_value = True

    class FakeRedis:
        @classmethod
        def from_url(cls, url, db=None, **kwargs):
            fake_client.url = url
            fake_client.kwargs = kwargs
            fake_client.db = db
            return fake_client

    redis_module = types.SimpleNamespace(Redis=FakeRedis)
    monkeypatch.setitem(sys.modules, "redis", redis_module)

    cfg = CacheConfig(redis_url="redis://localhost:6379/0", username="user", password="pass", db=2)
    client = service._maybe_redis_client(cfg)

    assert client is fake_client
    fake_client.ping.assert_called_once()
    assert fake_client.kwargs["username"] == "user"
    assert fake_client.kwargs["password"] == "pass"
    assert fake_client.kwargs["retry_on_timeout"] is True
    assert fake_client.db == 2


def test_maybe_redis_client_sentinel(monkeypatch):
    fake_client = mock.Mock()
    fake_client.ping.return_value = True

    sentinel_instance = mock.Mock()
    sentinel_instance.master_for.return_value = fake_client
    SentinelMock = mock.Mock(return_value=sentinel_instance)

    monkeypatch.setitem(sys.modules, "redis", types.SimpleNamespace())
    monkeypatch.setitem(sys.modules, "redis.sentinel", types.SimpleNamespace(Sentinel=SentinelMock))

    cfg = CacheConfig(
        mode="sentinel",
        sentinel_endpoints=(("redis-sentinel", 26379),),
        sentinel_master="mymaster",
        username="svc",
        password="secret",
        db=5,
    )

    client = service._maybe_redis_client(cfg)

    assert client is fake_client
    SentinelMock.assert_called_once()
    sentinel_args, sentinel_kwargs = SentinelMock.call_args
    assert sentinel_args[0] == (("redis-sentinel", 26379),)
    assert sentinel_kwargs["username"] == "svc"
    assert sentinel_kwargs["password"] == "secret"
    sentinel_instance.master_for.assert_called_once_with(
        "mymaster", db=5, username="svc", password="secret", socket_connect_timeout=mock.ANY,
        socket_keepalive=True, socket_timeout=mock.ANY, retry_on_timeout=True
    )
    fake_client.ping.assert_called_once()


def test_maybe_redis_client_cluster(monkeypatch):
    fake_client = mock.Mock()
    fake_client.ping.return_value = True
    cluster_mock = mock.Mock(return_value=fake_client)

    monkeypatch.setitem(sys.modules, "redis", types.SimpleNamespace())
    monkeypatch.setitem(sys.modules, "redis.cluster", types.SimpleNamespace(RedisCluster=cluster_mock))

    cfg = CacheConfig(
        mode="cluster",
        cluster_nodes=("cache-primary:7000", "cache-secondary"),
        username="svc",
        password="secret",
    )

    client = service._maybe_redis_client(cfg)

    assert client is fake_client
    args, kwargs = cluster_mock.call_args
    assert kwargs["username"] == "svc"
    assert kwargs["password"] == "secret"
    assert kwargs["retry_on_timeout"] is True
    assert {"host": "cache-primary", "port": 7000} in kwargs["startup_nodes"]
    assert {"host": "cache-secondary", "port": 6379} in kwargs["startup_nodes"]


def test_cluster_support_missing(monkeypatch, caplog):
    class BrokenModule:
        def __getattr__(self, name):
            raise RuntimeError("cluster disabled")

    monkeypatch.setitem(sys.modules, "redis", types.SimpleNamespace())
    monkeypatch.setitem(sys.modules, "redis.cluster", BrokenModule())

    cfg = CacheConfig(mode="cluster", cluster_nodes=("cache:6379",))
    with caplog.at_level("WARNING"):
        client = service._maybe_redis_client(cfg)

    assert client is None
    assert "cluster mode requested" in caplog.text


def test_ping_failure_logs_warning(monkeypatch, caplog):
    fake_client = mock.Mock()
    fake_client.ping.side_effect = RuntimeError("boom")

    class FakeRedis:
        @classmethod
        def from_url(cls, url, db=None, **kwargs):
            return fake_client

    monkeypatch.setitem(sys.modules, "redis", types.SimpleNamespace(Redis=FakeRedis))
    cfg = CacheConfig(redis_url="redis://localhost:6379/0")

    with caplog.at_level("WARNING"):
        client = service._maybe_redis_client(cfg)

    assert client is None
    assert "Redis client initialization failed" in caplog.text


def test_missing_redis_module(monkeypatch):
    original_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "redis":
            raise ModuleNotFoundError
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    cfg = CacheConfig(redis_url="redis://localhost:6379/0")

    assert service._maybe_redis_client(cfg) is None
