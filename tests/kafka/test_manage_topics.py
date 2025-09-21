from __future__ import annotations

from types import SimpleNamespace

import scripts.kafka.manage_topics as manage_topics


class _EnumStub:
    def __getitem__(self, key: str) -> SimpleNamespace:
        return SimpleNamespace(name=key)


class _BindingStub:
    def __init__(self, resource_type, resource_name, pattern, principal, host, operation, permission):
        self.resource_type = resource_type
        self.resource_name = resource_name
        self.pattern = pattern
        self.principal = principal
        self.host = host
        self.operation = operation
        self.permission = permission


class _NewTopicStub:
    def __init__(self, topic, num_partitions, replication_factor, config=None):
        self.topic = topic
        self.num_partitions = num_partitions
        self.replication_factor = replication_factor
        self.config = config or {}


class _ConfigResourceStub:
    def __init__(self, resource_type, name, set_config):
        self.resource_type = resource_type
        self.name = name
        self.set_config = set_config


class _AdminStub:
    def __init__(self, existing: set[str] | None = None):
        self._existing = existing or set()
        self.created: list[_NewTopicStub] = []
        self.updated: list[_ConfigResourceStub] = []
        self.acls = []

    def list_topics(self, timeout: float):  # noqa: ARG002 - parity with AdminClient
        return SimpleNamespace(topics={name: object() for name in self._existing})

    def create_topics(self, topics, request_timeout: float):  # noqa: ARG002
        self.created.extend(topics)
        return {topic.topic: SimpleNamespace(result=lambda: None) for topic in topics}

    def alter_configs(self, resources, request_timeout: float):  # noqa: ARG002
        self.updated.extend(resources)
        return {resource: SimpleNamespace(result=lambda: None) for resource in resources}

    def create_acls(self, bindings, request_timeout: float):  # noqa: ARG002
        self.acls.extend(bindings)
        return {binding: SimpleNamespace(result=lambda: None) for binding in bindings}


def _patch_confluent(monkeypatch) -> None:
    monkeypatch.setattr(manage_topics, "_CONFLUENT_AVAILABLE", True)
    monkeypatch.setattr(manage_topics, "AclOperation", _EnumStub())
    monkeypatch.setattr(manage_topics, "AclPermissionType", _EnumStub())
    monkeypatch.setattr(manage_topics, "ResourcePatternType", _EnumStub())
    monkeypatch.setattr(manage_topics, "ResourceType", SimpleNamespace(TOPIC="TOPIC"))
    monkeypatch.setattr(manage_topics, "AclBinding", _BindingStub)
    monkeypatch.setattr(manage_topics, "NewTopic", _NewTopicStub)
    monkeypatch.setattr(manage_topics, "ConfigResource", _ConfigResourceStub)


def test_build_plan_merges_defaults(monkeypatch):
    _patch_confluent(monkeypatch)
    config = {
        "defaults": {
            "partitions": 6,
            "replication_factor": 2,
            "config": {"cleanup.policy": "delete"},
            "acl": {"host": "*", "permission": "ALLOW", "pattern": "LITERAL"},
        },
        "topics": [
            {
                "name": "aurum.test.topic",
                "config": {"retention.ms": "86400000"},
                "acls": [
                    {"principal": "User:producer", "operations": ["Write", "Describe"]},
                    {"principal": "User:consumer", "operations": ["Read"]},
                ],
            }
        ],
    }

    plans = manage_topics._build_plan(config)
    assert len(plans) == 1
    plan = plans[0]
    assert plan.name == "aurum.test.topic"
    assert plan.partitions == 6
    assert plan.replication_factor == 2
    assert plan.config["cleanup.policy"] == "delete"
    assert plan.config["retention.ms"] == "86400000"
    assert len(plan.acls) == 3  # operations expanded
    operations = {binding.operation.name for binding in plan.acls}
    assert operations == {"WRITE", "DESCRIBE", "READ"}


def test_create_topics_dry_run(capsys, monkeypatch):
    _patch_confluent(monkeypatch)
    plans = [
        manage_topics.TopicPlan(
            name="aurum.new.topic",
            partitions=3,
            replication_factor=1,
            config={"retention.ms": "3600000"},
            acls=[],
        )
    ]
    admin = _AdminStub(existing={"aurum.existing"})

    manage_topics._create_topics(admin, plans, dry_run=True, timeout=10.0)
    out = capsys.readouterr().out
    assert "aurum.new.topic" in out
    assert not admin.created


def test_apply_acls_dry_run(capsys, monkeypatch):
    _patch_confluent(monkeypatch)
    plan = manage_topics.TopicPlan(
        name="aurum.topic",
        partitions=1,
        replication_factor=1,
        config={},
        acls=[_BindingStub("TOPIC", "aurum.topic", None, "User:test", "*", SimpleNamespace(name="READ"), None)],
    )
    manage_topics._apply_acls(_AdminStub(), [plan], dry_run=True, timeout=10.0)
    out = capsys.readouterr().out
    assert "User:test" in out
    assert "READ" in out
