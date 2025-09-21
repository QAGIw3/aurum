#!/usr/bin/env python
"""Create or update Kafka topics and ACLs from a declarative config."""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping, MutableMapping, TYPE_CHECKING, Any

try:  # pragma: no cover - optional dependency
    from confluent_kafka import KafkaError, KafkaException
    from confluent_kafka.admin import (
        AclPermissionType,
        AclOperation,
        AclBinding,
        AdminClient,
        ConfigResource,
        NewTopic,
        ResourcePatternType,
        ResourceType,
    )
    _CONFLUENT_AVAILABLE = True
except ModuleNotFoundError:  # pragma: no cover - handled in main
    KafkaError = KafkaException = None  # type: ignore
    AdminClient = None  # type: ignore
    AclPermissionType = AclOperation = ResourcePatternType = ResourceType = None  # type: ignore
    AclBinding = Any  # type: ignore
    ConfigResource = NewTopic = None  # type: ignore
    _CONFLUENT_AVAILABLE = False

if TYPE_CHECKING:  # pragma: no cover - typing only
    from confluent_kafka.admin import AclBinding as _AclBinding
else:
    _AclBinding = Any


DEFAULT_CONFIG_PATH = Path("config/kafka_topics.json")


@dataclass
class TopicPlan:
    name: str
    partitions: int
    replication_factor: int
    config: dict[str, str]
    acls: list[_AclBinding]


def _load_config(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:  # pragma: no cover - handled at CLI
        raise SystemExit(f"Config file not found: {path}") from exc
    except json.JSONDecodeError as exc:  # pragma: no cover - handled at CLI
        raise SystemExit(f"Invalid JSON in config file: {path}\n{exc}") from exc


def _merge_dicts(base: Mapping[str, str] | None, override: Mapping[str, str] | None) -> dict[str, str]:
    merged: dict[str, str] = {}
    if base:
        merged.update(base)
    if override:
        merged.update(override)
    return {k: str(v) for k, v in merged.items()}


def _build_acl_bindings(
    topic: str,
    acl_specs: Iterable[MutableMapping[str, object]],
    defaults: Mapping[str, object],
) -> list[_AclBinding]:
    bindings: list[_AclBinding] = []
    host = str(defaults.get("host", "*"))
    permission_raw = str(defaults.get("permission", "ALLOW")).upper()
    pattern_raw = str(defaults.get("pattern", "LITERAL")).upper()
    try:
        permission = AclPermissionType[permission_raw]
        pattern = ResourcePatternType[pattern_raw]
    except KeyError as exc:
        raise ValueError(f"Unsupported ACL setting: {exc}") from exc

    for spec in acl_specs:
        principal = str(spec.get("principal"))
        if not principal:
            raise ValueError(f"ACL entry for {topic} missing principal")
        operations = spec.get("operations")
        if not operations:
            raise ValueError(f"ACL entry for {topic} missing operations for {principal}")
        for op in operations:
            op_name = str(op).upper()
            try:
                operation = AclOperation[op_name]
            except KeyError as exc:
                raise ValueError(f"Unsupported ACL operation '{op_name}' for {principal}") from exc
            bindings.append(
                AclBinding(
                    ResourceType.TOPIC,
                    topic,
                    pattern,
                    principal,
                    host,
                    operation,
                    permission,
                )
            )
    return bindings


def _build_plan(config: dict) -> list[TopicPlan]:
    defaults = config.get("defaults", {})
    default_partitions = int(defaults.get("partitions", 3))
    default_replication = int(defaults.get("replication_factor", 3))
    default_topic_config = {
        k: str(v) for k, v in (defaults.get("config") or {}).items()
    }
    default_acl = defaults.get("acl") or {}

    plans: list[TopicPlan] = []
    for entry in config.get("topics", []):
        name = entry.get("name")
        if not name:
            raise ValueError("Topic entry missing name")
        partitions = int(entry.get("partitions", default_partitions))
        replication = int(entry.get("replication_factor", default_replication))
        topic_config = _merge_dicts(default_topic_config, entry.get("config") or {})
        acl_specs = entry.get("acls") or []
        acls = _build_acl_bindings(name, acl_specs, default_acl)
        plans.append(
            TopicPlan(
                name=name,
                partitions=partitions,
                replication_factor=replication,
                config=topic_config,
                acls=acls,
            )
        )
    return plans


def _create_topics(admin: AdminClient, plans: list[TopicPlan], *, dry_run: bool, timeout: float) -> None:
    metadata = admin.list_topics(timeout=timeout)
    existing = set(metadata.topics.keys())

    to_create: list[NewTopic] = []
    for plan in plans:
        if plan.name in existing:
            continue
        to_create.append(
            NewTopic(
                topic=plan.name,
                num_partitions=plan.partitions,
                replication_factor=plan.replication_factor,
                config=plan.config or None,
            )
        )

    if not to_create:
        return
    if dry_run:
        for topic in to_create:
            print(f"[dry-run] would create topic {topic.topic} (partitions={topic.num_partitions}, replication={topic.replication_factor})")
        return

    futures = admin.create_topics(to_create, request_timeout=timeout)
    for topic, future in futures.items():
        try:
            future.result()
            print(f"Created topic {topic}")
        except KafkaException as exc:
            error = exc.args[0]
            if isinstance(error, KafkaError) and error.code() == KafkaError.TOPIC_ALREADY_EXISTS:
                print(f"Topic {topic} already exists", file=sys.stderr)
                continue
            raise


def _apply_topic_configs(admin: AdminClient, plans: list[TopicPlan], *, dry_run: bool, timeout: float) -> None:
    resources: list[ConfigResource] = []
    for plan in plans:
        if not plan.config:
            continue
        resources.append(
            ConfigResource(
                ResourceType.TOPIC,
                plan.name,
                set_config=plan.config,
            )
        )

    if not resources:
        return

    if dry_run:
        for resource in resources:
            print(f"[dry-run] would set configs on {resource.name}: {resource.set_config}")
        return

    futures = admin.alter_configs(resources, request_timeout=timeout)
    for resource, future in futures.items():
        try:
            future.result()
            print(f"Updated config for {resource.name}")
        except KafkaException as exc:
            error = exc.args[0]
            if isinstance(error, KafkaError):
                print(f"Failed to update config for {resource.name}: {error.str()}", file=sys.stderr)
            else:
                raise


def _apply_acls(admin: AdminClient, plans: list[TopicPlan], *, dry_run: bool, timeout: float) -> None:
    desired: dict[tuple[str, str, str], AclBinding] = {}
    for plan in plans:
        for binding in plan.acls:
            key = (binding.principal, binding.operation.name, plan.name)
            desired[key] = binding

    if not desired:
        return

    if dry_run:
        for binding in desired.values():
            print(
                f"[dry-run] would ensure ACL {binding.operation.name} on {binding.resource_name} for {binding.principal}"
            )
        return

    bindings = list(desired.values())
    futures = admin.create_acls(bindings, request_timeout=timeout)
    for binding, future in futures.items():
        try:
            future.result()
            print(
                f"Created ACL {binding.operation.name} on {binding.resource_name} for {binding.principal}"
            )
        except KafkaException as exc:
            error = exc.args[0]
            if isinstance(error, KafkaError) and error.code() == KafkaError.SECURITY_NOT_SUPPORTED:
                raise SystemExit(
                    "Broker does not support ACLs. Enable authorizer to manage ACL bindings."
                ) from exc
            if isinstance(error, KafkaError) and error.code() == KafkaError.SECURITY_DISABLED:
                raise SystemExit(
                    "ACL management is disabled on the Kafka cluster"
                ) from exc
            if isinstance(error, KafkaError) and error.code() == KafkaError.ACL_ALREADY_EXISTS:
                continue
            raise


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manage Kafka topics and ACLs for Aurum")
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Kafka bootstrap servers (default: localhost:9092)",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help=f"Path to topics config JSON (default: {DEFAULT_CONFIG_PATH})",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=15.0,
        help="Admin client request timeout in seconds (default: 15)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show planned changes without applying them",
    )
    parser.add_argument(
        "--skip-acls",
        action="store_true",
        help="Do not create ACL bindings",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    if not _CONFLUENT_AVAILABLE:
        raise SystemExit(
            "The 'confluent-kafka' package is required. Install with 'pip install confluent-kafka[avro]'"
        )
    config = _load_config(args.config)
    plans = _build_plan(config)

    admin = AdminClient({"bootstrap.servers": args.bootstrap_servers})

    _create_topics(admin, plans, dry_run=args.dry_run, timeout=args.timeout)
    _apply_topic_configs(admin, plans, dry_run=args.dry_run, timeout=args.timeout)
    if not args.skip_acls:
        _apply_acls(admin, plans, dry_run=args.dry_run, timeout=args.timeout)

    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(1)
