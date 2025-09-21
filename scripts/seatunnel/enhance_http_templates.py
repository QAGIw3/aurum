#!/usr/bin/env python3
"""Enhance SeaTunnel HTTP templates with improved retry and backoff configuration."""
from __future__ import annotations

import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
TEMPLATE_DIR = REPO_ROOT / "seatunnel" / "jobs" / "templates"

# Enhanced HTTP configuration to add to each template
HTTP_ENHANCEMENTS = {
    "retry": "5",
    "retry_backoff_multiplier_ms": "1500",
    "retry_backoff_max_ms": "90000",
    "rate_limit_sleep_ms": "200"
}

# Kafka producer enhancements
KAFKA_PRODUCER_ENHANCEMENTS = {
    "retries": "10",
    "retry.backoff.ms": "100",
    "retry.backoff.max.ms": "10000",
    "delivery.timeout.ms": "120000",
    "request.timeout.ms": "30000"
}

def enhance_http_template(template_path: Path) -> bool:
    """Enhance a single HTTP template with better retry configuration."""
    content = template_path.read_text()

    # Check if already enhanced
    if "retry_backoff_multiplier_ms" in content:
        print(f"Skipping {template_path} - already enhanced")
        return False

    # Find Http source blocks and enhance them
    def enhance_http_block(match):
        block = match.group(0)

        # Add retry enhancements after connection_timeout_ms
        enhanced_block = re.sub(
            r'(connection_timeout_ms = \$\{[^}]+\}|connection_timeout_ms = \d+)',
            r'\1\n    retry = 5\n    retry_backoff_multiplier_ms = 1500\n    retry_backoff_max_ms = 90000\n    rate_limit_sleep_ms = 200',
            block
        )

        return enhanced_block

    # Enhance HTTP source blocks
    content = re.sub(r'(Http \{(?:[^{}]|{(?:[^{}]|{[^{}]*})*})*\})', enhance_http_block, content, flags=re.MULTILINE | re.DOTALL)

    # Enhance Kafka producer blocks
    def enhance_kafka_producer(match):
        block = match.group(0)

        # Add producer enhancements
        enhanced_block = block
        for key, value in KAFKA_PRODUCER_ENHANCEMENTS.items():
            if key not in block:
                enhanced_block = re.sub(
                    r'(producer \{)',
                    f'\\1\n      {key} = {value}',
                    enhanced_block,
                    count=1
                )

        return enhanced_block

    # Find and enhance Kafka producer blocks
    content = re.sub(r'(producer \{(?:[^{}]|{(?:[^{}]|{[^{}]*})*})*\})', enhance_kafka_producer, content, flags=re.MULTILINE | re.DOTALL)

    # Write back enhanced content
    template_path.write_text(content)
    print(f"Enhanced {template_path}")
    return True

def main() -> int:
    if not TEMPLATE_DIR.exists():
        print(f"Template directory not found: {TEMPLATE_DIR}", file=sys.stderr)
        return 1

    enhanced_count = 0
    for template_path in TEMPLATE_DIR.glob("*.conf.tmpl"):
        if "Http {" in template_path.read_text():
            if enhance_http_template(template_path):
                enhanced_count += 1

    print(f"Enhanced {enhanced_count} HTTP templates")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
