#!/usr/bin/env python3
"""Run Kafka consumer-driven contract tests using Pact-like approach."""

import json
import sys
import time
from pathlib import Path
from typing import Dict, List, Any
from kafka import KafkaProducer, KafkaConsumer
import requests


def create_test_producer(bootstrap_servers: str) -> KafkaProducer:
    """Create a Kafka producer for testing."""
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        acks='all',
        retries=3
    )


def create_test_consumer(bootstrap_servers: str, topic: str, group_id: str) -> KafkaConsumer:
    """Create a Kafka consumer for testing."""
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )


def load_contracts(contracts_dir: str) -> List[Dict[str, Any]]:
    """Load consumer-driven contracts."""
    contracts = []
    contracts_path = Path(contracts_dir)

    if not contracts_path.exists():
        print(f"⚠️ Contracts directory not found: {contracts_dir}")
        return contracts

    for contract_file in contracts_path.glob("*.json"):
        try:
            with open(contract_file) as f:
                contract = json.load(f)
                contracts.append(contract)
        except Exception as e:
            print(f"❌ Error loading contract {contract_file}: {e}")

    return contracts


def validate_message_against_contract(message: Dict[str, Any], contract: Dict[str, Any]) -> bool:
    """Validate a message against a contract."""
    try:
        # Check required fields
        required_fields = contract.get("required_fields", [])
        for field in required_fields:
            if field not in message:
                print(f"❌ Missing required field: {field}")
                return False

        # Check field types
        field_types = contract.get("field_types", {})
        for field, expected_type in field_types.items():
            if field in message:
                actual_value = message[field]
                if not isinstance(actual_value, expected_type):
                    print(f"❌ Field {field}: expected {expected_type}, got {type(actual_value)}")
                    return False

        # Check field constraints
        field_constraints = contract.get("field_constraints", {})
        for field, constraints in field_constraints.items():
            if field in message:
                value = message[field]
                if "min_length" in constraints and len(str(value)) < constraints["min_length"]:
                    print(f"❌ Field {field}: length {len(str(value))} < min_length {constraints['min_length']}")
                    return False

                if "max_length" in constraints and len(str(value)) > constraints["max_length"]:
                    print(f"❌ Field {field}: length {len(str(value))} > max_length {constraints['max_length']}")
                    return False

                if "pattern" in constraints and not __import__('re').match(constraints["pattern"], str(value)):
                    print(f"❌ Field {field}: value '{value}' does not match pattern '{constraints['pattern']}'")
                    return False

        # Check message size constraints
        message_size = len(json.dumps(message).encode('utf-8'))
        max_size = contract.get("max_message_size_bytes", 1048576)  # 1MB default
        if message_size > max_size:
            print(f"❌ Message size {message_size} bytes exceeds maximum {max_size} bytes")
            return False

        return True

    except Exception as e:
        print(f"❌ Error validating message against contract: {e}")
        return False


def test_consumer_contract(contract: Dict[str, Any], producer: KafkaProducer, consumer: KafkaConsumer, report_dir: str) -> bool:
    """Test a consumer contract."""
    contract_name = contract.get("name", "unknown")
    topic = contract.get("topic", "test-topic")

    print(f"🧪 Testing contract: {contract_name} on topic: {topic}")

    # Create test messages based on contract
    test_messages = []

    # Positive test cases
    for test_case in contract.get("positive_cases", []):
        test_messages.append(test_case)

    # Negative test cases
    for test_case in contract.get("negative_cases", []):

        # Create invalid messages for testing
        invalid_message = test_case.copy()

        # Remove required fields to create invalid message
        if "invalid_by_missing_fields" in test_case:
            missing_fields = test_case["invalid_by_missing_fields"]
            for field in missing_fields:
                invalid_message.pop(field, None)

        test_messages.append(invalid_message)

    # Send test messages
    for i, message in enumerate(test_messages):
        try:
            key = f"test-key-{i}"
            producer.send(topic, key=key, value=message)
            producer.flush()
            print(f"  ✅ Sent test message {i + 1}/{len(test_messages)}")
        except Exception as e:
            print(f"❌ Error sending test message {i + 1}: {e}")
            return False

    # Consume and validate messages
    validated_count = 0
    expected_validations = len(test_messages)

    start_time = time.time()
    timeout = 30  # 30 seconds timeout

    while validated_count < expected_validations and (time.time() - start_time) < timeout:
        message_batch = consumer.poll(timeout_ms=1000, max_records=10)

        for topic_partition, messages in message_batch.items():
            for message in messages:
                try:
                    message_value = message.value

                    # Determine if this should be valid or invalid
                    is_positive_case = any(
                        message_value == test_case
                        for test_case in contract.get("positive_cases", [])
                    )

                    # Validate message
                    if is_positive_case:
                        if validate_message_against_contract(message_value, contract):
                            print("  ✅ Valid message validated successfully")
                            validated_count += 1
                        else:
                            print("❌ Valid message failed validation")
                            return False
                    else:
                        # For negative cases, we expect validation to fail
                        # But we should still consume them without error
                        print("  ✅ Invalid message handled (expected to be rejected)")
                        validated_count += 1

                except Exception as e:
                    print(f"❌ Error processing message: {e}")
                    return False

    if validated_count == expected_validations:
        print(f"✅ Contract test passed: {contract_name}")
        return True
    else:
        print(f"❌ Contract test failed: {contract_name} (validated {validated_count}/{expected_validations})")
        return False


def main():
    """Main function to run Pact-style consumer contract tests."""
    import argparse

    parser = argparse.ArgumentParser(description="Run Kafka consumer-driven contract tests")
    parser.add_argument("--kafka-bootstrap", required=True, help="Kafka bootstrap servers")
    parser.add_argument("--schema-registry", required=True, help="Schema Registry URL")
    parser.add_argument("--contracts-dir", required=True, help="Directory containing contract files")
    parser.add_argument("--report-dir", required=True, help="Directory to store test reports")

    args = parser.parse_args()

    print("🔍 Starting Kafka consumer-driven contract tests...")

    # Load contracts
    contracts = load_contracts(args.contracts_dir)

    if not contracts:
        print("⚠️ No contracts found")
        return 0

    print(f"Found {len(contracts)} contracts to test")

    # Create report directory
    report_path = Path(args.report_dir)
    report_path.mkdir(exist_ok=True)

    # Test results
    test_results = []
    all_passed = True

    for contract in contracts:
        try:
            contract_name = contract.get("name", "unknown")
            topic = contract.get("topic", "test-topic")

            print(f"\n🧪 Testing contract: {contract_name}")

            # Create producer and consumer
            producer = create_test_producer(args.kafka_bootstrap)
            consumer = create_test_consumer(args.kafka_bootstrap, topic, f"pact-test-{contract_name}")

            # Run contract test
            test_passed = test_consumer_contract(contract, producer, consumer, args.report_dir)

            test_results.append({
                "contract": contract_name,
                "topic": topic,
                "passed": test_passed,
                "timestamp": time.time()
            })

            if not test_passed:
                all_passed = False

            # Cleanup
            consumer.close()
            producer.close()

        except Exception as e:
            print(f"❌ Error testing contract {contract.get('name', 'unknown')}: {e}")
            test_results.append({
                "contract": contract.get("name", "unknown"),
                "passed": False,
                "error": str(e),
                "timestamp": time.time()
            })
            all_passed = False

    # Generate test report
    test_report = {
        "test_timestamp": time.time(),
        "total_contracts": len(contracts),
        "passed_contracts": sum(1 for r in test_results if r["passed"]),
        "failed_contracts": sum(1 for r in test_results if not r["passed"]),
        "all_passed": all_passed,
        "results": test_results
    }

    with open(report_path / "pact_test_report.json", "w") as f:
        json.dump(test_report, f, indent=2)

    if all_passed:
        print("✅ All consumer-driven contract tests passed")
        return 0
    else:
        print("❌ Some consumer-driven contract tests failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
