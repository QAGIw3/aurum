"""Service layer performance regression tests."""

import pytest
import time
import math
from typing import Dict, Any, List
from tests.factories import ScenarioFactory, CurveFactory


@pytest.mark.perf
class TestServicePerformance:
    """Performance regression tests for service layer operations."""

    def test_scenario_creation_performance(self, benchmark):
        """Benchmark scenario creation operations."""
        def create_scenarios():
            """Create multiple scenarios."""
            scenarios = []
            for i in range(100):
                scenario = ScenarioFactory(
                    name=f"Performance Test Scenario {i}",
                    scenario_type="monte_carlo"
                )
                scenarios.append(scenario)
            return scenarios

        result = benchmark(create_scenarios)

        # Verify creation works
        assert len(result) == 100
        assert all("name" in scenario for scenario in result)

    def test_curve_data_processing_performance(self, benchmark):
        """Benchmark curve data processing operations."""
        # Create test curve data
        curves = []
        for i in range(50):
            curve = CurveFactory()
            curves.append(curve)

        def process_curves():
            """Process curve data."""
            processed = []

            for curve in curves:
                # Simulate data processing operations
                data_points = curve.get("data_points", [])
                if data_points:
                    # Calculate statistics
                    values = [point["value"] for point in data_points]

                    processed_curve = {
                        "curve_id": curve["curve_id"],
                        "statistics": {
                            "mean": sum(values) / len(values),
                            "min": min(values),
                            "max": max(values),
                            "std_dev": math.sqrt(sum((x - sum(values)/len(values))**2 for x in values) / len(values)),
                            "count": len(values),
                        },
                        "quality_metrics": {
                            "completeness": len([p for p in data_points if p.get("confidence", 0) > 0.8]) / len(data_points),
                            "data_quality": sum(p.get("confidence", 0) for p in data_points) / len(data_points),
                        }
                    }
                    processed.append(processed_curve)

            return processed

        result = benchmark(process_curves)

        # Verify processing works
        assert len(result) == 50
        assert all("statistics" in item for item in result)

    def test_data_aggregation_performance(self, benchmark):
        """Benchmark data aggregation operations."""
        # Create test data
        scenarios = [ScenarioFactory() for _ in range(200)]

        def aggregate_data():
            """Aggregate scenario data."""
            # Simulate data aggregation across scenarios
            aggregated = {
                "total_scenarios": len(scenarios),
                "scenario_types": {},
                "assumption_types": {},
                "performance_stats": {
                    "total_assumptions": 0,
                    "avg_assumptions_per_scenario": 0,
                    "total_parameters": 0,
                }
            }

            for scenario in scenarios:
                # Count scenario types
                scenario_type = scenario.get("scenario_type", "unknown")
                aggregated["scenario_types"][scenario_type] = aggregated["scenario_types"].get(scenario_type, 0) + 1

                # Count assumption types
                assumptions = scenario.get("assumptions", [])
                for assumption in assumptions:
                    assumption_type = assumption.get("type", "unknown")
                    aggregated["assumption_types"][assumption_type] = aggregated["assumption_types"].get(assumption_type, 0) + 1

                # Update performance stats
                aggregated["performance_stats"]["total_assumptions"] += len(assumptions)
                aggregated["performance_stats"]["total_parameters"] += len(scenario.get("parameters", {}))

            # Calculate averages
            if scenarios:
                aggregated["performance_stats"]["avg_assumptions_per_scenario"] = (
                    aggregated["performance_stats"]["total_assumptions"] / len(scenarios)
                )

            return aggregated

        result = benchmark(aggregate_data)

        # Verify aggregation works
        assert "total_scenarios" in result
        assert "scenario_types" in result
        assert "assumption_types" in result
        assert result["total_scenarios"] == 200

    def test_memory_usage_simulation(self, benchmark):
        """Benchmark memory usage patterns during data processing."""
        def simulate_memory_usage():
            """Simulate memory-intensive operations."""
            data_structures = []

            # Simulate building large data structures
            for i in range(100):
                structure = {
                    "metadata": {"id": i, "timestamp": time.time()},
                    "data": [__import__("random").random() for _ in range(1000)],
                    "calculations": {
                        "sum": sum([__import__("random").random() for _ in range(100)]),
                        "mean": __import__("random").random(),
                        "variance": __import__("random").random() ** 2,
                    },
                    "nested": {
                        "level1": {
                            "level2": {
                                "level3": [__import__("random").random() for _ in range(50)]
                            }
                        }
                    }
                }
                data_structures.append(structure)

            # Simulate cleanup
            total_memory_estimate = len(data_structures) * 1000 * 8  # Rough bytes estimate
            return data_structures, total_memory_estimate

        result = benchmark(simulate_memory_usage)

        # Verify memory simulation works
        data_structures, memory_estimate = result
        assert len(data_structures) == 100
        assert memory_estimate > 0

    def test_parallel_processing_simulation(self, benchmark):
        """Benchmark parallel processing patterns."""
        def simulate_parallel_processing():
            """Simulate parallel data processing."""
            import threading
            import queue

            # Create test data
            data_queue = queue.Queue()
            for i in range(100):
                data_queue.put({"id": i, "value": i * 10, "metadata": {"priority": i % 3}})

            results = []
            results_lock = threading.Lock()

            def worker():
                """Simulate worker processing data."""
                while True:
                    try:
                        item = data_queue.get_nowait()
                        # Simulate processing time
                        time.sleep(0.001)

                        # Simulate computation
                        processed = {
                            "id": item["id"],
                            "original_value": item["value"],
                            "processed_value": item["value"] * 2,
                            "metadata": item["metadata"],
                            "processing_time": 0.001,
                        }

                        with results_lock:
                            results.append(processed)

                        data_queue.task_done()
                        break
                    except queue.Empty:
                        break

            # Start workers
            workers = []
            num_workers = 5
            for _ in range(num_workers):
                thread = threading.Thread(target=worker)
                thread.start()
                workers.append(thread)

            # Wait for completion
            for worker in workers:
                worker.join()

            return results

        result = benchmark(simulate_parallel_processing)

        # Verify parallel processing works
        assert len(result) == 100
        assert all("processed_value" in item for item in result)

    def test_cache_performance_simulation(self, benchmark):
        """Benchmark caching operations."""
        def simulate_cache_operations():
            """Simulate cache hit/miss patterns."""
            cache = {}
            hit_count = 0
            miss_count = 0

            # Simulate cache operations
            for i in range(1000):
                key = f"key-{i % 100}"  # 100 unique keys

                if key in cache:
                    # Cache hit
                    value = cache[key]
                    hit_count += 1
                else:
                    # Cache miss - simulate expensive computation
                    value = {"data": [__import__("random").random() for _ in range(100)], "computed_at": time.time()}
                    cache[key] = value
                    miss_count += 1

            return {
                "cache": cache,
                "hit_count": hit_count,
                "miss_count": miss_count,
                "hit_rate": hit_count / (hit_count + miss_count) if (hit_count + miss_count) > 0 else 0,
            }

        result = benchmark(simulate_cache_operations)

        # Verify cache simulation works
        assert "hit_count" in result
        assert "miss_count" in result
        assert "hit_rate" in result
        assert 0 <= result["hit_rate"] <= 1

    def test_io_operation_simulation(self, benchmark):
        """Benchmark I/O operation patterns."""
        def simulate_io_operations():
            """Simulate I/O bound operations."""
            import io
            import json

            operations = []

            # Simulate writing data
            for i in range(50):
                data = {
                    "id": i,
                    "timestamp": time.time(),
                    "payload": {
                        "scenarios": [{"id": j, "data": [__import__("random").random() for _ in range(10)]} for j in range(10)]
                    }
                }

                # Simulate JSON serialization (I/O operation)
                json_str = json.dumps(data)
                operations.append({
                    "operation": "write",
                    "size": len(json_str),
                    "data": json_str[:100] + "..."  # Truncate for memory
                })

            # Simulate reading data
            for operation in operations[:25]:  # Read half of them
                # Simulate JSON parsing (I/O operation)
                parsed = json.loads(operation["data"].replace("...", ""))
                operations.append({
                    "operation": "read",
                    "size": len(operation["data"]),
                    "data": parsed
                })

            return operations

        result = benchmark(simulate_io_operations)

        # Verify I/O simulation works
        assert len(result) == 75  # 50 writes + 25 reads
        assert all("operation" in op for op in result)

        # Verify mix of operations
        operations = [op["operation"] for op in result]
        assert "write" in operations
        assert "read" in operations
