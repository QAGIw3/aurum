import sys
from pathlib import Path
from typing import Optional, Any

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
for path in (ROOT, SRC):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)

# Ensure older pytest-asyncio style decorator remains available
try:
    import pytest
    import pytest_asyncio

    if not hasattr(pytest_asyncio, "asyncio"):
        pytest_asyncio.asyncio = pytest.mark.asyncio  # type: ignore[attr-defined]
except Exception:  # pragma: no cover - defensive import shim
    pass

# Opt-in fixture to enable default settings auto-init in selected test modules.
# Usage in a module: `pytestmark = pytest.mark.usefixtures("enable_test_default_settings")`
import os
import pytest

# Lightweight Airflow stub to satisfy DAG factory tests without requiring Airflow.
if "airflow" not in sys.modules:
    from types import ModuleType

    airflow_stub = ModuleType("airflow")

    class _StubDAG:
        def __init__(
            self,
            dag_id: str,
            default_args: Optional[dict] = None,
            schedule_interval: Optional[str] = None,
            description: Optional[str] = None,
            catchup: bool = False,
            max_active_runs: int = 1,
            start_date: Optional[Any] = None,
            tags: Optional[list] = None,
            **_: Any,
        ):
            self.dag_id = dag_id
            self.default_args = default_args or {}
            self.schedule_interval = schedule_interval
            self.description = description
            self.catchup = catchup
            self.max_active_runs = max_active_runs
            self.start_date = start_date
            self.tags = tags or []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    airflow_stub.DAG = _StubDAG

    operators_module = ModuleType("airflow.operators")
    operators_module.__path__ = []  # mark as package for import machinery

    bash_module = ModuleType("airflow.operators.bash")

    class _StubBashOperator:
        def __init__(self, task_id: str, bash_command: str, dag=None, env=None, **kwargs: Any):
            self.task_id = task_id
            self.bash_command = bash_command
            self.dag = dag
            self.env = env or {}
            self.kwargs = kwargs
            self.downstream: list[Any] = []
            self.upstream: list[Any] = []

        def __rshift__(self, other):
            self.downstream.append(other)
            upstream = getattr(other, "upstream", None)
            if upstream is not None:
                upstream.append(self)
            return other

        def __lshift__(self, other):
            self.upstream.append(other)
            downstream = getattr(other, "downstream", None)
            if downstream is not None:
                downstream.append(self)
            return other

    bash_module.BashOperator = _StubBashOperator

    python_module = ModuleType("airflow.operators.python")

    class _StubPythonOperator:
        def __init__(self, task_id: str, python_callable, dag=None, op_args=None, op_kwargs=None, **kwargs: Any):
            self.task_id = task_id
            self.python_callable = python_callable
            self.dag = dag
            self.op_args = op_args or []
            self.op_kwargs = op_kwargs or {}
            self.kwargs = kwargs
            self.downstream: list[Any] = []
            self.upstream: list[Any] = []

        def __rshift__(self, other):
            self.downstream.append(other)
            upstream = getattr(other, "upstream", None)
            if upstream is not None:
                upstream.append(self)
            return other

        def __lshift__(self, other):
            self.upstream.append(other)
            downstream = getattr(other, "downstream", None)
            if downstream is not None:
                downstream.append(self)
            return other

    python_module.PythonOperator = _StubPythonOperator

    empty_module = ModuleType("airflow.operators.empty")

    class _StubEmptyOperator:
        def __init__(self, task_id: str, dag=None, **kwargs: Any):
            self.task_id = task_id
            self.dag = dag
            self.kwargs = kwargs
            self.downstream: list[Any] = []
            self.upstream: list[Any] = []

        def __rshift__(self, other):
            targets = other if isinstance(other, list) else [other]
            for target in targets:
                self.downstream.append(target)
                upstream = getattr(target, "upstream", None)
                if upstream is not None:
                    upstream.append(self)
            return other

        def __lshift__(self, other):
            sources = other if isinstance(other, list) else [other]
            for source in sources:
                self.upstream.append(source)
                downstream = getattr(source, "downstream", None)
                if downstream is not None:
                    downstream.append(self)
            return other

        def __rrshift__(self, other):
            sources = other if isinstance(other, list) else [other]
            for source in sources:
                downstream = getattr(source, "downstream", None)
                if downstream is not None:
                    downstream.append(self)
                self.upstream.append(source)
            return self

    empty_module.EmptyOperator = _StubEmptyOperator

    utils_module = ModuleType("airflow.utils")
    task_group_module = ModuleType("airflow.utils.task_group")

    class _StubTaskGroup:
        def __init__(self, group_id: str, prefix_group_id: bool = True, **_: Any):
            self.group_id = group_id
            self.prefix_group_id = prefix_group_id
            self.tasks: list[Any] = []

        def add(self, task):
            self.tasks.append(task)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    task_group_module.TaskGroup = _StubTaskGroup
    utils_module.task_group = task_group_module

    sys.modules["airflow"] = airflow_stub
    sys.modules["airflow.operators"] = operators_module
    sys.modules["airflow.operators.bash"] = bash_module
    sys.modules["airflow.operators.python"] = python_module
    sys.modules["airflow.operators.empty"] = empty_module
    sys.modules["airflow.utils"] = utils_module
    sys.modules["airflow.utils.task_group"] = task_group_module


@pytest.fixture(scope="module")
def enable_test_default_settings():
    """Set AURUM_TEST_DEFAULT_SETTINGS=1 for all tests in a module.

    Apply with `pytestmark = pytest.mark.usefixtures("enable_test_default_settings")`
    at the top of a test module to opt-in.
    """
    prev = os.environ.get("AURUM_TEST_DEFAULT_SETTINGS")
    os.environ["AURUM_TEST_DEFAULT_SETTINGS"] = "1"
    try:
        yield
    finally:
        if prev is None:
            os.environ.pop("AURUM_TEST_DEFAULT_SETTINGS", None)
        else:
            os.environ["AURUM_TEST_DEFAULT_SETTINGS"] = prev
