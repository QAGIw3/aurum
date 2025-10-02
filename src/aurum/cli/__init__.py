"""Aurum command-line tools package.

Provides small helper CLIs (e.g., scenario operations) that interact with the
running Aurum API. Modules in this package are safe to import in Airflow and
batch contexts.
"""

from .admin import main as admin
from .feature import main as feature
from .scenario import main as scenario
from .workflow import main as workflow
from .stress_test import main as stress_test

__all__ = ["admin", "feature", "scenario", "workflow", "stress_test"]
