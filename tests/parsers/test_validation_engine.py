import pandas as pd

from aurum.parsers.validation_engine import ValidationEngine


def test_validation_engine_reports_missing_columns():
    frame = pd.DataFrame({"curve_key": ["a"], "tenor_label": ["2024-01"], "mid": [42.0]})
    engine = ValidationEngine()
    result = engine.validate(frame)

    assert result.confidence < 1.0
    assert any("Missing" in issue.message for issue in result.issues)
