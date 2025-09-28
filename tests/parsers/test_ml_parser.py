import pandas as pd

from aurum.parsers.ml_parser import CurveAnomalyDetector


def test_anomaly_detector_flags_outliers():
    data = pd.DataFrame(
        {
            "curve_key": ["c"] * 10,
            "tenor_label": [f"2024-{i:02d}" for i in range(1, 11)],
            "mid": [50.0] * 9 + [120.0],
        }
    )

    detector = CurveAnomalyDetector(min_points=5, zscore_threshold=3.0)
    result = detector.detect(data)

    assert not result.anomalies.empty
    assert result.confidence_score < 1.0
