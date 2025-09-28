import pandas as pd

from aurum.ml import AdaptiveAnomalyDetector, detect_anomalies


def test_detect_anomalies_spike_and_dip():
    values = [10] * 10 + [50] + [10] * 10 + [5] + [10] * 10
    idx = pd.date_range("2024-01-01", periods=len(values), freq="h")
    s = pd.Series(values, index=idx)
    df = detect_anomalies(s, window=6, z_threshold=2.0)
    assert len(df) >= 2
    assert set(df["side"]).issubset({"long", "short", "neutral"})


def test_adaptive_anomaly_detector_update_stream():
    detector = AdaptiveAnomalyDetector(window=6, z_threshold=2.0, mad_threshold=2.5)
    series = [10] * 12 + [40] + [10] * 5
    events = []
    for i, val in enumerate(series):
        event = detector.update(float(val), timestamp=i, index_position=i)
        if event:
            events.append(event)
    assert events
    assert any(e.side == "short" for e in events)
    assert all(e.method == "adaptive" for e in events)

